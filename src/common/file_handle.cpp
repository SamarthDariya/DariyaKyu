#include "common/file_handle.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <utility>

#include "common/errors.hpp"

using namespace std;

namespace dariyakyu {

namespace {

int flagsFor(FileHandle::Mode mode) {
    switch (mode) {
        case FileHandle::Mode::ReadOnly:  return O_RDONLY;
        case FileHandle::Mode::ReadWrite: return O_RDWR | O_CREAT;
        case FileHandle::Mode::CreateNew: return O_RDWR | O_CREAT | O_EXCL;
    }
    return O_RDONLY;
}

}  // namespace

FileHandle::FileHandle(const filesystem::path& path, Mode mode) : path_(path) {
    fd_ = ::open(path.c_str(), flagsFor(mode), 0644);
    if (fd_ < 0) throw IoError("open", path, errno);

    // Start appending where the file currently ends. For a freshly created file
    // that is zero; for one being reopened after a restart it is its length.
    const off_t end = ::lseek(fd_, 0, SEEK_END);
    if (end < 0) {
        const int saved = errno;
        ::close(fd_);
        fd_ = -1;
        throw IoError("lseek", path, saved);
    }
    writePosition_.store(static_cast<uint64_t>(end), memory_order_relaxed);
}

FileHandle::~FileHandle() {
    // Destructors must not throw, so a failing close is swallowed here. Callers
    // that need to observe close errors call close() explicitly.
    if (fd_ >= 0) ::close(fd_);
}

// An atomic is not itself movable, so the value is carried across by hand.
// Relaxed ordering is correct here: a FileHandle being moved is not being
// shared, or the move would already be a race.
FileHandle::FileHandle(FileHandle&& other) noexcept
    : fd_(exchange(other.fd_, -1)),
      path_(std::move(other.path_)),
      writePosition_(other.writePosition_.load(memory_order_relaxed)) {
    other.writePosition_.store(0, memory_order_relaxed);
}

FileHandle& FileHandle::operator=(FileHandle&& other) noexcept {
    if (this != &other) {
        if (fd_ >= 0) ::close(fd_);
        fd_   = exchange(other.fd_, -1);
        path_ = std::move(other.path_);
        writePosition_.store(other.writePosition_.load(memory_order_relaxed),
                             memory_order_relaxed);
        other.writePosition_.store(0, memory_order_relaxed);
    }
    return *this;
}

uint64_t FileHandle::append(span<const uint8_t> bytes) {
    // Only the appender thread writes writePosition_, so it may read its own
    // value without acquiring.
    const uint64_t startedAt = writePosition_.load(memory_order_relaxed);

    // pwrite() can return short — on a signal, or when the device is nearly
    // full. Looping is not paranoia; a partial write that we failed to finish
    // would leave a torn record for recovery to find.
    size_t written = 0;
    while (written < bytes.size()) {
        const ssize_t n = ::pwrite(fd_, bytes.data() + written, bytes.size() - written,
                                   static_cast<off_t>(startedAt + written));
        if (n < 0) {
            if (errno == EINTR) continue;
            throw IoError("pwrite", path_, errno);
        }
        written += static_cast<size_t>(n);
    }

    // Publish. Everything written above happens-before any reader that observes
    // this new size, so a reader can never see the length without the bytes.
    writePosition_.store(startedAt + written, memory_order_release);
    return startedAt;
}

size_t FileHandle::readAt(uint64_t position, span<uint8_t> out) const {
    size_t read = 0;
    while (read < out.size()) {
        const ssize_t n = ::pread(fd_, out.data() + read, out.size() - read,
                                  static_cast<off_t>(position + read));
        if (n < 0) {
            if (errno == EINTR) continue;
            throw IoError("pread", path_, errno);
        }
        if (n == 0) break;  // end of file — a short read, not an error
        read += static_cast<size_t>(n);
    }
    return read;
}

void FileHandle::truncate(uint64_t newSize) {
    if (::ftruncate(fd_, static_cast<off_t>(newSize)) < 0)
        throw IoError("ftruncate", path_, errno);
    writePosition_.store(newSize, memory_order_release);
}

void FileHandle::sync() {
    if (::fsync(fd_) < 0) throw IoError("fsync", path_, errno);
}

void FileHandle::close() {
    if (fd_ < 0) return;
    const int fd = exchange(fd_, -1);
    if (::close(fd) < 0) throw IoError("close", path_, errno);
}

}  // namespace dariyakyu
