#include "common/mapped_file.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <utility>

#include "common/errors.hpp"

using namespace std;

namespace dariyakyu {

MappedFile::MappedFile(const filesystem::path& path, size_t length)
    : MappedFile(path, length, Access::ReadWrite) {}

MappedFile MappedFile::openReadOnly(const filesystem::path& path) {
    error_code ec;
    const auto fileSize = filesystem::file_size(path, ec);
    if (ec) throw IoError("stat", path, ec.value());
    return MappedFile(path, static_cast<size_t>(fileSize), Access::ReadOnly);
}

MappedFile::MappedFile(const filesystem::path& path, size_t length, Access access)
    : length_(length), writable_(access == Access::ReadWrite), path_(path) {
    if (length == 0) {
        // An empty read-only mapping is a real state — a sealed segment whose
        // index never reached one index interval. There is nothing to map, and
        // bytes() correctly returns an empty span.
        if (!writable_) return;
        // Asking to preallocate nothing, on the other hand, is a bug.
        throw Error("MappedFile: zero-length writable mapping requested for " + path.string());
    }

    const int fd = ::open(path.c_str(), writable_ ? (O_RDWR | O_CREAT) : O_RDONLY, 0644);
    if (fd < 0) throw IoError("open", path, errno);

    if (writable_) {
        // Grow to the full mapping length if the file is shorter. Mapping past
        // the end of a file is legal, but touching those pages raises SIGBUS —
        // a signal, with no stack unwinding and nothing useful to catch. So the
        // space has to exist before it is mapped.
        struct stat st {};
        if (::fstat(fd, &st) < 0) {
            const int saved = errno;
            ::close(fd);
            throw IoError("fstat", path, saved);
        }
        if (static_cast<size_t>(st.st_size) < length) {
            if (::ftruncate(fd, static_cast<off_t>(length)) < 0) {
                const int saved = errno;
                ::close(fd);
                throw IoError("ftruncate", path, saved);
            }
        }
    }

    const int protection = writable_ ? (PROT_READ | PROT_WRITE) : PROT_READ;
    addr_                = ::mmap(nullptr, length, protection, MAP_SHARED, fd, 0);

    // The mapping holds its own reference to the file, so the descriptor can go
    // immediately. That matters at scale: a broker with a thousand partitions
    // would otherwise keep thousands of descriptors open purely to be able to
    // truncate once, at seal time.
    const int savedErrno = errno;
    ::close(fd);

    if (addr_ == MAP_FAILED) {
        addr_ = nullptr;
        throw IoError("mmap", path, savedErrno);
    }
}

MappedFile::~MappedFile() {
    unmap();
}

MappedFile::MappedFile(MappedFile&& other) noexcept
    : addr_(exchange(other.addr_, nullptr)),
      length_(exchange(other.length_, 0)),
      writable_(exchange(other.writable_, false)),
      path_(std::move(other.path_)) {}

MappedFile& MappedFile::operator=(MappedFile&& other) noexcept {
    if (this != &other) {
        unmap();
        addr_     = exchange(other.addr_, nullptr);
        length_   = exchange(other.length_, 0);
        writable_ = exchange(other.writable_, false);
        path_     = std::move(other.path_);
    }
    return *this;
}

span<const uint8_t> MappedFile::bytes() const {
    return {static_cast<const uint8_t*>(addr_), length_};
}

span<uint8_t> MappedFile::mutableBytes() {
    if (!writable_)
        throw Error("MappedFile: write attempted on a read-only mapping of " + path_.string());
    return {static_cast<uint8_t*>(addr_), length_};
}

void MappedFile::flush() {
    if (addr_ == nullptr || !writable_) return;
    if (::msync(addr_, length_, MS_SYNC) < 0) throw IoError("msync", path_, errno);
}

void MappedFile::unmapAndTrim(size_t usedBytes) {
    // Checked before writability, because unmap() clears writable_ — otherwise a
    // second call would report "read-only" when the truth is "already unmapped".
    if (!isMapped())
        throw Error("MappedFile: trim attempted on an unmapped file: " + path_.string());
    if (!writable_)
        throw Error("MappedFile: trim attempted on a read-only mapping of " + path_.string());

    flush();
    unmap();

    const int fd = ::open(path_.c_str(), O_RDWR);
    if (fd < 0) throw IoError("open", path_, errno);
    const int rc    = ::ftruncate(fd, static_cast<off_t>(usedBytes));
    const int saved = errno;
    ::close(fd);
    if (rc < 0) throw IoError("ftruncate", path_, saved);
}

void MappedFile::unmap() {
    if (addr_ == nullptr) return;
    // Nothing useful to do on failure here: this runs from the destructor, and
    // a destructor that throws during unwinding calls std::terminate.
    ::munmap(addr_, length_);
    addr_     = nullptr;
    length_   = 0;
    writable_ = false;
}

}  // namespace dariyakyu
