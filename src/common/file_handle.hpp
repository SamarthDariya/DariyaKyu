#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <span>

namespace dariyakyu {

// RAII wrapper around a POSIX file descriptor.
//
// Every file in dariyakyu is opened through this: segment logs, offset indexes,
// metadata checkpoints. It is move-only, because a descriptor has exactly one
// owner — closing one twice is a bug this makes unrepresentable.
//
// Note what is NOT here: no read-into-a-vector convenience. The hot read path
// never copies bytes into user space; it hands a descriptor and a byte range to
// sendfile (DESIGN.md decision 5). readAt() exists for headers, indexes, and
// recovery scans, not for serving consumers.
class FileHandle {
public:
    enum class Mode {
        ReadOnly,   // sealed segments, index lookups
        ReadWrite,  // the active segment, index appends; creates if missing
        CreateNew,  // fails if the file already exists — used when rolling
    };

    FileHandle() = default;
    FileHandle(const std::filesystem::path& path, Mode mode);
    ~FileHandle();

    FileHandle(FileHandle&& other) noexcept;
    FileHandle& operator=(FileHandle&& other) noexcept;
    FileHandle(const FileHandle&)            = delete;
    FileHandle& operator=(const FileHandle&) = delete;

    bool isOpen() const { return fd_ >= 0; }

    // Raw descriptor, for sendfile() and mmap(). Borrowed, never owned by the
    // caller — the FileHandle stays alive for as long as it is in use.
    int fd() const { return fd_; }

    const std::filesystem::path& path() const { return path_; }

    // Appends at the current end of file and returns the byte position it was
    // written at — which is exactly what the offset index needs to record
    // (DESIGN.md decision 10).
    //
    // Single-appender only: one partition's writer thread and nobody else.
    std::uint64_t append(std::span<const std::uint8_t> bytes);

    // Positional read. Uses pread(), so it moves no file cursor and is safe to
    // call concurrently from many threads on the same descriptor — which is
    // what makes sealed segments lock-free to read (DESIGN.md decision 20).
    //
    // Returns bytes actually read; short at end of file.
    std::size_t readAt(std::uint64_t position, std::span<std::uint8_t> out) const;

    std::uint64_t size() const { return writePosition_; }

    // Drops everything from newSize onward. Used by crash recovery, which
    // truncates the active segment at the first bad CRC (decision 13), and by
    // failover truncation (decision 16).
    void truncate(std::uint64_t newSize);

    // fsync. Deliberately rare — durability comes from replication, not from
    // forcing the platter (DESIGN.md decision 14).
    void sync();

    void close();

private:
    int                   fd_ = -1;
    std::filesystem::path path_;

    // Tracked rather than derived from lseek() so append() can report where it
    // wrote without a syscall, and so size() is free on the hot path.
    std::uint64_t writePosition_ = 0;
};

}  // namespace dariyakyu
