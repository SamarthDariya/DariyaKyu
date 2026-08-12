#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <span>

namespace dariyakyu {

// RAII wrapper around mmap(2).
//
// Used for offset index files. An index is an array of fixed-size entries that
// we binary search, and mapping it makes that search pure pointer arithmetic
// rather than a pread per probe. It also lets the OS decide what stays
// resident: hot partitions keep their index cached, cold ones get evicted, and
// we never write a cache of our own (DESIGN.md decision 10).
//
// A read-write mapping is created at a fixed length and never grows, because
// index files are preallocated — touching a mapped page past the end of a file
// raises SIGBUS, which is a signal rather than an exception and therefore not
// something to catch. Sealing a segment trims the file back to the bytes
// actually used.
//
// Mappings come in two kinds, mirroring the ActiveSegment / SealedSegment split
// one layer up: the active segment's index is written, and every sealed
// segment's index is immutable. Mapping an immutable index writable would let a
// stray write corrupt it silently and would dirty pages the kernel then writes
// back for no reason.
//
// Note that an index never needs to be durable. Every entry in it can be
// rebuilt by walking the log, so a torn or stale index after a crash is not a
// data-loss problem — recovery simply discards the active segment's index and
// rebuilds it. That is why nothing here msyncs on the write path and why index
// entries carry no checksum.
class MappedFile {
public:
    MappedFile() = default;

    // Creates `path` if needed, grows it to `length`, and maps all of it
    // read-write and shared.
    MappedFile(const std::filesystem::path& path, std::size_t length);

    // Maps an existing file read-only, in its entirety. Writing through the
    // returned bytes is a segfault at the point of the bug rather than silent
    // corruption discovered later.
    //
    // A zero-length file is valid and yields an empty mapping rather than an
    // error: a segment that rolled on age with less than one index interval of
    // data in it has no index entries at all, and its trimmed index file is 0
    // bytes. Refusing that would fail to open a perfectly good partition on any
    // low-traffic topic after a restart.
    static MappedFile openReadOnly(const std::filesystem::path& path);

    ~MappedFile();

    MappedFile(MappedFile&& other) noexcept;
    MappedFile& operator=(MappedFile&& other) noexcept;
    MappedFile(const MappedFile&)            = delete;
    MappedFile& operator=(const MappedFile&) = delete;

    // False for an empty read-only mapping, which is a legitimate state: there
    // are no pages because there is nothing in the file. bytes() is still safe
    // to call and returns an empty span.
    bool isMapped() const { return addr_ != nullptr; }
    bool isWritable() const { return writable_; }

    std::span<const std::uint8_t> bytes() const;

    // Throws if the mapping is read-only, so the mistake surfaces as an error
    // naming the file rather than as a signal.
    std::span<std::uint8_t> mutableBytes();

    std::size_t size() const { return length_; }

    const std::filesystem::path& path() const { return path_; }

    // msync. Rare and deliberate, never per-write — see the note above about
    // indexes being rebuildable.
    void flush();

    // Unmaps and truncates the file to `usedBytes`. This is what seal() calls: a
    // preallocated index holding 300 real entries is mostly zeroes until now.
    void unmapAndTrim(std::size_t usedBytes);

private:
    // A bool parameter at a call site says nothing; the enum carries the meaning
    // in the type.
    enum class Access { ReadOnly, ReadWrite };

    MappedFile(const std::filesystem::path& path, std::size_t length, Access access);
    void unmap();

    void*                 addr_     = nullptr;
    std::size_t           length_   = 0;
    bool                  writable_ = false;
    std::filesystem::path path_;
};

}  // namespace dariyakyu
