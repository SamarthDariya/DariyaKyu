#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <filesystem>

#include "common/mapped_file.hpp"
#include "common/types.hpp"

namespace dariyakyu::storage {

// The translation from "record number 4,500,000" to "byte 2,847,232 of this
// .log file". One index accompanies one segment, as `<baseOffset>.index`.
//
// It is *sparse* on purpose (DESIGN.md decision 11). An entry is written once
// per index interval of log bytes — 4 KB by default — not once per record, so a
// 1 GB segment costs a couple of megabytes of index rather than hundreds. A
// lookup therefore lands slightly *before* the target and the caller scans
// forward over batch headers from there. That is the whole bargain: the index
// narrows a gigabyte down to a few kilobytes, and the log itself does the last
// step.
//
// Which also means every entry is a *hint*, never a fact the reader depends on.
// Losing entries costs a longer forward scan and nothing else. That single
// property is why so much of this class is allowed to be lenient where the
// record batch codec is strict:
//
//   - nothing here is checksummed
//   - nothing here is fsynced
//   - a torn or stale index is discarded and rebuilt by walking the log
//
// The interval logic itself lives one layer up, in ActiveSegment, which knows
// how many bytes it has written since the last entry. This class stores what it
// is told and searches it.
//
// # Entry layout
//
//   relativeOffset  uint32   offset - baseOffset
//   position        uint32   byte position in the .log file
//
// Eight fixed bytes, so entry N lives at byte 8N and a binary search is pure
// arithmetic on a mapped file rather than a pread per probe.
//
// Relative rather than absolute offsets halves the entry: the absolute offset
// would be an int64 that shares its top bits with every other entry in the
// segment, and those bits are already in the filename. Halving the entry
// doubles the entries per page, which is the number that decides how many page
// faults a lookup costs.
//
// The two uint32s bound a segment at 4 GiB of bytes and 4 Gi records, both far
// above any sane roll policy; append rejects anything that would not fit rather
// than truncating it into a plausible wrong answer.
//
// Integers are stored big-endian, like everything else this codebase writes to
// disk. Native-endian would allow overlaying the mapping as an array of Entry
// directly, and was rejected: a data directory copied to a machine of the other
// endianness would then binary search comfortably over transposed garbage and
// hand out byte positions that point anywhere. That failure is silent, and it
// surfaces as corrupt data served to a consumer. Eight byte-swaps per lookup is
// not a price worth arguing about.
//
// # Concurrency
//
// One appender — the partition's I/O thread — and any number of concurrent
// readers, with no lock between them (DESIGN.md decision 20). `entryCount_` is
// the publication point: the appender writes an entry's bytes and *then*
// releases the new count, so a reader that acquires the count sees complete
// entries only. A reader never looks past the count it loaded, which is why a
// half-written entry is unobservable rather than merely unlikely.
class OffsetIndex {
public:
    struct Entry {
        std::uint32_t relativeOffset = 0;
        std::uint32_t position       = 0;

        // Same shape as Record::offsetFrom — an entry knows its distance from
        // the segment's base, and the caller supplies the base.
        Offset offsetFrom(Offset baseOffset) const {
            return baseOffset + static_cast<std::int64_t>(relativeOffset);
        }
    };

    static constexpr std::size_t kEntrySize = 8;

    // A fresh index for a new segment.
    //
    // `maxBytes` is preallocated up front and the mapping never grows, because
    // extending a live mapping mid-append is exactly the SIGBUS that
    // MappedFile's contract exists to avoid. Rounded down to a whole number of
    // entries; a value below one entry is a configuration error.
    //
    // Any existing file at `path` is truncated to zero first. A segment that
    // rolled and then crashed before its first append can leave a stale index
    // behind, and inheriting entries that describe a different segment's bytes
    // would be worse than having none.
    static OffsetIndex create(const std::filesystem::path& path, Offset baseOffset,
                              std::size_t maxBytes);

    // An existing index, mapped read-only, for a sealed segment. Immutable in
    // the type system for the same reason SealedSegment is: a stray write would
    // corrupt it silently and dirty pages the kernel then writes back for
    // nothing.
    static OffsetIndex openSealed(const std::filesystem::path& path, Offset baseOffset);

    OffsetIndex() = default;

    OffsetIndex(OffsetIndex&&) noexcept;
    OffsetIndex& operator=(OffsetIndex&&) noexcept;
    OffsetIndex(const OffsetIndex&)            = delete;
    OffsetIndex& operator=(const OffsetIndex&) = delete;

    // Records that `offset` begins at byte `position` of the .log file.
    //
    // Offsets and positions must both strictly increase. A caller that breaks
    // that has a bug in its own bookkeeping, not bad data on disk, so this
    // throws OffsetInvariantViolated rather than CorruptData.
    //
    // A full index is NOT an error: the entry is dropped and the index simply
    // becomes sparser, which costs a longer forward scan and stays correct.
    // Failing a produce request over a benign capacity limit would be the worse
    // outcome by far. `isFull()` exists so the roll policy can seal the segment
    // before that starts happening.
    void append(Offset offset, std::uint32_t position);

    // The greatest entry at or before `target` — where to start scanning the
    // .log file for it.
    //
    // Total, never optional. When the index holds nothing at or before the
    // target the answer is the segment's own beginning, `{0, 0}`, which is a
    // correct place to start scanning from and needs no special case in the hot
    // read path. That covers a segment younger than its first index interval,
    // and an index emptied by recovery.
    //
    // `target` below the segment's base offset is a routing bug in Log, which
    // chose this segment — so that, unlike the above, throws.
    Entry lookup(Offset target) const;

    // Final flush, then cut the file back to the entries actually written. A
    // preallocated 10 MB index holding 300 real entries is mostly zeroes until
    // this runs.
    //
    // This ENDS the object's life. The mapping is gone, so a later lookup would
    // be reading unmapped pages; rather than document that and hope, the index
    // marks itself spent and every subsequent append or lookup throws. Same
    // idiom as BufferWriter::take(), and safe because Log holds its exclusive
    // lock across sealing — no reader can be mid-lookup here.
    //
    // Sealing reopens the trimmed file through openSealed().
    void flushAndTrim();

    bool isSpent() const { return spent_; }

    Offset      baseOffset() const { return baseOffset_; }
    std::size_t entryCount() const { return entryCount_.load(std::memory_order_acquire); }
    bool        isEmpty() const { return entryCount() == 0; }
    bool        isFull() const { return entryCount() >= maxEntries_; }
    std::size_t maxEntries() const { return maxEntries_; }
    std::size_t usedBytes() const { return entryCount() * kEntrySize; }
    bool        isWritable() const { return map_.isWritable(); }

    const std::filesystem::path& path() const { return map_.path(); }

private:
    OffsetIndex(MappedFile map, Offset baseOffset, std::size_t entryCount,
                std::size_t maxEntries);

    void  requireLive() const;
    Entry entryAt(std::size_t index) const;

    MappedFile               map_;
    Offset                   baseOffset_{0};
    std::atomic<std::size_t> entryCount_{0};
    std::size_t              maxEntries_ = 0;
    bool                     spent_      = false;
};

static_assert(sizeof(OffsetIndex::Entry) == OffsetIndex::kEntrySize,
              "an index entry is eight bytes on disk; the in-memory struct is kept the same "
              "size so entry N is unambiguously at byte 8N");

}  // namespace dariyakyu::storage
