#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <string>

#include "common/file_handle.hpp"
#include "common/types.hpp"
#include "storage/offset_index.hpp"

namespace dariyakyu::storage {

// A segment's two files are named for the offset of their first record:
//
//   00000000000000000000.log     00000000000000000000.index
//   00000000000001073741.log     00000000000001073741.index
//
// Twenty zero-padded digits, so that sorting the names as STRINGS gives the same
// order as sorting the offsets as NUMBERS. Without padding, "9.log" sorts after
// "10.log", because '9' > '1' as a character. With it, "…009" sorts before
// "…010" and the two orders agree.
//
// That is the property Log will rely on: to find which segment holds an offset,
// it searches the filenames, and only then opens that segment's index. Twenty
// digits because the largest int64 is 19 digits, plus one to spare.

// Offset(1073741) -> "00000000000001073741"
std::string segmentBaseName(Offset baseOffset);

// The two paths inside a partition directory.
std::filesystem::path segmentLogPath(const std::filesystem::path& dir, Offset baseOffset);
std::filesystem::path segmentIndexPath(const std::filesystem::path& dir, Offset baseOffset);

// The reverse: "…/00000000000001073741.log" -> Offset(1073741).
//
// Strict, and throws CorruptData on anything that is not exactly twenty digits
// followed by ".log". M3's LogManager will iterate a partition directory it does
// not fully own — partition.meta, leader-epoch-checkpoint, an editor's swap
// file, whatever someone drops in. A name this cannot interpret must not
// silently become Offset(0), because a phantom segment claiming to start at
// offset 0 would shadow the real first segment and hide records.
Offset baseOffsetFromLogPath(const std::filesystem::path& logFile);

// When to stop writing to a segment and start a new one.
//
// Rolling is what bounds three separate things, and a partition that never rolls
// breaks all of them:
//
//   - retention deletes whole segments, so a log with one segment never shrinks
//   - recovery only scans the newest segment, so one that grows forever makes
//     startup time unbounded
//   - only the newest segment is mutable, so a log with one segment has nothing
//     that can be read without synchronisation
struct RollPolicy {
    // Kafka's default, and it is a trade rather than a magic number: bigger
    // segments mean fewer files and less frequent rolling, but retention can
    // only free space a whole segment at a time, so a 1 GiB segment means
    // deletion happens in 1 GiB steps.
    std::uint64_t maxSegmentBytes = 1ull << 30;   // 1 GiB

    // Age of the segment's FIRST append, measured on the wall clock — not the
    // largest record timestamp. Record timestamps are chosen by producers, and
    // letting a client's clock decide the broker's file layout means one skewed
    // producer can hold a segment open forever, or force a roll per record.
    //
    // A low-traffic partition needs this: without it, a topic receiving one
    // record a day would keep the same active segment for years, and since
    // retention only touches sealed segments, nothing would ever be deleted.
    std::int64_t maxSegmentAgeMs = 7ll * 24 * 60 * 60 * 1000;   // 7 days

    // Bytes of log between index entries — the sparseness knob. One entry per
    // 4 KB means a lookup lands within 4 KB of its target, so the forward scan
    // that finishes the job touches a page or two.
    //
    // Smaller: faster lookups, bigger index. Larger: the opposite. 4 KB is one
    // page, which is the natural unit for a scan the kernel will fault in.
    std::uint64_t indexIntervalBytes = 4096;

    // Preallocated up front, because extending a live mmap and then touching the
    // new pages raises SIGBUS. Trimmed back to the entries actually used when
    // the segment is sealed.
    std::size_t maxIndexBytes = 10ull << 20;   // 10 MiB
};

// The index must be able to describe a full-size segment without filling up.
//
// A full index forces a roll — everything claiming to be "bounded by one index
// interval" stops being bounded once entries are dropped — so if these defaults
// were mismatched, segments would roll on a full index long before reaching
// maxSegmentBytes, and the size knob would silently do nothing.
//
//   10 MiB / 8 bytes  = 1,310,720 entries
//   1,310,720 x 4 KiB = 5 GiB of log covered, against a 1 GiB segment
static_assert((RollPolicy{}.maxIndexBytes / OffsetIndex::kEntrySize) *
                      RollPolicy{}.indexIntervalBytes >
                  RollPolicy{}.maxSegmentBytes,
              "the default index is too small to describe a default-sized segment, so segments "
              "would roll on a full index rather than on their size");

// Everything a segment can do that does not depend on whether it is still being
// written: report where it starts and ends, and resolve an offset to a location.
//
// Inherited purely to share that machinery. NOT polymorphic — there are no
// virtual functions, because Log always knows which of the two kinds it is
// holding and dispatches on an offset comparison rather than a vtable. The
// destructor is protected to make that safe: `delete` through a SegmentBase*
// does not compile, so the missing virtual destructor cannot bite.
class SegmentBase {
public:
    Offset baseOffset() const { return baseOffset_; }

    // One past the last offset this segment holds.
    //
    // On an active segment this grows under the appender's hand while readers
    // are looking at it, so it is an atomic and this load uses acquire
    // ordering. Paired with the release store in append(), that guarantees a
    // reader who sees the new offset also sees the bytes behind it — without
    // the pairing a reader could observe the larger offset while the data was
    // still invisible, and read zeroes out of a file it had just measured.
    Offset nextOffset() const { return nextOffset_.load(std::memory_order_acquire); }

    // Bytes written. FileHandle tracks this in its own atomic for the same
    // reason, so this is free and needs no syscall.
    std::uint64_t sizeBytes() const { return log_.size(); }

    // The largest record timestamp seen, or -1 for an empty segment. M3's
    // retention uses this — deleting by age is the one place where the
    // producer's notion of time is the right one.
    std::int64_t largestTimestamp() const {
        return largestTimestamp_.load(std::memory_order_acquire);
    }

    bool isEmpty() const { return nextOffset() == baseOffset_; }

    // Half-open: the last offset a segment holds is nextOffset() - 1.
    bool contains(Offset offset) const { return offset >= baseOffset_ && offset < nextOffset(); }

    // Borrowed, for sendfile() and for tests. The FileHandle stays alive as long
    // as this segment does.
    int                          fd() const { return log_.fd(); }
    const std::filesystem::path& logFilePath() const { return log_.path(); }
    const OffsetIndex&           index() const { return index_; }

    // A segment owns a file descriptor and a memory mapping. Copying one would
    // mean two owners of both.
    SegmentBase(const SegmentBase&)            = delete;
    SegmentBase& operator=(const SegmentBase&) = delete;

protected:
    // FileHandle and OffsetIndex are both move-only, so they arrive by value and
    // are moved into place. Only the two derived classes call this.
    SegmentBase(FileHandle log, OffsetIndex index, Offset baseOffset);
    ~SegmentBase() = default;

    FileHandle                log_;
    OffsetIndex               index_;
    Offset                    baseOffset_{0};
    std::atomic<Offset>       nextOffset_{Offset{0}};
    std::atomic<std::int64_t> largestTimestamp_{-1};
};

// The one writable segment in a partition. Everything that mutates a log lives
// here, which is why there is exactly one of these per partition and why only
// one thread ever touches it.
class ActiveSegment final : public SegmentBase {
public:
    // A brand-new segment: creates both files in `dir` and preallocates the
    // index to policy.maxIndexBytes.
    //
    // Fails if the .log already exists. Rolling to an offset that already has a
    // segment means the caller's offset bookkeeping is wrong, and appending into
    // that file would interleave two segments' records into one log — a
    // corruption no checksum would catch, because every individual batch would
    // still be intact.
    static std::unique_ptr<ActiveSegment> create(const std::filesystem::path& dir,
                                                 Offset baseOffset, const RollPolicy& policy);

    // Appends one batch, whose header must already carry `baseOffset` — Log
    // stamps it before calling (that stamp is the only write the broker ever
    // makes to producer bytes).
    //
    // Single-appender only: this partition's I/O thread and nobody else. Two
    // threads here would interleave bytes inside a batch, and no checksum would
    // survive it.
    //
    // The batch must begin exactly where the segment left off. A gap or an
    // overlap is a bug in Log's bookkeeping rather than bad data on disk, so it
    // throws OffsetInvariantViolated rather than CorruptData.
    void append(Offset baseOffset, std::span<const std::uint8_t> batchBytes);

    // The policy this segment was created with. Held rather than passed in per
    // call, so append() and shouldRoll() cannot disagree about the interval or
    // the size limit.
    const RollPolicy& policy() const { return policy_; }

private:
    ActiveSegment(FileHandle log, OffsetIndex index, Offset baseOffset,
                  const RollPolicy& policy);

    RollPolicy policy_;
};

}  // namespace dariyakyu::storage
