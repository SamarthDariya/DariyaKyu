#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <string>

#include "common/file_handle.hpp"
#include "common/types.hpp"
#include "storage/offset_index.hpp"
#include "storage/record_batch.hpp"

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

    // Where the bytes for `offset` live — never the bytes themselves. The
    // network layer hands this straight to sendfile(), so the payload never
    // enters user space (DESIGN.md decision 13). If this ever returned a
    // vector<uint8_t>, zero-copy would be dead, which is why the return type
    // enforces it rather than a comment asking politely.
    //
    // The range starts exactly on a batch boundary — the batch CONTAINING
    // `offset`, which may begin earlier than it. A batch is the unit of
    // transfer, so a consumer asking for offset 503 of a batch spanning 500..504
    // receives the whole batch from 500 and skips what it already has.
    //
    // The range's END is not batch-aligned: it is capped at `maxBytes` wherever
    // that falls. Aligning it would mean parsing a header per batch in the
    // range — one pread each, so a thousand syscalls for a 1 MB fetch of small
    // batches — to save the consumer a check it has to make anyway. So the fetch
    // contract is Kafka's: a response may end mid-batch, and the consumer
    // discards an incomplete trailing batch rather than treating it as damage.
    //
    // Except that a range ALWAYS carries at least one whole batch when one is
    // available, even if that exceeds maxBytes. Without that, a consumer whose
    // fetch size is smaller than the next batch would poll forever and never
    // advance — a permanent stall from a merely conservative setting.
    //
    // A zero-length range means "nothing here yet": the caller has caught up
    // with what this segment holds. That is a success, and it is the most common
    // read in the entire system — every caught-up consumer, several times a
    // second, forever. It must not cost an exception.
    //
    // An offset below the segment's base offset throws: Log chose this segment,
    // so getting here with an offset it cannot hold is a routing bug.
    FileRange read(Offset offset, std::size_t maxBytes) const;

    // One batch's header, plus how far it is to the next one.
    struct BatchAt {
        BatchHeader   header;
        std::uint64_t position  = 0;   // where this batch starts
        std::size_t   totalSize = 0;   // add to position to reach the next batch
    };

    // The batch beginning at `position`, reading its header only — never a record
    // body. This is the step that walks a segment: call it, use the header, add
    // totalSize, call it again.
    //
    // `limit` is how far the caller considers the file to extend, and it is a
    // parameter rather than read from the file each time on purpose: a caller
    // walking a segment takes ONE snapshot of the size and passes it to every
    // call, so a batch the appender adds part-way through the walk cannot be
    // half-included.
    //
    // Returns nothing — not an error — when fewer than a whole batch remains
    // before `limit`. On an active segment that is simply a write in flight, and
    // treating it as absent is what lets a reader run alongside the appender
    // with no lock. On a damaged file it means "the good data ends here", which
    // is the same action recovery wants to take.
    //
    // Throws CorruptData if there ARE enough bytes but they do not parse — a bad
    // magic byte, or a length below the header size. `position` must be a real
    // batch boundary; called mid-batch it will read a header out of record data
    // and almost certainly throw.
    std::optional<BatchAt> batchAt(std::uint64_t position, std::uint64_t limit) const;

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

    // Learns nextOffset and largestTimestamp by reading the log, for a segment
    // opened from disk rather than built by appending.
    //
    // Neither number is stored anywhere: the .log file is the only authority on
    // what a segment contains, so there is no second copy to fall out of sync
    // with it or to be trusted when it should not be.
    //
    // It does not read the whole file. It starts at the LAST index entry and
    // walks headers from there to the end, which is bounded by one index
    // interval — a page or two — because a segment rolls when its index fills.
    // Remove that roll trigger and this becomes a full scan of every segment at
    // every startup.
    void deriveEndStateFromLog();

    FileHandle                log_;
    OffsetIndex               index_;
    Offset                    baseOffset_{0};
    std::atomic<Offset>       nextOffset_{Offset{0}};
    std::atomic<std::int64_t> largestTimestamp_{-1};
};

// A segment that will never be written to again. Immutable, which is what makes
// it lock-free to read and safe for retention to delete underneath a reader.
class SealedSegment final : public SegmentBase {
public:
    // Opens an existing segment and TRUSTS its bytes. No checksum scan: sealing
    // already validated them, and re-verifying every sealed segment at startup
    // is exactly the unbounded recovery that segmenting the log exists to avoid.
    // Only the active segment gets scanned.
    static std::unique_ptr<SealedSegment> open(const std::filesystem::path& logFile);

    // Retention: removes this segment's two files from the directory.
    //
    // UNLINK, not close. On POSIX, removing a directory entry does not free the
    // inode while a descriptor is still open on it — so a consumer part-way
    // through a sendfile from this segment keeps reading valid bytes, and the
    // space comes back when this object is destroyed. That is what makes
    // retention safe to run while readers are working, with no lock between
    // them.
    //
    // Does not throw. A sweep across every partition must not abort because one
    // file could not be removed, and retention is idempotent — the next sweep
    // retries.
    void unlinkFiles();

private:
    SealedSegment(FileHandle log, OffsetIndex index, Offset baseOffset);
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

    // Whether this segment should be sealed and a new one started.
    //
    // `nowMs` is passed in rather than read from the clock here. Two reasons:
    // tests can advance time without sleeping, and M3's maintenance thread
    // sweeps every partition on the broker — it reads the clock once and passes
    // the same instant to all of them, rather than making a syscall per
    // partition and comparing against slightly different "now"s.
    //
    // Advisory. Log decides when to act on it, which is why a single oversized
    // batch can still push a segment past maxSegmentBytes.
    bool shouldRoll(std::int64_t nowMs) const;

    // fsync the log. One of the very few sync points in the system — durability
    // comes from replication across machines, not from forcing one machine's
    // platter (DESIGN.md decision 14).
    void flush();

    // Consumes the active segment and yields an immutable one.
    //
    // Taking the unique_ptr BY VALUE is the guarantee, not a style choice. After
    // this returns, the caller's pointer is null and the ActiveSegment object no
    // longer exists — so a writable handle to these bytes cannot be held,
    // because there is nothing left to hold one with. Immutability by ownership
    // rather than by discipline.
    //
    // Implemented by closing everything and reopening through
    // SealedSegment::open, so a segment sealed at runtime and a segment
    // discovered at startup travel exactly the same code path. Costs two open()
    // calls, once per segment — which is once per gigabyte.
    static std::unique_ptr<SealedSegment> seal(std::unique_ptr<ActiveSegment> active);

    // The policy this segment was created with. Held rather than passed in per
    // call, so append() and shouldRoll() cannot disagree about the interval or
    // the size limit.
    const RollPolicy& policy() const { return policy_; }

private:
    ActiveSegment(FileHandle log, OffsetIndex index, Offset baseOffset,
                  const RollPolicy& policy);

    RollPolicy policy_;

    // Log bytes written since the last index entry. The sparseness counter: an
    // entry goes in once this reaches policy_.indexIntervalBytes, then it resets.
    // Starting at zero is what keeps the segment's first batch out of the index.
    std::uint64_t bytesSinceIndexEntry_ = 0;

    // Wall clock at the FIRST append, or -1 while the segment is empty. Age is
    // measured from here, so a segment that sat empty for a week is not
    // instantly stale the moment it receives a record.
    std::int64_t firstAppendMs_ = -1;
};

}  // namespace dariyakyu::storage
