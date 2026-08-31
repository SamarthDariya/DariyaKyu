#pragma once

#include <atomic>
#include <cstddef>
#include <filesystem>
#include <map>
#include <memory>
#include <shared_mutex>
#include <span>
#include <type_traits>

#include "common/types.hpp"
#include "storage/segment.hpp"

namespace dariyakyu::storage {

// What a read of a partition can go wrong in.
//
// A read has three outcomes and they are one integer apart from each other:
//
//   offset <  logStartOffset   aged out by retention   -> BelowLogStart
//   offset == logEndOffset     caught up, nothing new  -> None, zero bytes
//   offset >  logEndOffset     asking for the future   -> AboveLogEnd
//
// The middle row is why this is a returned value and not an exception. It is the
// most common read in the entire system: every caught-up consumer polls for an
// offset that does not exist yet, several times a second, forever. Throwing
// there would mean allocating and unwinding on the hot path for the normal case.
//
// The two error rows have to stay distinguishable, because a client's reset
// policy treats them differently: below the log start means "you are too slow,
// jump to the earliest available offset", while above the log end means the
// client is confused or the log was truncated under it by a failover — a
// situation that needs a different recovery, not a silent skip forward.
//
// Deliberately NOT a wire error code. Storage never learns what a protocol is;
// M4's request handler maps these to whatever the response format calls them
// (DESIGN.md decision 12).
enum class ReadError {
    None,
    BelowLogStart,
    AboveLogEnd,
};

// Where a read's bytes are, or why there are none.
//
// Note that success and "there are bytes" are different questions. A caught-up
// read is a SUCCESS carrying a zero-length range, so callers ask ok() to find
// out whether anything went wrong and range.empty() to find out whether there is
// anything to send.
struct ReadResult {
    ReadError error = ReadError::None;
    FileRange range{};

    bool ok() const { return error == ReadError::None; }
};

// Returned by value from the hot read path, so it must stay cheap to copy: no
// allocation, no destructor, small enough to travel in registers.
static_assert(std::is_trivially_copyable_v<ReadResult>,
              "ReadResult is returned by value on every fetch; it must not acquire anything "
              "that needs copying or destroying");

// One partition's log: a directory of segments, and the rules for moving between
// them.
//
// # Why a std::map
//
// Sealed segments live in a map keyed by base offset, so finding the segment that
// holds an offset is upper_bound() then step back — DESIGN.md decision 11's
// "binary search over filenames", in two lines of standard library. An
// unordered_map would need a linear scan for exactly the query that matters.
//
// # Why the locking looks so thin
//
// One shared_mutex, and it guards the MAP — never file contents.
//
// That works because sealed segments are immutable: their bytes cannot change,
// so reading them needs no synchronisation at all. The lock is held only while
// the map itself is being modified, which happens when a segment rolls or
// retention deletes one — rarely, and never per read.
//
// The two offsets are atomics rather than mutex-guarded for the same reason.
// One appender publishes them, many readers consume them, and a reader that
// wants the log end offset should not have to queue behind a roll
// (DESIGN.md decision 20).
class Log {
public:
    // A brand-new, empty partition: creates the directory and one active segment
    // based at offset 0.
    static std::unique_ptr<Log> create(TopicPartition tp, std::filesystem::path dir,
                                       RollPolicy policy);

    // Adopts a partition directory that already exists — the startup path.
    //
    // The newest segment is the only one that could have been mid-write when the
    // process died, so it is the only one recovered: its checksums are walked and
    // it is truncated at the first failure. Every older segment was sealed, and is
    // opened and trusted. That asymmetry is the point of segmenting the log at
    // all — startup costs one segment's worth of work, not one partition's.
    //
    // A directory with no segments in it yet is treated as a new partition, so
    // this is safe to call on a half-created one.
    static std::unique_ptr<Log> open(TopicPartition tp, std::filesystem::path dir,
                                     RollPolicy policy);

    // Appends one batch and returns the offset assigned to its first record.
    //
    // The span is NON-const, and that is load-bearing: this function stamps the
    // assigned base offset into the batch header in place. Twelve bytes, and the
    // only write the broker ever makes to a producer's bytes. A const span would
    // force a full copy of every batch, which is precisely the cost the whole
    // design exists to avoid.
    //
    // Legal because the v2 layout puts baseOffset and partitionLeaderEpoch
    // BEFORE the crc field, so the checksum does not cover them and does not
    // need recomputing — over a body the broker may not even be able to read.
    //
    // Single-appender only: one thread per partition.
    Offset append(std::span<std::uint8_t> batchBytes);

    // Where the bytes for `offset` live, or why there are none.
    //
    // Resolves a location only — the blocking read happens later, on an I/O
    // thread, when the network layer hands this range to sendfile.
    //
    // Bounded by the LOG END OFFSET, not the high watermark. On a single node
    // they are equal so it makes no difference, but the distinction matters at
    // M7: a follower fetching for replication must be able to read right up to
    // the leader's log end, while a consumer must not see past the high
    // watermark. So the watermark clamp belongs to the caller that knows which
    // of the two it is serving, not here.
    ReadResult read(Offset offset, std::size_t maxBytes) const;

    // Seals the active segment and starts a new one, if the roll policy says so.
    //
    // append() calls this itself, so the write path needs nothing extra. It is
    // public because M3's maintenance thread must ALSO call it: a partition
    // receiving no writes would otherwise never re-evaluate its age, so its
    // active segment would never seal, so retention — which only deletes sealed
    // segments — would never free anything on an idle topic.
    void maybeRoll(std::int64_t nowMs);

    // The earliest offset still on disk. Rises as retention deletes segments,
    // which is why it is a question about the segment map rather than a stored
    // number.
    Offset logStartOffset() const;

    // One past the last offset written. Atomic, so this costs no lock.
    Offset logEndOffset() const { return logEndOffset_.load(std::memory_order_acquire); }

    // The highest offset replicated widely enough for consumers to see. On a
    // single node it simply tracks the log end offset; M7 is where the two
    // diverge and the distinction starts doing work.
    Offset highWatermark() const { return highWatermark_.load(std::memory_order_acquire); }
    void   setHighWatermark(Offset offset);

    // Discards every record at or after `offset`, so the log ends there.
    //
    // This is failover truncation (M8). When a broker rejoins and discovers its
    // log diverged from the new leader's, the records it has that the leader does
    // not never existed as far as the cluster is concerned, and they have to go
    // before it can start following.
    //
    // Batches are never split, so the resulting log end offset is at most
    // `offset` and may be lower. Truncating to at or above the current end does
    // nothing.
    void truncateTo(Offset offset);

    std::size_t segmentCount() const;

    const TopicPartition&        topicPartition() const { return tp_; }
    const std::filesystem::path& directory() const { return dir_; }
    const RollPolicy&            policy() const { return policy_; }

    Log(const Log&)            = delete;
    Log& operator=(const Log&) = delete;

private:
    Log(TopicPartition tp, std::filesystem::path dir, RollPolicy policy,
        std::map<Offset, std::unique_ptr<SealedSegment>> sealed,
        std::unique_ptr<ActiveSegment> active);

    TopicPartition        tp_;
    std::filesystem::path dir_;
    RollPolicy            policy_;

    std::map<Offset, std::unique_ptr<SealedSegment>> sealed_;
    std::unique_ptr<ActiveSegment>                   active_;

    std::atomic<Offset> logEndOffset_{Offset{0}};
    std::atomic<Offset> highWatermark_{Offset{0}};

    // mutable, so const readers can take a shared lock. Guards the map and the
    // active_ pointer — not the bytes either of them describes.
    mutable std::shared_mutex segmentsMutex_;
};

}  // namespace dariyakyu::storage
