#include "storage/log.hpp"

#include <utility>

#include "common/errors.hpp"
#include "storage/record_batch.hpp"

using namespace std;

namespace dariyakyu::storage {

Log::Log(TopicPartition tp, filesystem::path dir, RollPolicy policy,
         unique_ptr<ActiveSegment> active)
    : tp_(std::move(tp)), dir_(std::move(dir)), policy_(policy), active_(std::move(active)) {
    // Both offsets start where the active segment does. For a new log that is
    // zero; for one being reopened it is wherever recovery left off.
    logEndOffset_.store(active_->nextOffset(), memory_order_relaxed);
    highWatermark_.store(active_->nextOffset(), memory_order_relaxed);
}

unique_ptr<Log> Log::create(TopicPartition tp, filesystem::path dir, RollPolicy policy) {
    error_code ec;
    filesystem::create_directories(dir, ec);
    if (ec) throw IoError("create_directories", dir, ec.value());

    // A new partition's first segment is based at offset 0 — the first record it
    // will ever hold. ActiveSegment::create refuses if a segment is already
    // there, so this cannot quietly adopt an existing partition.
    auto active = ActiveSegment::create(dir, Offset(0), policy);

    return unique_ptr<Log>(
        new Log(std::move(tp), std::move(dir), policy, std::move(active)));
}

Offset Log::append(span<uint8_t> batchBytes) {
    // The producer could not know this: it encoded before the broker had seen
    // the batch, so every offset inside it is a delta and the base is blank.
    // Assigning it here is what makes offsets a single global sequence per
    // partition rather than something clients negotiate.
    //
    // relaxed is enough for the load: only the appender writes this, so it is
    // reading its own last value.
    const Offset base = logEndOffset_.load(memory_order_relaxed);

    // Checked BEFORE the append, not after, so the decision is "is this segment
    // already full enough" rather than "did that last batch overflow it". A
    // segment therefore overshoots maxSegmentBytes by at most one batch, which
    // is why it is a threshold and not a hard cap.
    maybeRoll(wallClockMillis());

    RecordBatch::stampBaseOffset(batchBytes, base);
    // partitionLeaderEpoch is left at -1 until M8, where the same in-place trick
    // stamps the current leader epoch beside the offset.

    // No lock. This is deliberate, and it is the reason the write path is fast.
    //
    // active_ is only ever REASSIGNED by a roll, and a roll happens inside this
    // same function on this same thread — so the appender cannot race with
    // itself. Readers take a shared lock and rolling takes an exclusive one, so
    // a reader can never observe the pointer mid-swap. The appender needs no
    // lock because it is the only writer.
    active_->append(base, batchBytes);

    const Offset next = active_->nextOffset();

    // Release, so a reader that sees the new log end offset also sees the bytes
    // and the segment state behind it.
    logEndOffset_.store(next, memory_order_release);

    // Single node: nothing is replicated, so everything written is immediately
    // committed and visible. M7 is where this stops being a copy of the log end
    // offset and becomes the minimum across in-sync replicas — the point of it
    // being a separate number at all.
    highWatermark_.store(next, memory_order_release);

    return base;
}

void Log::maybeRoll(int64_t nowMs) {
    // Read outside the lock. Only the appender reassigns active_, and only from
    // this function, so this cannot be racing with a swap.
    if (!active_->shouldRoll(nowMs)) return;

    // The new segment starts exactly where the old one ended — which is what
    // makes the directory listing a contiguous sequence of base offsets.
    const Offset base = active_->nextOffset();

    // Created BEFORE the swap, and before the lock, for exception safety: if the
    // file already exists, or the disk is full, this throws while active_ is
    // still a perfectly good segment and the partition carries on.
    //
    // A crash in the gap leaves an empty segment for an offset the log has not
    // reached. Startup takes the highest base offset as the active segment, so
    // it adopts the empty one and treats the previous one as sealed — untrimmed
    // index and all, which OffsetIndex::openSealed already handles. Survivable,
    // which is the most that can be asked of a crash window.
    auto next = ActiveSegment::create(dir_, base, policy_);

    // Exclusive, and this is the only place in the write path that takes a lock.
    // Readers hold a shared lock across a whole read, so this waits for them —
    // and in exchange no reader can ever observe active_ mid-swap or hold a
    // pointer into a segment being closed.
    unique_lock lock(segmentsMutex_);

    // seal() consumes active_, leaving it null for the moment between these two
    // lines. The lock is what makes that moment unobservable.
    auto sealed = ActiveSegment::seal(std::move(active_));
    sealed_.emplace(sealed->baseOffset(), std::move(sealed));
    active_ = std::move(next);
}

Offset Log::logStartOffset() const {
    shared_lock lock(segmentsMutex_);

    // The oldest segment still on disk starts the log. std::map is ordered, so
    // begin() is the smallest base offset; with nothing sealed yet, the active
    // segment is the whole log.
    if (!sealed_.empty()) return sealed_.begin()->first;
    return active_->baseOffset();
}

size_t Log::segmentCount() const {
    shared_lock lock(segmentsMutex_);
    return sealed_.size() + 1;   // there is always exactly one active segment
}

void Log::setHighWatermark(Offset offset) {
    // Release, to pair with the acquire in highWatermark(): a consumer that sees
    // a raised watermark must also see the data it makes visible.
    highWatermark_.store(offset, memory_order_release);
}

}  // namespace dariyakyu::storage
