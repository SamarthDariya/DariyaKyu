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
