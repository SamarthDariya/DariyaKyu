#include "storage/log.hpp"

#include <utility>

#include "common/errors.hpp"

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
