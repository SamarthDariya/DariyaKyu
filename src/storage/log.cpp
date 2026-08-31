#include "storage/log.hpp"

#include <cerrno>
#include <utility>

#include "common/errors.hpp"
#include "storage/record_batch.hpp"

using namespace std;

namespace dariyakyu::storage {

Log::Log(TopicPartition tp, filesystem::path dir, RollPolicy policy,
         map<Offset, unique_ptr<SealedSegment>> sealed, unique_ptr<ActiveSegment> active)
    : tp_(std::move(tp)),
      dir_(std::move(dir)),
      policy_(policy),
      sealed_(std::move(sealed)),
      active_(std::move(active)) {
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
        new Log(std::move(tp), std::move(dir), policy, {}, std::move(active)));
}

unique_ptr<Log> Log::open(TopicPartition tp, filesystem::path dir, RollPolicy policy) {
    if (!filesystem::is_directory(dir)) throw IoError("open", dir, ENOENT);

    // Only .log files. A partition directory also holds .index files and, from
    // M3, partition.meta and leader-epoch-checkpoint — so the scan picks out the
    // segments and ignores everything else. A file that DOES end in .log but is
    // not named for an offset throws from baseOffsetFromLogPath rather than being
    // skipped: it is either a bug or tampering, and guessing would be worse.
    map<Offset, filesystem::path> logs;
    for (const auto& entry : filesystem::directory_iterator(dir))
        if (entry.path().extension() == ".log")
            logs.emplace(baseOffsetFromLogPath(entry.path()), entry.path());

    // Nothing here yet — a directory created but never written to, or a partition
    // interrupted between mkdir and its first segment.
    //
    // The segment is built into a named local FIRST. Inlining it into the Log
    // constructor call would put `ActiveSegment::create(dir, ...)` and
    // `std::move(dir)` in the same argument list, and argument evaluation order
    // is unspecified — so the move could run first, leaving `dir` empty and
    // creating the segment in the process's working directory instead of the
    // partition. Exactly the hazard Log::create documents, in a second place.
    if (logs.empty()) {
        auto active = ActiveSegment::create(dir, Offset(0), policy);
        return unique_ptr<Log>(
            new Log(std::move(tp), std::move(dir), policy, {}, std::move(active)));
    }

    // The map is ordered, so the last key is the newest segment.
    const auto newest = prev(logs.end());

    // Sealed segments first, deliberately. They are cheap to open, so a broken
    // one fails before paying for the newest segment's full checksum scan.
    map<Offset, unique_ptr<SealedSegment>> sealed;
    for (auto it = logs.begin(); it != newest; ++it)
        sealed.emplace(it->first, SealedSegment::open(it->second));

    // Contiguity. Each segment must begin exactly where the previous one ended,
    // or a segment file has been removed by hand and there is a hole in the
    // offset sequence. Reads would silently skip it — every consumer crossing the
    // gap would jump forward and never know — so it is refused instead.
    for (auto it = sealed.begin(); it != sealed.end(); ++it) {
        const auto following = next(it);
        const Offset expected =
            (following == sealed.end()) ? newest->first : following->first;
        if (it->second->nextOffset() != expected)
            throw CorruptData("log " + dir.string() + ": segment " +
                              it->first.toString() + " ends at " +
                              it->second->nextOffset().toString() + " but the next begins at " +
                              expected.toString() + " — a segment file is missing");
    }

    auto active = ActiveSegment::recover(newest->second, policy);

    return unique_ptr<Log>(new Log(std::move(tp), std::move(dir), policy, std::move(sealed),
                                   std::move(active)));
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

ReadResult Log::read(Offset offset, size_t maxBytes) const {
    // Shared, so any number of readers proceed at once. Held across the whole
    // call, including the segment's forward scan — which does read the file, but
    // only batch headers, and only within one index interval. What the lock
    // actually protects is the segment map and the active_ pointer: it stops a
    // roll from closing a segment this read is standing on.
    shared_lock lock(segmentsMutex_);

    const Offset end = logEndOffset_.load(memory_order_acquire);

    // Asking for the future. Either the client is confused, or the log was
    // truncated under it by a failover — which needs a different recovery from
    // ordinary lag, so it must stay distinguishable from BelowLogStart.
    if (offset > end) return {ReadError::AboveLogEnd, {}};

    // Caught up. The most common read in the system, and a success: no error, no
    // bytes. Note this is checked before any segment is consulted, so the
    // ordinary case costs one atomic load and nothing else.
    if (offset == end) return {ReadError::None, {}};

    // In the active segment. Checked first because it is where reads cluster —
    // consumers are usually near the head of the log.
    if (offset >= active_->baseOffset())
        return {ReadError::None, active_->read(offset, maxBytes)};

    // Otherwise find the sealed segment holding it. upper_bound gives the first
    // base offset GREATER than the target, so stepping back one gives the
    // greatest base offset at or below it — decision 11's binary search over
    // segment names, in two lines, because the map is ordered.
    auto it = sealed_.upper_bound(offset);

    // Nothing at or below the target: every segment that old has been deleted by
    // retention. This is also the correct answer when nothing is sealed at all —
    // upper_bound on an empty map returns begin(), so the case needs no special
    // handling.
    if (it == sealed_.begin()) return {ReadError::BelowLogStart, {}};

    --it;
    return {ReadError::None, it->second->read(offset, maxBytes)};
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
