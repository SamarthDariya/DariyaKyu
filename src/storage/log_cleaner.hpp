#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <span>
#include <thread>
#include <vector>

#include "common/types.hpp"
#include "storage/key_offset_map.hpp"
#include "storage/log.hpp"
#include "storage/log_manager.hpp"
#include "storage/record_batch.hpp"
#include "storage/segment.hpp"

namespace dariyakyu::storage {

// One batch, as the cleaner needs it: the header, and the bytes.
//
// The bytes rather than a FileRange, which is the one place this codebase copies
// record payloads on purpose. Compaction has to look INSIDE records — at their
// keys — and rebuild batches from the ones it keeps, so there is nothing for
// sendfile to do here. That is why the cleaner is a background thread rather than
// anything on a request path.
struct ScannedBatch {
    BatchHeader               header;
    std::vector<std::uint8_t> bytes;
    std::uint64_t             position = 0;
};

// Walks every complete batch in a segment file, oldest first.
//
// Stops at the first batch that does not fit in what remains rather than
// throwing. A sealed segment should never end mid-batch — sealing is what
// promises it does not — but a torn tail is exactly what a crash during a roll
// can leave, and a cleaner that threw there would be permanently unable to clean
// that partition. Everything before the tear is complete and is worth compacting;
// the tear itself is left alone for recovery to find.
void scanSegment(const std::filesystem::path& logFile,
                 const std::function<void(const ScannedBatch&)>& visit);

// Everything the retain decision needs that is not in the record itself.
struct RetainRules {
    const KeyOffsetMap* map               = nullptr;
    std::int64_t        nowMs             = 0;
    std::int64_t        deleteRetentionMs = 0;
};

// Whether a rewrite keeps this record.
//
// Four cases, and three of them resolve toward keeping. That asymmetry is
// deliberate: a record wrongly dropped is gone, and a record wrongly kept costs
// disk until the next pass.
//
//   no key         KEEP. It cannot be compacted by key at all, so there is no
//                  sense in which a newer record replaces it. Kafka treats an
//                  unkeyed record on a compacted topic as a producer error and
//                  discards it; this keeps it, because the failure mode there is
//                  a misconfigured producer silently losing everything it writes,
//                  which is worse than a compacted topic that does not fully
//                  compact.
//   not the newest KEEP NOT. The whole point.
//   newest, valued KEEP.
//   newest, null   a TOMBSTONE — keep until the horizon, then drop.
//
// The horizon is measured against the record's own timestamp, which comes from
// the producer. That inherits the skew RetentionPolicy already accepts and for
// the same reason: the alternative is deciding by when the broker happened to
// receive it, which makes the window meaningless after any replay or backfill.
// Kafka instead measures from when the segment was last cleaned, which is more
// robust to a bad clock and needs per-segment state this does not keep — banked.
// `offset` and `timestampMs` are absolute, resolved against the batch header —
// a Record holds both as deltas, and a predicate that took the deltas would be
// answering about the wrong record.
bool retain(const Record& record, Offset offset, std::int64_t timestampMs,
            const RetainRules& rules);

// Whether a record says "this key is gone". A NULL value, which is not the same
// as an empty one: an empty value is an ordinary record whose value happens to be
// zero bytes long, and it means the key is still there.
inline bool isTombstone(const Record& record) { return !record.value.has_value(); }

// What one segment's rewrite produced.
struct CleanedSegment {
    std::filesystem::path logFile;      // the .cleaned one, not yet in place
    std::filesystem::path indexFile;
    std::uint64_t         recordsKept    = 0;
    std::uint64_t         recordsDropped = 0;
    bool                  empty() const { return recordsKept == 0; }
};

// Rewrites one segment into `<base>.log.cleaned` / `<base>.index.cleaned`,
// keeping only the records `rules` retains.
//
// Absolute offsets are PRESERVED, never renumbered — a consumer holding offset
// 4,000 must still find offset 4,000, and a FileRange already on a socket must
// still mean what it meant. A batch that loses records keeps the survivors at
// their own offsets, so it gains holes; a batch that loses all of them is not
// written at all.
//
// The caller hands the result to Log::replaceSegment, which is where it becomes
// visible. Nothing here touches the live segment.
CleanedSegment rewriteSegment(const std::filesystem::path& logFile, Offset baseOffset,
                              const RollPolicy& roll, const RetainRules& rules);

// How a pass over one partition went.
struct CleanResult {
    std::size_t   segmentsCleaned = 0;
    std::size_t   segmentsDeleted = 0;   // compacted down to nothing
    std::uint64_t recordsKept     = 0;
    std::uint64_t recordsDropped  = 0;
    std::size_t   keysMapped      = 0;
    bool          mapFilled       = false;

    // Where the next pass should start: the first offset this one did NOT map.
    Offset nextDirtyOffset{0};
};

// The broker's cleaner settings. Not per-partition, and not in partition.meta:
// these are memory and scheduling, which belong to the process rather than to the
// data. delete.retention.ms is the per-topic one, and it lives in LogConfig.
struct CleanerConfig {
    // Entries, not bytes. The map is the memory bound (see KeyOffsetMap), and an
    // entry is a fixed size, so entries is the honest unit.
    std::size_t maxKeyMapEntries = 1'000'000;

    // Below this, a partition is not worth a pass. Without it the cleaner would
    // rewrite whole segments to remove a handful of records, burning I/O
    // proportional to the log for a saving proportional to nothing.
    double minCleanableDirtyRatio = 0.5;

    std::int64_t intervalMs = 15'000;
};

// One compaction pass over one partition.
//
// The key map is built over EVERY dirty segment before any of them is rewritten.
// A key written in segment 1 and again in segment 5 has to lose its copy in
// segment 1, which a map built segment-at-a-time would never notice.
//
// The active segment is never touched. It is being appended to, and every
// invariant in this codebase depends on it having exactly one writer.
// `firstDirtyOffset` is where the last pass stopped. Segments entirely below it
// were cleaned already and are skipped — which is what stops a cleaner rewriting
// the same untouched bytes every fifteen seconds forever.
CleanResult cleanLog(Log& log, const CleanerConfig& cleaner, Offset firstDirtyOffset,
                     std::int64_t nowMs);

// How much of a log's sealed bytes have not been cleaned since they were written.
//
// The number the cleaner schedules on. Without a floor under it, a partition
// whose last pass left it spotless would be rewritten in full to remove the
// handful of records written since — I/O proportional to the log for a saving
// proportional to nothing.
//
// Only sealed segments count, on both sides of the fraction. The active segment
// is neither cleanable nor countable-against: including it would make a busy
// partition look permanently dirty and a quiet one permanently clean, which is
// the opposite of what the ratio is for.
double dirtyRatio(const Log& log, Offset firstDirtyOffset);

// The background cleaner: which partition to compact next, and how far each one
// has been compacted already.
//
// # Why the marks live here rather than on disk
//
// firstDirtyOffset is CLEANER state, not log state — it records what this process
// has done, not what the partition contains. Kafka checkpoints it to a
// cleaner-offset-checkpoint file so a restart does not redo work. This keeps it
// in memory, so a restart re-cleans from the beginning: wasteful, never wrong,
// and it means there is no checkpoint file to be out of step with the segments
// it describes. The checkpoint is banked, not forgotten.
//
// # Why one partition at a time
//
// A pass holds a whole key map, and the map is the memory bound. Cleaning two
// partitions at once would double that for no gain a single thread can collect —
// the work is disk-bound, not CPU-bound.
class LogCleaner {
public:
    explicit LogCleaner(LogManager& logs, CleanerConfig config = {});

    // The compacted partition most worth cleaning, or nothing if none is dirty
    // enough. Dirtiest first, so a partition that is mostly garbage is not
    // starved by one that is mostly fresh.
    std::optional<TopicPartition> pickDirtiest() const;

    // One pass over one partition, if it is worth a pass.
    //
    // Returns nothing when the partition is not compacted, is not dirty enough,
    // or is no longer hosted here. The last is not an error: an administrator can
    // delete a topic between the pick and the pass.
    std::optional<CleanResult> cleanOnce(const TopicPartition& tp, std::int64_t nowMs);

    // Picks and cleans in one step. What the thread calls.
    std::optional<CleanResult> runOnce(std::int64_t nowMs);

    void startCleaning();
    void stopCleaning();

    std::uint64_t passCount() const { return passes_.load(std::memory_order_acquire); }
    std::uint64_t failureCount() const { return failures_.load(std::memory_order_acquire); }
    std::uint64_t recordsDropped() const { return dropped_.load(std::memory_order_acquire); }

    Offset firstDirtyOffset(const TopicPartition& tp) const;

    const CleanerConfig& config() const { return config_; }

    ~LogCleaner();

    LogCleaner(const LogCleaner&)            = delete;
    LogCleaner& operator=(const LogCleaner&) = delete;

private:
    LogManager&   logs_;
    CleanerConfig config_;

    // Guards the marks and the thread's own state. Deliberately never held while
    // a pass runs: a pass rewrites files and can take a long time, and a lock held
    // across it would make stopping the cleaner wait for it.
    mutable std::mutex                        mutex_;
    std::map<TopicPartition, Offset>          firstDirty_;

    std::thread             thread_;
    std::condition_variable wake_;
    bool                    stopRequested_ = false;

    std::atomic<std::uint64_t> passes_{0};
    std::atomic<std::uint64_t> failures_{0};
    std::atomic<std::uint64_t> dropped_{0};
};

}  // namespace dariyakyu::storage
