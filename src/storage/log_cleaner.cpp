#include "storage/log_cleaner.hpp"

#include <chrono>

#include "common/errors.hpp"
#include "common/file_handle.hpp"
#include "storage/offset_index.hpp"
#include "storage/log.hpp"
#include "storage/segment.hpp"

using namespace std;

namespace dariyakyu::storage {

void scanSegment(const filesystem::path&                        logFile,
                 const function<void(const ScannedBatch&)>&     visit) {
    FileHandle     handle(logFile, FileHandle::Mode::ReadOnly);
    const uint64_t limit = handle.size();

    uint64_t position = 0;
    while (position + kBatchHeaderSize <= limit) {
        // The header first, because it says how long the batch is. Reading a
        // fixed-size prefix is the only way to find that out without trusting a
        // length we have not validated.
        vector<uint8_t> headerBytes(kBatchHeaderSize);
        if (handle.readAt(position, headerBytes) < headerBytes.size()) return;

        size_t total = 0;
        try {
            total = RecordBatch::totalSizeOf(headerBytes);
        } catch (const CorruptData&) {
            return;   // a torn tail; everything before it was complete
        }
        if (position + total > limit) return;

        ScannedBatch batch;
        batch.position = position;
        batch.bytes.resize(total);
        if (handle.readAt(position, batch.bytes) < total) return;

        try {
            batch.header = RecordBatch::parseHeader(batch.bytes);
        } catch (const CorruptData&) {
            return;
        }

        // Checked EXPLICITLY, because parseHeader does not — it reads the crc
        // field without testing it, which is right for the read path where the
        // payload is never touched. Here it is the whole point: a cleaner that
        // rewrote a corrupt batch would launder the corruption into a freshly
        // sealed segment that recovery has no reason to re-examine, turning
        // damage a scrub could still find into damage nothing will.
        if (!RecordBatch::verifyCrc(batch.bytes)) return;

        visit(batch);
        position += total;
    }
}

bool retain(const Record& record, Offset offset, int64_t timestampMs,
            const RetainRules& rules) {
    // Unkeyed: nothing can replace it, so nothing decides against it.
    if (!record.key) return true;

    if (rules.map && !rules.map->isLatest(*record.key, offset)) return false;

    if (!isTombstone(record)) return true;

    // The newest record for this key, and it says the key is gone. Collecting it
    // now would mean a consumer that was behind skips the deletion entirely and
    // goes on serving a value that no longer exists — so it stays until every
    // consumer has had the window to see it.
    return rules.nowMs - timestampMs < rules.deleteRetentionMs;
}

CleanedSegment rewriteSegment(const filesystem::path& logFile, Offset baseOffset,
                              const RollPolicy& roll, const RetainRules& rules) {
    CleanedSegment result;
    result.logFile   = filesystem::path(logFile).replace_extension(".log.cleaned");
    result.indexFile = filesystem::path(logFile).replace_extension(".index.cleaned");

    // Removed first, so a pass interrupted last time cannot be appended to. Half
    // a cleaned segment plus half another is not a segment.
    error_code ec;
    filesystem::remove(result.logFile, ec);
    filesystem::remove(result.indexFile, ec);

    FileHandle  out(result.logFile, FileHandle::Mode::ReadWrite);
    OffsetIndex index = OffsetIndex::create(result.indexFile, baseOffset, roll.maxIndexBytes);

    uint64_t sinceLastIndex = 0;

    scanSegment(logFile, [&](const ScannedBatch& batch) {
        const auto records = RecordBatch::decodeRecords(batch.bytes);

        RecordBatchBuilder builder;
        Offset             newBase{0};
        bool               started = false;

        for (const auto& record : records) {
            const Offset  offset      = record.offsetFrom(batch.header.baseOffset);
            const int64_t timestampMs = record.timestampFrom(batch.header.firstTimestamp);

            if (!retain(record, offset, timestampMs, rules)) {
                ++result.recordsDropped;
                continue;
            }

            // The batch is based at the first record it KEEPS, not at the offset
            // it used to start from. Every surviving record then sits at its own
            // absolute offset, and the deltas between them carry the holes.
            if (!started) {
                newBase = offset;
                started = true;
            }

            builder.appendAt(static_cast<int32_t>(offset - newBase), timestampMs,
                             record.key, record.value, record.headers);
            ++result.recordsKept;
        }

        // Every record lost. The batch is not written at all — an empty batch is
        // a header with nothing under it, and the encoder refuses to build one
        // for the same reason.
        if (!started) return;

        auto bytes = builder.build();
        RecordBatch::stampBaseOffset(bytes, newBase);

        // Appended first, because append() returns the position it wrote at —
        // which is precisely what the index has to record, and computing it
        // separately would be a second source of truth for the same number.
        const uint64_t position = out.append(bytes);

        // Position 0 is never indexed, because the index reserves it as its
        // empty marker (see OffsetIndex). It needs no entry anyway: a lookup that
        // finds nothing at or below its target already starts scanning from
        // position zero, which is where the first batch is.
        if (position > 0 && sinceLastIndex >= roll.indexIntervalBytes) {
            index.append(newBase, static_cast<uint32_t>(position));
            sinceLastIndex = 0;
        }
        sinceLastIndex += bytes.size();
    });

    out.sync();

    // Trimmed, not merely flushed: this produces a SEALED segment, and a sealed
    // segment's index is the trimmed one. Leaving it preallocated would make the
    // cleaned file differ from every other sealed index on disk, and
    // SealedSegment::open would be reading zeroes past the real entries.
    index.flushAndTrim();

    return result;
}

double dirtyRatio(const Log& log, Offset firstDirtyOffset) {
    uint64_t total = 0;
    uint64_t dirty = 0;

    for (const auto& info : log.sealedSegments()) {
        total += info.sizeBytes;
        if (info.baseOffset >= firstDirtyOffset) dirty += info.sizeBytes;
    }

    if (total == 0) return 0.0;
    return static_cast<double>(dirty) / static_cast<double>(total);
}

namespace {

// Whether rewriting this segment would actually remove anything.
//
// Read-only, and worth the extra scan: most passes re-examine segments that were
// already cleaned, and rewriting one that loses nothing costs a full write plus a
// swap plus a graveyard entry to produce a byte-identical file.
bool wouldDropAnything(const filesystem::path& logFile, const RetainRules& rules) {
    bool any = false;
    scanSegment(logFile, [&](const ScannedBatch& batch) {
        if (any) return;
        for (const auto& record : RecordBatch::decodeRecords(batch.bytes)) {
            const Offset  offset      = record.offsetFrom(batch.header.baseOffset);
            const int64_t timestampMs = record.timestampFrom(batch.header.firstTimestamp);
            if (!retain(record, offset, timestampMs, rules)) {
                any = true;
                return;
            }
        }
    });
    return any;
}

}  // namespace

CleanResult cleanLog(Log& log, const CleanerConfig& cleaner, Offset firstDirtyOffset,
                     int64_t nowMs) {
    CleanResult result;
    result.nextDirtyOffset = firstDirtyOffset;

    const auto sealed = log.sealedSegments();
    if (sealed.empty()) return result;

    // Phase one: map the DIRTY range — the segments written since the last pass.
    // Segments below the mark were mapped before and their keys are already
    // reflected in what survived, so remapping them would only cost time.
    KeyOffsetMap map(cleaner.maxKeyMapEntries);
    Offset       mappedThrough = firstDirtyOffset;

    for (const auto& info : sealed) {
        if (info.nextOffset <= firstDirtyOffset) continue;

        bool refused = false;
        scanSegment(info.logFile, [&](const ScannedBatch& batch) {
            if (refused) return;
            for (const auto& record : RecordBatch::decodeRecords(batch.bytes)) {
                if (!record.key) continue;
                if (!map.put(*record.key, record.offsetFrom(batch.header.baseOffset))) {
                    refused = true;
                    return;
                }
            }
        });

        // The budget filling is a NORMAL outcome, not a failure. This pass cleans
        // what it managed to map and the next starts where it stopped — a cleaner
        // that gave up here would stop compacting exactly the partitions with the
        // most keys.
        if (refused) {
            result.mapFilled = true;
            break;
        }

        mappedThrough = info.nextOffset;
    }

    result.keysMapped      = map.size();
    result.nextDirtyOffset = mappedThrough;

    if (map.size() == 0) return result;

    // Phase two: rewrite every sealed segment from the LOG START through the end
    // of the mapped range — not just the dirty part.
    //
    // This is the half that is easy to get wrong, and did get wrong. A key written
    // at offset 10 and again at offset 2,000 has its newer copy in the dirty range
    // and its older copy in a segment that was cleaned passes ago. Rewriting only
    // the dirty segments leaves that older copy on disk forever, and the partition
    // settles at one record per key PER PASS rather than one record per key.
    //
    // The cost is re-reading segments that have nothing to lose, which is why each
    // one is asked first.
    const RetainRules rules{&map, nowMs, log.config().compaction.deleteRetentionMs};

    for (const auto& info : sealed) {
        if (info.baseOffset >= mappedThrough) break;

        if (!wouldDropAnything(info.logFile, rules)) continue;

        const CleanedSegment cleaned =
            rewriteSegment(info.logFile, info.baseOffset, log.config().roll, rules);

        result.recordsKept += cleaned.recordsKept;
        result.recordsDropped += cleaned.recordsDropped;

        if (cleaned.empty()) {
            // Every record in the segment lost. Keeping an empty one would leave a
            // file that decodes to nothing and a base offset that no longer
            // describes anything — and a compacted log is allowed the gap.
            error_code ec;
            filesystem::remove(cleaned.logFile, ec);
            filesystem::remove(cleaned.indexFile, ec);
            if (log.removeSegment(info.baseOffset, nowMs)) ++result.segmentsDeleted;
            continue;
        }

        if (log.replaceSegment(info.baseOffset, cleaned.logFile, cleaned.indexFile, nowMs))
            ++result.segmentsCleaned;
    }

    return result;
}

LogCleaner::LogCleaner(LogManager& logs, CleanerConfig config)
    : logs_(logs), config_(config) {}

LogCleaner::~LogCleaner() { stopCleaning(); }

Offset LogCleaner::firstDirtyOffset(const TopicPartition& tp) const {
    lock_guard lock(mutex_);
    const auto found = firstDirty_.find(tp);
    return found == firstDirty_.end() ? Offset(0) : found->second;
}

optional<TopicPartition> LogCleaner::pickDirtiest() const {
    optional<TopicPartition> choice;
    double                   best = config_.minCleanableDirtyRatio;

    for (const auto& tp : logs_.hostedPartitions()) {
        const Log* log = logs_.get(tp);
        if (log == nullptr || !log->config().compacted()) continue;

        const double ratio = dirtyRatio(*log, firstDirtyOffset(tp));

        // Strictly greater, so the floor itself is not a qualifying score — a
        // partition sitting exactly at the threshold is left for when it is
        // properly dirty.
        if (ratio > best) {
            best   = ratio;
            choice = tp;
        }
    }

    return choice;
}

optional<CleanResult> LogCleaner::cleanOnce(const TopicPartition& tp, int64_t nowMs) {
    Log* log = logs_.get(tp);
    if (log == nullptr || !log->config().compacted()) return nullopt;

    const Offset from = firstDirtyOffset(tp);
    if (dirtyRatio(*log, from) <= config_.minCleanableDirtyRatio) return nullopt;

    // Run OUTSIDE the lock. A pass rewrites files and can take a long time, and
    // holding the lock across it would make stopping the cleaner wait for it.
    const CleanResult result = cleanLog(*log, config_, from, nowMs);

    {
        lock_guard lock(mutex_);
        firstDirty_[tp] = result.nextDirtyOffset;
    }

    dropped_.fetch_add(result.recordsDropped, memory_order_release);
    return result;
}

optional<CleanResult> LogCleaner::runOnce(int64_t nowMs) {
    const auto tp = pickDirtiest();
    if (!tp) return nullopt;
    return cleanOnce(*tp, nowMs);
}

void LogCleaner::startCleaning() {
    if (thread_.joinable()) return;

    thread_ = thread([this] {
        while (true) {
            {
                unique_lock lock(mutex_);

                // A condition variable rather than a sleep, for the reason
                // LogManager's sweeper uses one: with a plain sleep, stopping
                // would wait out the whole interval, and a shutdown that slow
                // gets SIGKILLed by whatever is supervising it.
                wake_.wait_for(lock, chrono::milliseconds(config_.intervalMs),
                               [this] { return stopRequested_; });
                if (stopRequested_) return;
            }

            // A pass that throws must not take the broker down — one unreadable
            // segment would stop every partition being compacted. But swallowing
            // it silently would leave compaction quietly dead, so failures are
            // counted where an operator can see them.
            try {
                runOnce(wallClockMillis());
                passes_.fetch_add(1, memory_order_release);
            } catch (const Error&) {
                failures_.fetch_add(1, memory_order_release);
            }
        }
    });
}

void LogCleaner::stopCleaning() {
    {
        lock_guard lock(mutex_);
        stopRequested_ = true;
    }
    wake_.notify_all();
    if (thread_.joinable()) thread_.join();
}

}  // namespace dariyakyu::storage
