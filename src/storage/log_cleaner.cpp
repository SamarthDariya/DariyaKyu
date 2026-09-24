#include "storage/log_cleaner.hpp"

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

CleanResult cleanLog(Log& log, const CleanerConfig& cleaner, int64_t nowMs) {
    CleanResult result;

    const auto sealed = log.sealedSegments();
    if (sealed.empty()) return result;

    result.nextDirtyOffset = sealed.front().baseOffset;

    // Phase one: map every dirty segment, oldest first, before rewriting any of
    // them. A key written early and again late has to lose its early copy, and a
    // map built one segment at a time would never see the later write.
    KeyOffsetMap map(cleaner.maxKeyMapEntries);
    size_t       mappable = 0;

    for (const auto& info : sealed) {
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
        // what it managed to map and the next one starts where this stopped — a
        // cleaner that gave up here would stop compacting exactly the partitions
        // with the most keys.
        if (refused) {
            result.mapFilled = true;
            break;
        }

        ++mappable;
        result.nextDirtyOffset = info.nextOffset;
    }

    result.keysMapped = map.size();

    // Phase two: rewrite the segments that were fully mapped.
    const RetainRules rules{&map, nowMs, log.config().compaction.deleteRetentionMs};

    for (size_t i = 0; i < mappable; ++i) {
        const auto& info = sealed[i];

        const CleanedSegment cleaned =
            rewriteSegment(info.logFile, info.baseOffset, log.config().roll, rules);

        result.recordsKept += cleaned.recordsKept;
        result.recordsDropped += cleaned.recordsDropped;

        if (cleaned.empty()) {
            // Every record in the segment lost. Keeping an empty one would leave
            // a file that decodes to nothing and a base offset that no longer
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

}  // namespace dariyakyu::storage
