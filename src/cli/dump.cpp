#include "cli/dump.hpp"

#include "storage/log_cleaner.hpp"

#include <unistd.h>

#include <cstdio>
#include <optional>
#include <map>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "common/errors.hpp"
#include "storage/log.hpp"
#include "storage/segment.hpp"

using namespace std;
using namespace dariyakyu::storage;

namespace dariyakyu::cli {

void generatePartition(const filesystem::path& dir, int records, uint64_t segmentBytes,
                       bool compact) {
    filesystem::remove_all(dir);

    LogConfig config;
    config.roll.maxSegmentBytes    = segmentBytes;
    config.roll.indexIntervalBytes = 200;
    config.roll.maxIndexBytes      = 400;
    if (compact) config.cleanup = CleanupPolicy::Compact;

    auto log = Log::create(TopicPartition{"orders", 0}, dir, config);
    for (int i = 0; i < records; ++i) {
        RecordBatchBuilder    builder;
        const string          key = "user" + to_string(i % 10);
        const vector<uint8_t> value(40, 0xAB);
        builder.append(1700000000000LL + i * 1000,
                       span<const uint8_t>(reinterpret_cast<const uint8_t*>(key.data()),
                                           key.size()),
                       span<const uint8_t>(value));
        auto bytes = builder.build();
        log->append(bytes);   // assigns and stamps the offset
    }
    if (!compact) {
        printf("generated %d records into %s\n\n", records, dir.c_str());
        return;
    }

    // One key deleted outright, so the dump has a tombstone to show.
    {
        RecordBatchBuilder builder;
        const string       key = "user3";
        builder.append(1700000000000LL + records * 1000,
                       span<const uint8_t>(reinterpret_cast<const uint8_t*>(key.data()),
                                           key.size()),
                       nullopt);
        auto bytes = builder.build();
        log->append(bytes);
    }

    // A cleaner pass with a horizon long enough that the tombstone survives it —
    // the interesting state to look at, because a collected tombstone leaves
    // nothing behind to see.
    const auto result = cleanLog(*log, CleanerConfig{}, Offset(0), 1700000000000LL);

    printf("generated %d records into %s, then compacted:\n", records + 1, dir.c_str());
    printf("  %zu segment(s) rewritten, %zu removed, %llu record(s) dropped, %zu key(s) mapped\n\n",
           result.segmentsCleaned, result.segmentsDeleted,
           (unsigned long long)result.recordsDropped, result.keysMapped);
}

// Segment .log files in base-offset order. The map is ordered and the names are
// zero-padded, so this is the sorted segment table.
namespace {

map<Offset, filesystem::path> segmentsIn(const filesystem::path& dir) {
    map<Offset, filesystem::path> logs;
    for (const auto& entry : filesystem::directory_iterator(dir))
        if (entry.path().extension() == ".log")
            logs.emplace(baseOffsetFromLogPath(entry.path()), entry.path());
    return logs;
}

}  // namespace

void inspectPartition(const filesystem::path& dir) {
    const auto logs = segmentsIn(dir);
    if (logs.empty()) {
        printf("%s holds no segments\n", dir.c_str());
        return;
    }

    printf("PARTITION %s   %zu segment(s)\n\n", dir.filename().c_str(), logs.size());

    uint64_t totalBytes  = 0;
    Offset   logEnd{0};
    int      totalHoles  = 0;
    int      segmentGaps = 0;

    // Set after the first segment, so the first comparison is not against zero.
    optional<Offset> previousEnd;

    for (const auto& [base, path] : logs) {
        auto segment = SealedSegment::open(path);

        // A base offset above where the previous segment ended means a whole
        // segment was compacted away.
        if (previousEnd && base > *previousEnd) {
            printf("  ... %lld offset(s) between segments — a segment was removed\n",
                   (long long)(base - *previousEnd));
            ++segmentGaps;
        }
        const auto indexPath = segmentIndexPath(dir, base);

        // A trimmed index means the segment was sealed; a fully preallocated one
        // means it was still active when the process stopped.
        const auto  indexBytes = filesystem::file_size(indexPath);
        const char* state = (indexBytes == segment->index().entryCount() * OffsetIndex::kEntrySize)
                                ? "sealed"
                                : "ACTIVE";

        printf("%s  [%s]\n", path.filename().c_str(), state);
        printf("  offsets %lld..%lld   %llu bytes   %zu index entries (%ju bytes on disk)\n",
               (long long)segment->baseOffset().value(),
               (long long)segment->nextOffset().value() - 1,
               (unsigned long long)segment->sizeBytes(), segment->index().entryCount(),
               indexBytes);

        uint64_t position   = 0;
        int      batches    = 0;
        int      holes      = 0;
        int      tombstones = 0;

        // Tracked across the WHOLE segment rather than per batch, so a segment
        // whose leading records were compacted away — base offset 42, first
        // surviving record 46 — is reported rather than silently looking normal.
        Offset expected = segment->baseOffset();
        while (auto at = segment->batchAt(position, segment->sizeBytes())) {
            vector<uint8_t> bytes(at->totalSize);
            ::pread(segment->fd(), bytes.data(), bytes.size(), static_cast<off_t>(position));

            printf("    byte %-7llu offsets %lld..%-4lld %2d rec  %4zu B  ts %lld  crc %s\n",
                   (unsigned long long)position, (long long)at->header.baseOffset.value(),
                   (long long)at->header.lastOffset().value(), at->header.recordCount,
                   at->totalSize, (long long)at->header.maxTimestamp,
                   RecordBatch::verifyCrc(bytes) ? "ok" : "BAD");

            // Records within the batch, which is where compaction shows. A batch
            // whose offsets are not consecutive has had records removed, and a
            // record with no value is a tombstone waiting out its horizon.
            for (const auto& record : RecordBatch::decodeRecords(bytes)) {
                const Offset offset = record.offsetFrom(at->header.baseOffset);
                if (offset != expected) {
                    printf("      ... %lld offset(s) compacted away\n",
                           (long long)(offset - expected));
                    ++holes;
                }
                if (!record.value) {
                    printf("      offset %-6lld TOMBSTONE\n", (long long)offset.value());
                    ++tombstones;
                }
                expected = offset + 1;
            }

            position += at->totalSize;
            ++batches;
        }

        // A gap here means the tail of the file did not parse — a torn write, or
        // damage. Worth saying out loud rather than silently stopping.
        if (position < segment->sizeBytes())
            printf("    %llu trailing byte(s) did not parse as a batch\n",
                   (unsigned long long)(segment->sizeBytes() - position));

        printf("    %d batch(es)", batches);
        if (holes > 0) printf(", %d compaction hole(s)", holes);
        if (tombstones > 0) printf(", %d tombstone(s)", tombstones);
        printf("\n\n");

        totalBytes += segment->sizeBytes();
        logEnd      = segment->nextOffset();
        totalHoles += holes;
        previousEnd = segment->nextOffset();
    }

    printf("logStartOffset %lld   logEndOffset %lld   %llu bytes total\n",
           (long long)logs.begin()->first.value(), (long long)logEnd.value(),
           (unsigned long long)totalBytes);

    // Said plainly, because on a Delete topic either of these is damage and on a
    // Compact one both are the cleaner working. The dump cannot tell which kind of
    // topic this is without partition.meta, so it reports and leaves the reading
    // to whoever ran it.
    if (totalHoles > 0 || segmentGaps > 0)
        printf("%d hole(s) within segments, %d gap(s) between them — expected on a "
               "compacted topic, damage on any other\n",
               totalHoles, segmentGaps);
}


}  // namespace dariyakyu::cli
