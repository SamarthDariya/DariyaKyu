#include "cli/dump.hpp"

#include <unistd.h>

#include <cstdio>
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

void generatePartition(const filesystem::path& dir, int records, uint64_t segmentBytes) {
    filesystem::remove_all(dir);

    LogConfig config;
    config.roll.maxSegmentBytes    = segmentBytes;
    config.roll.indexIntervalBytes = 200;
    config.roll.maxIndexBytes      = 400;

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
    printf("generated %d records into %s\n\n", records, dir.c_str());
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

    uint64_t totalBytes = 0;
    Offset   logEnd{0};

    for (const auto& [base, path] : logs) {
        auto segment = SealedSegment::open(path);
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

        uint64_t position = 0;
        int      batches  = 0;
        while (auto at = segment->batchAt(position, segment->sizeBytes())) {
            vector<uint8_t> bytes(at->totalSize);
            ::pread(segment->fd(), bytes.data(), bytes.size(), static_cast<off_t>(position));

            printf("    byte %-7llu offsets %lld..%-4lld %2d rec  %4zu B  ts %lld  crc %s\n",
                   (unsigned long long)position, (long long)at->header.baseOffset.value(),
                   (long long)at->header.lastOffset().value(), at->header.recordCount,
                   at->totalSize, (long long)at->header.maxTimestamp,
                   RecordBatch::verifyCrc(bytes) ? "ok" : "BAD");

            position += at->totalSize;
            ++batches;
        }

        // A gap here means the tail of the file did not parse — a torn write, or
        // damage. Worth saying out loud rather than silently stopping.
        if (position < segment->sizeBytes())
            printf("    %llu trailing byte(s) did not parse as a batch\n",
                   (unsigned long long)(segment->sizeBytes() - position));

        printf("    %d batch(es)\n\n", batches);
        totalBytes += segment->sizeBytes();
        logEnd = segment->nextOffset();
    }

    printf("logStartOffset %lld   logEndOffset %lld   %llu bytes total\n",
           (long long)logs.begin()->first.value(), (long long)logEnd.value(),
           (unsigned long long)totalBytes);
}


}  // namespace dariyakyu::cli
