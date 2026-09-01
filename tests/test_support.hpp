#pragma once

// Shared fixtures for the storage test suites.
//
// Every helper here is either an isolation device (TempDir) or a way to build a
// state that is otherwise awkward to reach — a partition with several sealed
// segments, a batch the producer has not stamped yet, a log file with damage in
// it. They live in one place because four test files need them and three copies
// of TempDir was already two too many.
//
// Include AFTER doctest: some of these assert.

#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/errors.hpp"
#include "storage/log.hpp"
#include "storage/log_manager.hpp"
#include "storage/offset_index.hpp"
#include "storage/partition_meta.hpp"
#include "storage/record_batch.hpp"
#include "storage/segment.hpp"

namespace dariyakyu::test {

using namespace std;
using namespace dariyakyu;
using namespace dariyakyu::storage;

// Same isolation the common suite uses: filesystem tests leak into each other
// more readily than any other kind.
class TempDir {
public:
    explicit TempDir(const string& name)
        : path_(filesystem::temp_directory_path() / ("dariyakyu-test-" + name)) {
        filesystem::remove_all(path_);
        filesystem::create_directories(path_);
    }
    ~TempDir() { filesystem::remove_all(path_); }

    TempDir(const TempDir&)            = delete;
    TempDir& operator=(const TempDir&) = delete;

    filesystem::path file(const string& name) const { return path_ / name; }

private:
    filesystem::path path_;
};

constexpr size_t kIndexBytes = 1024;

// Larger than anything these tests write, so reads are not capped unless a test
// is specifically about capping.
constexpr size_t kBigFetch = 1 << 20;   // 128 entries, plenty for most cases here

vector<uint8_t> readFile(const filesystem::path& path) {
    ifstream in(path, ios::binary);
    return vector<uint8_t>(istreambuf_iterator<char>(in), istreambuf_iterator<char>());
}

void writeFile(const filesystem::path& path, const vector<uint8_t>& bytes) {
    ofstream out(path, ios::binary | ios::trunc);
    out.write(reinterpret_cast<const char*>(bytes.data()),
              static_cast<streamsize>(bytes.size()));
}

// Hand-built index bytes, so the recovery cases can describe a file that no
// correct run of the code would produce.
void appendEntryBytes(vector<uint8_t>& out, uint32_t relativeOffset, uint32_t position) {
    for (int shift = 24; shift >= 0; shift -= 8)
        out.push_back(static_cast<uint8_t>(relativeOffset >> shift));
    for (int shift = 24; shift >= 0; shift -= 8)
        out.push_back(static_cast<uint8_t>(position >> shift));
}

// What lookup() must agree with, written the slow obvious way.
OffsetIndex::Entry referenceLookup(const vector<OffsetIndex::Entry>& entries, uint32_t target) {
    OffsetIndex::Entry best{};
    for (const auto& entry : entries)
        if (entry.relativeOffset <= target) best = entry;
    return best;
}

RollPolicy testPolicy() {
    // Deliberately small, so tests reach an index interval and a size limit
    // without writing megabytes.
    RollPolicy policy;
    policy.maxSegmentBytes    = 1 << 16;
    policy.indexIntervalBytes = 256;
    policy.maxIndexBytes      = 1024;
    return policy;
}

// The same small policy wrapped in a full config, for Log-level tests.
LogConfig configWith(const RollPolicy& roll) {
    LogConfig config;
    config.roll = roll;
    return config;
}

LogConfig testConfig() {
    return configWith(testPolicy());
}

// One batch of `records` records, each carrying `payloadBytes` of value, stamped
// so the header says it begins at `baseOffset`.
vector<uint8_t> makeBatch(Offset baseOffset, int64_t timestamp, size_t payloadBytes = 32,
                          int records = 1) {
    RecordBatchBuilder    builder;
    const vector<uint8_t> value(payloadBytes, 0xAB);
    for (int i = 0; i < records; ++i)
        builder.append(timestamp + i, nullopt, span<const uint8_t>(value));

    auto bytes = builder.build();
    // What Log will do on the write path: stamp the assigned offset into the
    // header, in place, without recomputing the checksum.
    RecordBatch::stampBaseOffset(bytes, baseOffset);
    return bytes;
}

// Appends `count` single-record batches, returning total bytes written.
uint64_t fillSegment(ActiveSegment& segment, int count, size_t payloadBytes = 32,
                     int64_t startTimestamp = 1000) {
    uint64_t written = 0;
    for (int i = 0; i < count; ++i) {
        const Offset base  = segment.nextOffset();
        auto         bytes = makeBatch(base, startTimestamp + i, payloadBytes);
        segment.append(base, bytes);
        written += bytes.size();
    }
    return written;
}

int64_t testNowMs() {
    return chrono::duration_cast<chrono::milliseconds>(
               chrono::system_clock::now().time_since_epoch())
        .count();
}

// Appends raw bytes straight onto the log file, bypassing ActiveSegment. This is
// how the damage cases are built: append() would refuse most of them, which is
// the point — a crash is not something the writer agreed to.
void appendRawToFile(const filesystem::path& path, const vector<uint8_t>& bytes) {
    ofstream out(path, ios::binary | ios::app);
    out.write(reinterpret_cast<const char*>(bytes.data()),
              static_cast<streamsize>(bytes.size()));
}

// Every distinct position the index names, in order, by sweeping lookups across
// the segment. OffsetIndex has no iterator, and this is what its callers
// actually observe anyway.
vector<uint32_t> indexPositions(const SegmentBase& segment) {
    vector<uint32_t> positions;
    for (int64_t offset = 0; offset < segment.nextOffset() - segment.baseOffset(); ++offset) {
        const uint32_t position =
            segment.index().lookup(segment.baseOffset() + offset).position;
        if (position != 0 && (positions.empty() || positions.back() != position))
            positions.push_back(position);
    }
    return positions;
}

// A batch the producer would send: encoded, but never stamped. Log assigns and
// stamps the offset, so a fixture that pre-stamped it would be testing the wrong
// thing.
vector<uint8_t> makeUnstampedBatch(int64_t timestamp, size_t payloadBytes = 32,
                                   int records = 1) {
    RecordBatchBuilder    builder;
    const vector<uint8_t> value(payloadBytes, 0xAB);
    for (int i = 0; i < records; ++i)
        builder.append(timestamp + i, nullopt, span<const uint8_t>(value));
    return builder.build();
}

// Walks every .log file in a partition directory in base-offset order and
// collects the offsets it actually finds on disk. Used to prove rolling loses
// nothing — deliberately independent of Log's own bookkeeping.
vector<int64_t> offsetsOnDisk(const filesystem::path& partition) {
    map<string, filesystem::path> logs;
    for (const auto& entry : filesystem::directory_iterator(partition))
        if (entry.path().extension() == ".log")
            logs[entry.path().filename().string()] = entry.path();

    vector<int64_t> offsets;
    for (const auto& [name, path] : logs) {
        const auto bytes = readFile(path);
        size_t     position = 0;
        while (position < bytes.size()) {
            const auto batch  = span<const uint8_t>(bytes).subspan(position);
            const auto header = RecordBatch::parseHeader(batch);
            for (int64_t i = 0; i < header.recordCount; ++i)
                offsets.push_back(header.baseOffset.value() + i);
            position += RecordBatch::totalSizeOf(batch);
        }
    }
    return offsets;
}

// Pulls the bytes a FileRange describes off the descriptor, the way the network
// layer's sendfile will. Proves the range is actually usable, not just plausible.
vector<uint8_t> pullRange(const FileRange& range) {
    vector<uint8_t> bytes(range.length);
    const ssize_t   read = ::pread(range.fd, bytes.data(), bytes.size(),
                                   static_cast<off_t>(range.position));
    REQUIRE(read == static_cast<ssize_t>(bytes.size()));
    return bytes;
}

// Writes a multi-segment partition and lets it go out of scope — the state a
// process leaves behind when it dies without a clean shutdown.
vector<Offset> writeThenAbandon(const filesystem::path& partition, const RollPolicy& policy,
                                int batches) {
    auto log = Log::create(TopicPartition{"orders", 0}, partition, configWith(policy));
    for (int i = 0; i < batches; ++i) {
        auto bytes = makeUnstampedBatch(1000 + i, 48);
        log->append(bytes);
    }

    vector<Offset> bases;
    map<string, filesystem::path> logs;
    for (const auto& entry : filesystem::directory_iterator(partition))
        if (entry.path().extension() == ".log")
            logs[entry.path().filename().string()] = entry.path();
    for (const auto& [name, path] : logs) bases.push_back(baseOffsetFromLogPath(path));
    return bases;
}

// Adds up the .log files in a partition directory. Deliberately independent of
// Log's own view, so the two can be compared.
uint64_t logBytesOnDisk(const filesystem::path& partition) {
    uint64_t total = 0;
    for (const auto& entry : filesystem::directory_iterator(partition))
        if (entry.path().extension() == ".log") total += filesystem::file_size(entry.path());
    return total;
}

// A log with several sealed segments, each carrying timestamps `stepMs` apart, so
// tests can age individual segments out.
unique_ptr<Log> logWithAgedSegments(const filesystem::path& partition, LogConfig config,
                                    int batches, int64_t firstTimestamp, int64_t stepMs) {
    config.roll.maxSegmentBytes = 400;
    auto log = Log::create(TopicPartition{"orders", 0}, partition, config);
    for (int i = 0; i < batches; ++i) {
        auto bytes = makeUnstampedBatch(firstTimestamp + i * stepMs, 48);
        log->append(bytes);
    }
    return log;
}

PartitionMeta sampleMeta() {
    PartitionMeta meta;
    meta.tp = TopicPartition{"orders", 3};
    meta.config.roll.maxSegmentBytes       = 1234;
    meta.config.roll.maxSegmentAgeMs       = 5678;
    meta.config.roll.indexIntervalBytes    = 91011;
    meta.config.roll.maxIndexBytes         = 121314;
    meta.config.retention.retentionMs      = 151617;
    meta.config.retention.retentionBytes   = 181920;
    meta.config.retention.segmentDeleteDelayMs = 212223;
    return meta;
}

uint32_t be32At(const vector<uint8_t>& bytes, size_t at) {
    return (uint32_t(bytes[at]) << 24) | (uint32_t(bytes[at + 1]) << 16) |
           (uint32_t(bytes[at + 2]) << 8) | uint32_t(bytes[at + 3]);
}

void checkMetaEqual(const PartitionMeta& got, const PartitionMeta& want) {
    CHECK(got.tp.topic == want.tp.topic);
    CHECK(got.tp.partition == want.tp.partition);
    CHECK(got.config.roll.maxSegmentBytes == want.config.roll.maxSegmentBytes);
    CHECK(got.config.roll.maxSegmentAgeMs == want.config.roll.maxSegmentAgeMs);
    CHECK(got.config.roll.indexIntervalBytes == want.config.roll.indexIntervalBytes);
    CHECK(got.config.roll.maxIndexBytes == want.config.roll.maxIndexBytes);
    CHECK(got.config.retention.retentionMs == want.config.retention.retentionMs);
    CHECK(got.config.retention.retentionBytes == want.config.retention.retentionBytes);
    CHECK(got.config.retention.segmentDeleteDelayMs ==
          want.config.retention.segmentDeleteDelayMs);
}

size_t partitionDirCount(const filesystem::path& dataDir) {
    size_t count = 0;
    for (const auto& entry : filesystem::directory_iterator(dataDir))
        if (entry.is_directory() && !entry.path().filename().string().ends_with(kDeletedSuffix))
            ++count;
    return count;
}

size_t deletedDirCount(const filesystem::path& dataDir) {
    size_t count = 0;
    for (const auto& entry : filesystem::directory_iterator(dataDir))
        if (entry.is_directory() && entry.path().filename().string().ends_with(kDeletedSuffix))
            ++count;
    return count;
}

}  // namespace dariyakyu::test
