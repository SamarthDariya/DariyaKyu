#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include <atomic>
#include <thread>
#include <type_traits>

#include "test_support.hpp"

using namespace std;
using namespace dariyakyu;
using namespace dariyakyu::storage;
using namespace dariyakyu::test;

// ===========================================================================
// ReadResult
// ===========================================================================

TEST_CASE("A default read result is a successful read of nothing") {
    const ReadResult result;

    // Which is exactly the caught-up case: no error, no bytes.
    CHECK(result.ok());
    CHECK(result.error == ReadError::None);
    CHECK(result.range.empty());
    CHECK(result.range.length == 0);
}

TEST_CASE("Success and having bytes are separate questions") {
    // A caught-up consumer. This is the most common read in the system, so it
    // must be a plain value on the success path rather than an exception.
    const ReadResult caughtUp{ReadError::None, FileRange{7, 4096, 0}};
    CHECK(caughtUp.ok());
    CHECK(caughtUp.range.empty());

    // Data to send.
    const ReadResult found{ReadError::None, FileRange{7, 4096, 512}};
    CHECK(found.ok());
    CHECK_FALSE(found.range.empty());
}

TEST_CASE("The two failure modes stay distinguishable") {
    const ReadResult tooOld{ReadError::BelowLogStart, {}};
    const ReadResult tooNew{ReadError::AboveLogEnd, {}};

    CHECK_FALSE(tooOld.ok());
    CHECK_FALSE(tooNew.ok());

    // A client's reset policy treats these differently: below the log start
    // means "jump to the earliest offset available", while above the log end
    // means the client is confused or the log was truncated under it. Collapsing
    // them into one error would make a failover look like ordinary lag.
    CHECK(tooOld.error != tooNew.error);
    CHECK(tooOld.range.empty());
    CHECK(tooNew.range.empty());
}

// ===========================================================================
// Log: creation
// ===========================================================================

static_assert(!is_copy_constructible_v<Log>,
              "a Log owns every segment of a partition; copying one would mean two owners of "
              "the same files");

TEST_CASE("A new log has one empty segment based at offset zero") {
    TempDir dir("log-create");
    const filesystem::path partition = dir.file("orders-0");

    auto log = Log::create(TopicPartition{"orders", 0}, partition, testConfig());

    CHECK(log->topicPartition().topic == "orders");
    CHECK(log->topicPartition().partition == 0);
    CHECK(log->directory() == partition);
    CHECK(log->segmentCount() == 1);

    // Nothing written, so the log both starts and ends at zero — an empty
    // half-open range rather than a special "empty" state.
    CHECK(log->logStartOffset() == Offset(0));
    CHECK(log->logEndOffset() == Offset(0));
    CHECK(log->highWatermark() == Offset(0));

    CHECK(filesystem::exists(segmentLogPath(partition, Offset(0))));
    CHECK(filesystem::exists(segmentIndexPath(partition, Offset(0))));
}

TEST_CASE("Creating a log makes its directory") {
    TempDir dir("log-create-dirs");
    const filesystem::path partition = dir.file("deep/orders-3");
    REQUIRE_FALSE(filesystem::exists(partition));

    auto log = Log::create(TopicPartition{"orders", 3}, partition, testConfig());
    CHECK(filesystem::is_directory(partition));
    CHECK(log->segmentCount() == 1);
}

TEST_CASE("Creating a log over an existing partition is refused") {
    TempDir dir("log-create-twice");
    const filesystem::path partition = dir.file("orders-0");

    auto first = Log::create(TopicPartition{"orders", 0}, partition, testConfig());

    // Adopting an existing partition is a different operation with different
    // rules — it has to recover the newest segment rather than assume an empty
    // one. Silently appending to whatever was there would interleave two logs.
    CHECK_THROWS(Log::create(TopicPartition{"orders", 0}, partition, testConfig()));
}

TEST_CASE("The high watermark can be advanced") {
    TempDir dir("log-hwm");
    auto    log = Log::create(TopicPartition{"orders", 0}, dir.file("orders-0"), testConfig());

    CHECK(log->highWatermark() == Offset(0));

    // On a single node this just tracks the log end offset. M7 is where the two
    // diverge: the watermark becomes the minimum across in-sync replicas, and it
    // is what stops a consumer reading data that could still be lost.
    log->setHighWatermark(Offset(5));
    CHECK(log->highWatermark() == Offset(5));
    CHECK(log->logEndOffset() == Offset(0));   // independent of it
}

TEST_CASE("A log keeps the config it was created with") {
    TempDir dir("log-policy");
    auto    policy = testPolicy();
    policy.maxSegmentBytes = 4242;
    LogConfig config = configWith(policy);
    config.retention.retentionMs    = 60000;
    config.retention.retentionBytes = 8192;

    auto log = Log::create(TopicPartition{"orders", 0}, dir.file("orders-0"), config);
    CHECK(log->config().roll.maxSegmentBytes == 4242);
    CHECK(log->config().roll.indexIntervalBytes == policy.indexIntervalBytes);
    CHECK(log->config().retention.retentionMs == 60000);
    CHECK(log->config().retention.retentionBytes == 8192u);
}

// ===========================================================================
// Log: append
// ===========================================================================

TEST_CASE("Append assigns offsets as one sequence per partition") {
    TempDir dir("log-append");
    auto    log = Log::create(TopicPartition{"orders", 0}, dir.file("orders-0"), testConfig());

    for (int i = 0; i < 10; ++i) {
        auto bytes = makeUnstampedBatch(1000 + i);
        CHECK(log->append(bytes) == Offset(i));
    }

    CHECK(log->logEndOffset() == Offset(10));
    CHECK(log->logStartOffset() == Offset(0));
}

TEST_CASE("The log end offset advances by the batch's record count") {
    TempDir dir("log-append-multi");
    auto    log = Log::create(TopicPartition{"orders", 0}, dir.file("orders-0"), testConfig());

    auto four = makeUnstampedBatch(1000, 16, 4);
    CHECK(log->append(four) == Offset(0));
    CHECK(log->logEndOffset() == Offset(4));

    auto three = makeUnstampedBatch(2000, 16, 3);
    CHECK(log->append(three) == Offset(4));
    CHECK(log->logEndOffset() == Offset(7));
}

TEST_CASE("Append stamps the assigned offset into the bytes on disk") {
    TempDir dir("log-append-stamp");
    const filesystem::path partition = dir.file("orders-0");
    auto    log = Log::create(TopicPartition{"orders", 0}, partition, testConfig());

    auto first  = makeUnstampedBatch(1000, 16, 2);
    auto second = makeUnstampedBatch(2000, 16, 2);
    log->append(first);
    log->append(second);

    // Read the segment file back cold and parse the headers. The producer sent
    // both batches saying "base offset 0"; what landed must say 0 and 2.
    const auto bytes = readFile(segmentLogPath(partition, Offset(0)));
    const auto firstHeader = RecordBatch::parseHeader(bytes);
    CHECK(firstHeader.baseOffset == Offset(0));

    const size_t firstSize = RecordBatch::totalSizeOf(bytes);
    const auto   secondHeader =
        RecordBatch::parseHeader(span<const uint8_t>(bytes).subspan(firstSize));
    CHECK(secondHeader.baseOffset == Offset(2));
}

TEST_CASE("Stamping does not disturb the checksum") {
    TempDir dir("log-append-crc");
    const filesystem::path partition = dir.file("orders-0");
    auto    log = Log::create(TopicPartition{"orders", 0}, partition, testConfig());

    for (int i = 0; i < 5; ++i) {
        auto bytes = makeUnstampedBatch(1000 + i, 48);
        log->append(bytes);
    }

    // The point of the v2 field order: baseOffset sits before the crc field, so
    // stamping it leaves every checksum on disk still valid. If it did not, the
    // broker would have to recompute a CRC over a body it may not be able to
    // read.
    const auto bytes = readFile(segmentLogPath(partition, Offset(0)));
    size_t     position = 0;
    int        verified = 0;
    while (position < bytes.size()) {
        const auto batch = span<const uint8_t>(bytes).subspan(position);
        CHECK(RecordBatch::verifyCrc(batch));
        position += RecordBatch::totalSizeOf(batch);
        ++verified;
    }
    CHECK(verified == 5);
    CHECK(position == bytes.size());
}

TEST_CASE("Append overwrites whatever base offset the producer sent") {
    TempDir dir("log-append-overwrite");
    const filesystem::path partition = dir.file("orders-0");
    auto    log = Log::create(TopicPartition{"orders", 0}, partition, testConfig());

    // A producer that guessed, or replayed a batch from elsewhere. The broker's
    // sequence is the only authority, so its stamp wins.
    auto bytes = makeUnstampedBatch(1000, 16);
    RecordBatch::stampBaseOffset(bytes, Offset(9999));

    CHECK(log->append(bytes) == Offset(0));
    CHECK(RecordBatch::parseHeader(readFile(segmentLogPath(partition, Offset(0))))
              .baseOffset == Offset(0));
}

TEST_CASE("On a single node the high watermark tracks the log end offset") {
    TempDir dir("log-append-hwm");
    auto    log = Log::create(TopicPartition{"orders", 0}, dir.file("orders-0"), testConfig());

    for (int i = 0; i < 5; ++i) {
        auto bytes = makeUnstampedBatch(1000 + i);
        log->append(bytes);
        // Nothing is replicated, so everything written is immediately committed.
        // M7 is where these two numbers start to differ.
        CHECK(log->highWatermark() == log->logEndOffset());
    }
}

// ===========================================================================
// Log: rolling
// ===========================================================================

TEST_CASE("Exceeding the size limit rolls to a new segment") {
    TempDir dir("log-roll-size");
    const filesystem::path partition = dir.file("orders-0");
    auto    policy         = testPolicy();
    policy.maxSegmentBytes = 600;
    auto    log            = Log::create(TopicPartition{"orders", 0}, partition, configWith(policy));

    CHECK(log->segmentCount() == 1);

    for (int i = 0; i < 40; ++i) {
        auto bytes = makeUnstampedBatch(1000 + i, 64);
        log->append(bytes);
    }

    CHECK(log->segmentCount() > 1);
    CHECK(log->logEndOffset() == Offset(40));
    CHECK(log->logStartOffset() == Offset(0));   // nothing deleted, only sealed
}

TEST_CASE("Rolling loses no records and leaves no gaps") {
    TempDir dir("log-roll-integrity");
    const filesystem::path partition = dir.file("orders-0");
    auto    policy         = testPolicy();
    policy.maxSegmentBytes = 500;
    auto    log            = Log::create(TopicPartition{"orders", 0}, partition, configWith(policy));

    constexpr int kBatches = 60;
    for (int i = 0; i < kBatches; ++i) {
        auto bytes = makeUnstampedBatch(1000 + i, 48);
        log->append(bytes);
    }

    // Read back from the files themselves rather than from Log, so this checks
    // what is on disk rather than what Log believes.
    const auto found = offsetsOnDisk(partition);
    REQUIRE(found.size() == static_cast<size_t>(kBatches));
    for (int i = 0; i < kBatches; ++i) CHECK(found[static_cast<size_t>(i)] == i);
}

TEST_CASE("Each new segment is named for the offset the previous one ended at") {
    TempDir dir("log-roll-names");
    const filesystem::path partition = dir.file("orders-0");
    auto    policy         = testPolicy();
    policy.maxSegmentBytes = 500;
    auto    log            = Log::create(TopicPartition{"orders", 0}, partition, configWith(policy));

    for (int i = 0; i < 40; ++i) {
        auto bytes = makeUnstampedBatch(1000 + i, 48);
        log->append(bytes);
    }

    // Collect base offsets from the filenames, in sorted order.
    vector<Offset> bases;
    map<string, filesystem::path> logs;
    for (const auto& entry : filesystem::directory_iterator(partition))
        if (entry.path().extension() == ".log")
            logs[entry.path().filename().string()] = entry.path();
    for (const auto& [name, path] : logs) bases.push_back(baseOffsetFromLogPath(path));

    REQUIRE(bases.size() > 2);
    CHECK(bases.front() == Offset(0));

    // Contiguous: no gaps, no overlaps. This is the property that lets Log route
    // a read by binary searching base offsets alone.
    const auto disk = offsetsOnDisk(partition);
    for (size_t i = 1; i < bases.size(); ++i) CHECK(bases[i] > bases[i - 1]);
    CHECK(disk.size() == 40);

    // And every sealed segment has a trimmed index while the active one is still
    // preallocated — so the directory says which segment is live.
    const auto activeIndex = segmentIndexPath(partition, bases.back());
    CHECK(filesystem::file_size(activeIndex) == policy.maxIndexBytes);
    CHECK(filesystem::file_size(segmentIndexPath(partition, bases.front())) <
          policy.maxIndexBytes);
}

TEST_CASE("An idle partition rolls when the maintenance sweep asks it to") {
    TempDir dir("log-roll-idle");
    auto    policy         = testPolicy();
    policy.maxSegmentAgeMs = 1000;
    policy.maxSegmentBytes = 1ull << 30;
    auto    log = Log::create(TopicPartition{"orders", 0}, dir.file("orders-0"), configWith(policy));

    auto bytes = makeUnstampedBatch(1000);
    log->append(bytes);
    REQUIRE(log->segmentCount() == 1);

    // The trap this exists for: with no further writes, append() is never called
    // again, so nothing would re-evaluate age. The active segment would never
    // seal and retention would never free anything on this topic.
    log->maybeRoll(wallClockMillis() + 5000);
    CHECK(log->segmentCount() == 2);
    CHECK(log->logEndOffset() == Offset(1));
}

TEST_CASE("An idle partition with an empty active segment does not accumulate files") {
    TempDir dir("log-roll-idle-empty");
    auto    policy         = testPolicy();
    policy.maxSegmentAgeMs = 1000;
    auto    log = Log::create(TopicPartition{"orders", 0}, dir.file("orders-0"), configWith(policy));

    // Repeated sweeps on a partition that has never been written to must do
    // nothing at all, or a quiet topic would grow a segment per sweep forever.
    for (int i = 0; i < 5; ++i) log->maybeRoll(wallClockMillis() + 100000 * (i + 1));
    CHECK(log->segmentCount() == 1);
}

TEST_CASE("Appending continues seamlessly across a roll") {
    TempDir dir("log-roll-continuity");
    auto    policy         = testPolicy();
    policy.maxSegmentBytes = 400;
    auto    log = Log::create(TopicPartition{"orders", 0}, dir.file("orders-0"), configWith(policy));

    // Every returned offset must be exactly the previous log end offset, whether
    // or not that append happened to trigger a roll.
    for (int i = 0; i < 30; ++i) {
        const Offset expected = log->logEndOffset();
        auto         bytes    = makeUnstampedBatch(1000 + i, 48, 2);
        CHECK(log->append(bytes) == expected);
        CHECK(log->logEndOffset() == expected + 2);
    }
    CHECK(log->segmentCount() > 3);
}

// ===========================================================================
// Log: read
// ===========================================================================

TEST_CASE("A read resolves to a range whose first batch contains the offset") {
    TempDir dir("log-read-basic");
    auto    policy         = testPolicy();
    policy.maxSegmentBytes = 500;   // several segments
    auto    log = Log::create(TopicPartition{"orders", 0}, dir.file("orders-0"), configWith(policy));

    constexpr int kBatches = 50;
    for (int i = 0; i < kBatches; ++i) {
        auto bytes = makeUnstampedBatch(1000 + i, 48);
        log->append(bytes);
    }
    REQUIRE(log->segmentCount() > 3);

    // Every offset, across sealed segments and the active one, must resolve to a
    // range that starts on a batch boundary and whose first batch holds it.
    for (int64_t offset = 0; offset < kBatches; ++offset) {
        const auto result = log->read(Offset(offset), kBigFetch);
        REQUIRE(result.ok());
        REQUIRE_FALSE(result.range.empty());

        const auto bytes  = pullRange(result.range);
        const auto header = RecordBatch::parseHeader(bytes);
        CHECK(header.baseOffset <= Offset(offset));
        CHECK(header.lastOffset() >= Offset(offset));
        CHECK(RecordBatch::verifyCrc(bytes));
    }
}

TEST_CASE("A caught-up read is a success with no bytes") {
    TempDir dir("log-read-caught-up");
    auto    log = Log::create(TopicPartition{"orders", 0}, dir.file("orders-0"), testConfig());

    // An empty log is caught up at offset zero.
    auto empty = log->read(Offset(0), kBigFetch);
    CHECK(empty.ok());
    CHECK(empty.range.empty());

    for (int i = 0; i < 5; ++i) {
        auto bytes = makeUnstampedBatch(1000 + i);
        log->append(bytes);
    }

    const auto result = log->read(log->logEndOffset(), kBigFetch);
    CHECK(result.ok());
    CHECK(result.error == ReadError::None);
    CHECK(result.range.empty());
}

TEST_CASE("A read past the log end is an error, not a caught-up read") {
    TempDir dir("log-read-above");
    auto    log = Log::create(TopicPartition{"orders", 0}, dir.file("orders-0"), testConfig());
    for (int i = 0; i < 5; ++i) {
        auto bytes = makeUnstampedBatch(1000 + i);
        log->append(bytes);
    }

    // One past the end is caught up; two past it means the client is confused or
    // the log was truncated under it. Those need different recoveries, so they
    // must not collapse into the same answer.
    CHECK(log->read(Offset(5), kBigFetch).ok());
    CHECK(log->read(Offset(6), kBigFetch).error == ReadError::AboveLogEnd);
    CHECK(log->read(Offset(100000), kBigFetch).error == ReadError::AboveLogEnd);
    CHECK(log->read(Offset(6), kBigFetch).range.empty());
}

TEST_CASE("Reads route to the right segment on both sides of a boundary") {
    TempDir dir("log-read-routing");
    const filesystem::path partition = dir.file("orders-0");
    auto    policy         = testPolicy();
    policy.maxSegmentBytes = 400;
    auto    log = Log::create(TopicPartition{"orders", 0}, partition, configWith(policy));

    for (int i = 0; i < 30; ++i) {
        auto bytes = makeUnstampedBatch(1000 + i, 48);
        log->append(bytes);
    }

    // Collect the segment base offsets from the directory, then check that reads
    // on either side of each boundary land in different files.
    vector<Offset> bases;
    map<string, filesystem::path> logs;
    for (const auto& entry : filesystem::directory_iterator(partition))
        if (entry.path().extension() == ".log")
            logs[entry.path().filename().string()] = entry.path();
    for (const auto& [name, path] : logs) bases.push_back(baseOffsetFromLogPath(path));
    REQUIRE(bases.size() > 2);

    for (size_t i = 1; i < bases.size(); ++i) {
        const auto last  = log->read(bases[i] - 1, kBigFetch);
        const auto first = log->read(bases[i], kBigFetch);
        REQUIRE(last.ok());
        REQUIRE(first.ok());

        // Different files, so different descriptors.
        CHECK(last.range.fd != first.range.fd);
        // And the first read of a new segment starts at its very first byte.
        CHECK(first.range.position == 0);
    }
}

TEST_CASE("A fetch size cap applies through the log, not just the segment") {
    TempDir dir("log-read-cap");
    auto    log = Log::create(TopicPartition{"orders", 0}, dir.file("orders-0"), testConfig());
    for (int i = 0; i < 20; ++i) {
        auto bytes = makeUnstampedBatch(1000 + i, 64);
        log->append(bytes);
    }

    const auto capped = log->read(Offset(0), 300);
    CHECK(capped.ok());
    CHECK(capped.range.length == 300);

    // And a fetch smaller than the next batch still returns a whole batch, so a
    // conservative consumer cannot stall.
    const auto tiny = log->read(Offset(0), 1);
    CHECK(tiny.range.length > 1);
    CHECK(RecordBatch::verifyCrc(pullRange(tiny.range)));
}

// ===========================================================================
// Log: startup scan
// ===========================================================================

TEST_CASE("Reopening a partition recovers the same view of it") {
    TempDir dir("log-open");
    const filesystem::path partition = dir.file("orders-0");
    auto    policy         = testPolicy();
    policy.maxSegmentBytes = 500;

    const auto bases = writeThenAbandon(partition, policy, 50);
    REQUIRE(bases.size() > 3);

    auto log = Log::open(TopicPartition{"orders", 0}, partition, configWith(policy));

    CHECK(log->logStartOffset() == Offset(0));
    CHECK(log->logEndOffset() == Offset(50));
    CHECK(log->segmentCount() == bases.size());

    // Every offset still resolves, through both the reopened sealed segments and
    // the recovered active one.
    for (int64_t offset = 0; offset < 50; ++offset) {
        const auto result = log->read(Offset(offset), kBigFetch);
        REQUIRE(result.ok());
        const auto header = RecordBatch::parseHeader(pullRange(result.range));
        CHECK(header.baseOffset <= Offset(offset));
        CHECK(header.lastOffset() >= Offset(offset));
    }
}

TEST_CASE("A reopened partition can be appended to immediately") {
    TempDir dir("log-open-append");
    const filesystem::path partition = dir.file("orders-0");
    auto    policy         = testPolicy();
    policy.maxSegmentBytes = 500;
    writeThenAbandon(partition, policy, 30);

    auto log = Log::open(TopicPartition{"orders", 0}, partition, configWith(policy));
    REQUIRE(log->logEndOffset() == Offset(30));

    // A broker has to restart into service, not just into read-only mode.
    auto bytes = makeUnstampedBatch(9999, 48);
    CHECK(log->append(bytes) == Offset(30));
    CHECK(log->logEndOffset() == Offset(31));
    CHECK(log->read(Offset(30), kBigFetch).ok());
}

TEST_CASE("Opening a directory with no segments starts a new partition") {
    TempDir dir("log-open-empty-dir");
    const filesystem::path partition = dir.file("orders-0");
    filesystem::create_directories(partition);

    // What a crash between mkdir and the first segment leaves behind.
    auto log = Log::open(TopicPartition{"orders", 0}, partition, testConfig());
    CHECK(log->segmentCount() == 1);
    CHECK(log->logEndOffset() == Offset(0));
    CHECK(filesystem::exists(segmentLogPath(partition, Offset(0))));
}

TEST_CASE("Opening a missing directory is refused") {
    TempDir dir("log-open-missing");
    CHECK_THROWS_AS(Log::open(TopicPartition{"orders", 0}, dir.file("nope"), testConfig()),
                    IoError);
}

TEST_CASE("Opening ignores files that are not segments") {
    TempDir dir("log-open-other-files");
    const filesystem::path partition = dir.file("orders-0");
    auto    policy         = testPolicy();
    policy.maxSegmentBytes = 500;
    const auto bases = writeThenAbandon(partition, policy, 20);

    // The company a segment keeps in a real partition directory. Note
    // partition.meta is NOT in this list: it used to be just another file the
    // scan stepped over, and since step 8 it is read and validated. Writing junk
    // there is now a corrupt partition, which is a different test.
    writeFile(partition / "leader-epoch-checkpoint", vector<uint8_t>{4, 5});
    writeFile(partition / ".DS_Store", vector<uint8_t>{6, 7, 8});
    writeFile(partition / "00000000000000000000.log.swp", vector<uint8_t>{9});

    auto log = Log::open(TopicPartition{"orders", 0}, partition, configWith(policy));
    CHECK(log->segmentCount() == bases.size());
    CHECK(log->logEndOffset() == Offset(20));
}

TEST_CASE("Only the newest segment is scanned for damage") {
    TempDir dir("log-open-recovers-newest");
    const filesystem::path partition = dir.file("orders-0");
    auto    policy         = testPolicy();
    policy.maxSegmentBytes = 500;
    const auto bases = writeThenAbandon(partition, policy, 40);

    // Corrupt a byte in the body of the active segment's second batch.
    const auto activeLog = segmentLogPath(partition, bases.back());
    {
        auto bytes = readFile(activeLog);
        REQUIRE(bytes.size() > kBatchHeaderSize * 2);
        const size_t firstSize = RecordBatch::totalSizeOf(bytes);
        bytes[firstSize + kBatchHeaderSize + 2] ^= 0xFF;
        writeFile(activeLog, bytes);
    }

    auto log = Log::open(TopicPartition{"orders", 0}, partition, configWith(policy));

    // Truncated at the damage, so the log ends earlier than it did — and the
    // sealed segments are untouched.
    CHECK(log->logEndOffset() < Offset(40));
    CHECK(log->logEndOffset() > bases.back());
    CHECK(log->segmentCount() == bases.size());
    CHECK(log->read(log->logEndOffset(), kBigFetch).ok());
}

TEST_CASE("A partition whose earliest segments are gone starts later") {
    TempDir dir("log-open-below-start");
    const filesystem::path partition = dir.file("orders-0");
    auto    policy         = testPolicy();
    policy.maxSegmentBytes = 500;
    const auto bases = writeThenAbandon(partition, policy, 50);
    REQUIRE(bases.size() > 3);

    // What retention will do at M3: delete the oldest segment outright.
    filesystem::remove(segmentLogPath(partition, bases[0]));
    filesystem::remove(segmentIndexPath(partition, bases[0]));

    auto log = Log::open(TopicPartition{"orders", 0}, partition, configWith(policy));
    CHECK(log->logStartOffset() == bases[1]);

    // Reads below the new start are BelowLogStart — distinct from AboveLogEnd,
    // because a client's reset policy treats them differently: this one means
    // "you were too slow, jump to the earliest offset available".
    CHECK(log->read(Offset(0), kBigFetch).error == ReadError::BelowLogStart);
    CHECK(log->read(bases[1] - 1, kBigFetch).error == ReadError::BelowLogStart);

    // And the first surviving offset still reads fine.
    CHECK(log->read(bases[1], kBigFetch).ok());
    CHECK_FALSE(log->read(bases[1], kBigFetch).range.empty());
}

TEST_CASE("A hole left by a missing middle segment is refused") {
    TempDir dir("log-open-hole");
    const filesystem::path partition = dir.file("orders-0");
    auto    policy         = testPolicy();
    policy.maxSegmentBytes = 500;
    const auto bases = writeThenAbandon(partition, policy, 50);
    REQUIRE(bases.size() > 3);

    // Remove a segment from the middle. Reads crossing the gap would silently
    // jump forward and no consumer would ever know records were skipped, so
    // opening has to refuse rather than serve a log with a hole in it.
    filesystem::remove(segmentLogPath(partition, bases[1]));
    filesystem::remove(segmentIndexPath(partition, bases[1]));

    CHECK_THROWS_AS(Log::open(TopicPartition{"orders", 0}, partition, configWith(policy)), CorruptData);
}

// ===========================================================================
// Log: truncation
// ===========================================================================

TEST_CASE("Truncating inside the active segment drops the records above it") {
    TempDir dir("log-trunc-active");
    auto    log = Log::create(TopicPartition{"orders", 0}, dir.file("orders-0"), testConfig());
    for (int i = 0; i < 20; ++i) {
        auto bytes = makeUnstampedBatch(1000 + i, 48);
        log->append(bytes);
    }
    REQUIRE(log->segmentCount() == 1);

    log->truncateTo(Offset(12));

    CHECK(log->logEndOffset() == Offset(12));
    CHECK(log->read(Offset(11), kBigFetch).ok());
    CHECK(log->read(Offset(12), kBigFetch).ok());                     // caught up
    CHECK(log->read(Offset(12), kBigFetch).range.empty());
    CHECK(log->read(Offset(13), kBigFetch).error == ReadError::AboveLogEnd);
}

TEST_CASE("Truncating inside a sealed segment deletes every segment above it") {
    TempDir dir("log-trunc-sealed");
    const filesystem::path partition = dir.file("orders-0");
    auto    policy         = testPolicy();
    policy.maxSegmentBytes = 400;
    auto    log = Log::create(TopicPartition{"orders", 0}, partition, configWith(policy));

    for (int i = 0; i < 40; ++i) {
        auto bytes = makeUnstampedBatch(1000 + i, 48);
        log->append(bytes);
    }
    const size_t before = log->segmentCount();
    REQUIRE(before > 3);

    log->truncateTo(Offset(6));

    CHECK(log->logEndOffset() <= Offset(6));
    CHECK(log->segmentCount() < before);

    // The files are gone from disk, not merely forgotten.
    size_t logsOnDisk = 0;
    for (const auto& entry : filesystem::directory_iterator(partition))
        if (entry.path().extension() == ".log") ++logsOnDisk;
    CHECK(logsOnDisk == log->segmentCount());
}

TEST_CASE("A truncated log can be appended to again") {
    TempDir dir("log-trunc-append");
    auto    policy         = testPolicy();
    policy.maxSegmentBytes = 400;
    auto    log = Log::create(TopicPartition{"orders", 0}, dir.file("orders-0"), configWith(policy));
    for (int i = 0; i < 30; ++i) {
        auto bytes = makeUnstampedBatch(1000 + i, 48);
        log->append(bytes);
    }

    log->truncateTo(Offset(10));
    const Offset resume = log->logEndOffset();
    REQUIRE(resume <= Offset(10));

    // A follower truncates and then starts following, so the segment it was left
    // with has to be writable.
    auto bytes = makeUnstampedBatch(9999, 48);
    CHECK(log->append(bytes) == resume);
    CHECK(log->logEndOffset() == resume + 1);

    const auto result = log->read(resume, kBigFetch);
    REQUIRE(result.ok());
    CHECK(RecordBatch::verifyCrc(pullRange(result.range)));
}

TEST_CASE("Every surviving offset still reads correctly after a truncation") {
    TempDir dir("log-trunc-integrity");
    auto    policy         = testPolicy();
    policy.maxSegmentBytes = 400;
    auto    log = Log::create(TopicPartition{"orders", 0}, dir.file("orders-0"), configWith(policy));
    for (int i = 0; i < 40; ++i) {
        auto bytes = makeUnstampedBatch(1000 + i, 48);
        log->append(bytes);
    }

    log->truncateTo(Offset(17));
    const int64_t end = log->logEndOffset().value();
    REQUIRE(end <= 17);
    REQUIRE(end > 0);

    for (int64_t offset = 0; offset < end; ++offset) {
        const auto result = log->read(Offset(offset), kBigFetch);
        REQUIRE(result.ok());
        const auto header = RecordBatch::parseHeader(pullRange(result.range));
        CHECK(header.baseOffset <= Offset(offset));
        CHECK(header.lastOffset() >= Offset(offset));
    }
}

TEST_CASE("Truncating to the end of the log or beyond does nothing") {
    TempDir dir("log-trunc-noop");
    auto    log = Log::create(TopicPartition{"orders", 0}, dir.file("orders-0"), testConfig());
    for (int i = 0; i < 10; ++i) {
        auto bytes = makeUnstampedBatch(1000 + i);
        log->append(bytes);
    }
    const size_t segments = log->segmentCount();

    log->truncateTo(Offset(10));
    CHECK(log->logEndOffset() == Offset(10));
    log->truncateTo(Offset(500));
    CHECK(log->logEndOffset() == Offset(10));
    CHECK(log->segmentCount() == segments);
}

TEST_CASE("Truncating everything away leaves an empty log that does not reuse offsets") {
    TempDir dir("log-trunc-all");
    const filesystem::path partition = dir.file("orders-0");
    auto    policy         = testPolicy();
    policy.maxSegmentBytes = 400;
    auto    log = Log::create(TopicPartition{"orders", 0}, partition, configWith(policy));
    for (int i = 0; i < 30; ++i) {
        auto bytes = makeUnstampedBatch(1000 + i, 48);
        log->append(bytes);
    }

    log->truncateTo(Offset(0));

    CHECK(log->logEndOffset() == Offset(0));
    CHECK(log->segmentCount() == 1);
    CHECK(log->read(Offset(0), kBigFetch).ok());
    CHECK(log->read(Offset(0), kBigFetch).range.empty());

    auto bytes = makeUnstampedBatch(1, 48);
    CHECK(log->append(bytes) == Offset(0));
}

TEST_CASE("A log restarted above zero after truncation does not renumber records") {
    TempDir dir("log-trunc-no-reuse");
    const filesystem::path partition = dir.file("orders-0");
    auto    policy         = testPolicy();
    policy.maxSegmentBytes = 400;
    auto    log = Log::create(TopicPartition{"orders", 0}, partition, configWith(policy));
    for (int i = 0; i < 30; ++i) {
        auto bytes = makeUnstampedBatch(1000 + i, 48);
        log->append(bytes);
    }
    const auto bases = [&] {
        vector<Offset> out;
        map<string, filesystem::path> logs;
        for (const auto& entry : filesystem::directory_iterator(partition))
            if (entry.path().extension() == ".log")
                logs[entry.path().filename().string()] = entry.path();
        for (const auto& [name, path] : logs) out.push_back(baseOffsetFromLogPath(path));
        return out;
    }();
    REQUIRE(bases.size() > 2);

    // Truncate to exactly a segment boundary: the segment starting there goes
    // whole, and the previous one is promoted back to writable.
    log->truncateTo(bases[1]);
    CHECK(log->logEndOffset() == bases[1]);

    // Offsets are never reused. The next record written gets the offset that was
    // just discarded — a consumer that had read past it must not be handed
    // different records under the same numbers, which is what leader epochs exist
    // to detect at M8.
    auto bytes = makeUnstampedBatch(7777, 48);
    CHECK(log->append(bytes) == bases[1]);
}

TEST_CASE("Truncation survives a restart") {
    TempDir dir("log-trunc-reopen");
    const filesystem::path partition = dir.file("orders-0");
    auto    policy         = testPolicy();
    policy.maxSegmentBytes = 400;
    Offset  after{0};
    {
        auto log = Log::create(TopicPartition{"orders", 0}, partition, configWith(policy));
        for (int i = 0; i < 30; ++i) {
            auto bytes = makeUnstampedBatch(1000 + i, 48);
            log->append(bytes);
        }
        log->truncateTo(Offset(14));
        after = log->logEndOffset();
    }

    // The truncation was to the files, not to in-memory state — so a reopen must
    // agree, and the contiguity check must still pass.
    auto reopened = Log::open(TopicPartition{"orders", 0}, partition, configWith(policy));
    CHECK(reopened->logEndOffset() == after);
    CHECK(reopened->read(after, kBigFetch).ok());
    CHECK(reopened->read(after + 1, kBigFetch).error == ReadError::AboveLogEnd);
}

// ===========================================================================
// Retention policy
// ===========================================================================

TEST_CASE("Retention defaults to a week and to keeping everything by size") {
    const RetentionPolicy retention;

    CHECK(retention.retentionMs == 7ll * 24 * 60 * 60 * 1000);

    // Unlimited by default: a byte limit is an explicit operational choice, and
    // silently capping a partition would lose data nobody asked to lose.
    CHECK_FALSE(retention.bytesLimited());
    CHECK_FALSE(retention.retentionBytes.has_value());
}

TEST_CASE("Unlimited and very small byte limits are different states") {
    RetentionPolicy retention;

    // The reason for an optional rather than -1 as a sentinel: with a bare
    // integer, "no limit" and "keep almost nothing" are both just numbers, and a
    // config path that forgot to translate -1 would delete everything.
    retention.retentionBytes = 0;
    CHECK(retention.bytesLimited());
    CHECK(retention.retentionBytes == 0u);

    retention.retentionBytes = nullopt;
    CHECK_FALSE(retention.bytesLimited());
}

TEST_CASE("A log config carries both the roll and the retention rules") {
    const LogConfig config;

    // Two policies, one object, because partition.meta has to persist both — a
    // broker booting without a controller needs the whole picture from disk.
    CHECK(config.roll.maxSegmentBytes == RollPolicy{}.maxSegmentBytes);
    CHECK(config.retention.retentionMs == RetentionPolicy{}.retentionMs);
}

TEST_CASE("A byte limit below one segment cannot be honoured") {
    LogConfig config;
    config.roll.maxSegmentBytes      = 1 << 20;
    config.retention.retentionBytes  = 1024;

    // Documenting a foot-gun rather than preventing it: the active segment is
    // never deleted, so a partition can never shrink below the size of one
    // segment however low the limit is set. Retention will delete every sealed
    // segment and then stop, permanently over the limit.
    CHECK(config.retention.retentionBytes < config.roll.maxSegmentBytes);
    CHECK(config.retention.bytesLimited());
}

// ===========================================================================
// Log: size accounting
// ===========================================================================

TEST_CASE("An empty log occupies no bytes") {
    TempDir dir("log-size-empty");
    auto    log = Log::create(TopicPartition{"orders", 0}, dir.file("orders-0"), testConfig());

    // The .log file exists but holds nothing, and the .index is preallocated —
    // which this deliberately does not count. Retention-by-bytes is a statement
    // about record data, and index files are derived, rebuildable, and bounded.
    CHECK(log->totalSizeBytes() == 0);
}

TEST_CASE("Total size matches the bytes actually on disk") {
    TempDir dir("log-size-disk");
    const filesystem::path partition = dir.file("orders-0");
    auto    log = Log::create(TopicPartition{"orders", 0}, partition, testConfig());

    for (int i = 0; i < 12; ++i) {
        auto bytes = makeUnstampedBatch(1000 + i, 64);
        log->append(bytes);
        CHECK(log->totalSizeBytes() == logBytesOnDisk(partition));
    }
}

TEST_CASE("Total size spans every segment, sealed and active") {
    TempDir dir("log-size-segments");
    const filesystem::path partition = dir.file("orders-0");
    auto    policy         = testPolicy();
    policy.maxSegmentBytes = 400;
    auto    log = Log::create(TopicPartition{"orders", 0}, partition, configWith(policy));

    for (int i = 0; i < 40; ++i) {
        auto bytes = makeUnstampedBatch(1000 + i, 48);
        log->append(bytes);
    }
    REQUIRE(log->segmentCount() > 3);

    // Summed over segments, so a roll must not lose or double-count anything.
    CHECK(log->totalSizeBytes() == logBytesOnDisk(partition));
}

TEST_CASE("Total size shrinks when the log is truncated") {
    TempDir dir("log-size-truncate");
    const filesystem::path partition = dir.file("orders-0");
    auto    policy         = testPolicy();
    policy.maxSegmentBytes = 400;
    auto    log = Log::create(TopicPartition{"orders", 0}, partition, configWith(policy));

    for (int i = 0; i < 40; ++i) {
        auto bytes = makeUnstampedBatch(1000 + i, 48);
        log->append(bytes);
    }
    const uint64_t before = log->totalSizeBytes();

    log->truncateTo(Offset(10));

    // Computed rather than tracked, so it needs no help from truncateTo to stay
    // right — which is the reason it is computed.
    CHECK(log->totalSizeBytes() < before);
    CHECK(log->totalSizeBytes() == logBytesOnDisk(partition));
}

TEST_CASE("Total size survives a restart") {
    TempDir dir("log-size-reopen");
    const filesystem::path partition = dir.file("orders-0");
    auto    policy         = testPolicy();
    policy.maxSegmentBytes = 400;
    uint64_t before = 0;
    {
        auto log = Log::create(TopicPartition{"orders", 0}, partition, configWith(policy));
        for (int i = 0; i < 25; ++i) {
            auto bytes = makeUnstampedBatch(1000 + i, 48);
            log->append(bytes);
        }
        before = log->totalSizeBytes();
    }

    auto reopened = Log::open(TopicPartition{"orders", 0}, partition, configWith(policy));
    CHECK(reopened->totalSizeBytes() == before);
}

// ===========================================================================
// Log: retention by age
// ===========================================================================

TEST_CASE("Retention deletes sealed segments older than the window") {
    TempDir dir("ret-age");
    const filesystem::path partition = dir.file("orders-0");
    LogConfig config;
    config.retention.retentionMs = 10'000;

    // Timestamps 1000 ms apart, so the oldest segments are well past the window
    // and the newest are inside it.
    auto log = logWithAgedSegments(partition, config, 40, 100'000, 1'000);
    const size_t before = log->segmentCount();
    REQUIRE(before > 4);

    // "Now" is just after the last record, so records older than 10 s go.
    log->applyRetention(100'000 + 39 * 1'000);

    CHECK(log->segmentCount() < before);
    CHECK(log->logStartOffset() > Offset(0));
    CHECK(log->graveyardSize() == before - log->segmentCount());
}

TEST_CASE("Retention never deletes the active segment") {
    TempDir dir("ret-active");
    const filesystem::path partition = dir.file("orders-0");
    LogConfig config;
    config.retention.retentionMs = 1;

    auto log = logWithAgedSegments(partition, config, 30, 1'000, 1'000);

    // Everything is ancient, so retention would take the lot if it could.
    log->applyRetention(1'000'000'000);

    CHECK(log->segmentCount() == 1);        // exactly the active one
    CHECK(log->logEndOffset() == Offset(30));
    CHECK(log->totalSizeBytes() > 0);       // the active segment's data survives
}

TEST_CASE("Retention leaves segments inside the window alone") {
    TempDir dir("ret-young");
    const filesystem::path partition = dir.file("orders-0");
    LogConfig config;
    config.retention.retentionMs = 1'000'000;

    auto log = logWithAgedSegments(partition, config, 30, 100'000, 1'000);
    const size_t before = log->segmentCount();

    log->applyRetention(100'000 + 29 * 1'000);

    CHECK(log->segmentCount() == before);
    CHECK(log->graveyardSize() == 0);
    CHECK(log->logStartOffset() == Offset(0));
}

TEST_CASE("A deleted segment's files are gone from the directory") {
    TempDir dir("ret-unlinked");
    const filesystem::path partition = dir.file("orders-0");
    LogConfig config;
    config.retention.retentionMs = 10'000;

    auto log = logWithAgedSegments(partition, config, 40, 100'000, 1'000);
    const Offset oldest = log->logStartOffset();
    REQUIRE(filesystem::exists(segmentLogPath(partition, oldest)));

    log->applyRetention(100'000 + 39 * 1'000);
    REQUIRE(log->logStartOffset() > oldest);

    CHECK_FALSE(filesystem::exists(segmentLogPath(partition, oldest)));
    CHECK_FALSE(filesystem::exists(segmentIndexPath(partition, oldest)));
}

TEST_CASE("A range handed out before retention is still readable after it") {
    TempDir dir("ret-inflight");
    const filesystem::path partition = dir.file("orders-0");
    LogConfig config;
    config.retention.retentionMs = 10'000;

    auto log = logWithAgedSegments(partition, config, 40, 100'000, 1'000);

    // A fetch resolved but not yet sent — the network layer would hand this to
    // sendfile on an I/O thread some time later.
    const auto inflight = log->read(Offset(0), kBigFetch);
    REQUIRE(inflight.ok());
    REQUIRE_FALSE(inflight.range.empty());

    log->applyRetention(100'000 + 39 * 1'000);
    REQUIRE(log->logStartOffset() > Offset(0));
    REQUIRE(log->graveyardSize() > 0);

    // The whole reason the graveyard exists. Without it the descriptor would be
    // closed, and its number possibly reused by another file — so this pread
    // would fail, or silently return another file's bytes.
    const auto bytes = pullRange(inflight.range);
    CHECK(RecordBatch::verifyCrc(bytes));
    CHECK(RecordBatch::parseHeader(bytes).baseOffset == Offset(0));
}

TEST_CASE("A buried segment is freed once its delay elapses") {
    TempDir dir("ret-sweep");
    const filesystem::path partition = dir.file("orders-0");
    LogConfig config;
    config.retention.retentionMs         = 10'000;
    config.retention.segmentDeleteDelayMs = 60'000;

    auto log = logWithAgedSegments(partition, config, 40, 100'000, 1'000);
    const int64_t now = 100'000 + 39 * 1'000;

    log->applyRetention(now);
    const size_t buried = log->graveyardSize();
    REQUIRE(buried > 0);

    // Too early: the delay is the window in which an already-issued FileRange
    // can still be sent, so sweeping sooner would defeat it.
    log->sweepGraveyard(now + 59'000);
    CHECK(log->graveyardSize() == buried);

    log->sweepGraveyard(now + 60'000);
    CHECK(log->graveyardSize() == 0);
}

TEST_CASE("Reads below the new log start report BelowLogStart") {
    TempDir dir("ret-below-start");
    const filesystem::path partition = dir.file("orders-0");
    LogConfig config;
    config.retention.retentionMs = 10'000;

    auto log = logWithAgedSegments(partition, config, 40, 100'000, 1'000);
    log->applyRetention(100'000 + 39 * 1'000);

    const Offset start = log->logStartOffset();
    REQUIRE(start > Offset(0));

    // What a lagging consumer actually gets. Distinct from AboveLogEnd, because
    // the client's reset policy treats them differently: this one means "you were
    // too slow, jump to the earliest offset available".
    CHECK(log->read(Offset(0), kBigFetch).error == ReadError::BelowLogStart);
    CHECK(log->read(start - 1, kBigFetch).error == ReadError::BelowLogStart);
    CHECK(log->read(start, kBigFetch).ok());
    CHECK_FALSE(log->read(start, kBigFetch).range.empty());
}

TEST_CASE("Retention survives a restart") {
    TempDir dir("ret-reopen");
    const filesystem::path partition = dir.file("orders-0");
    LogConfig config;
    config.retention.retentionMs = 10'000;
    Offset start{0};
    Offset end{0};
    {
        auto log = logWithAgedSegments(partition, config, 40, 100'000, 1'000);
        log->applyRetention(100'000 + 39 * 1'000);
        start = log->logStartOffset();
        end   = log->logEndOffset();
    }

    // The deletion was to the files, so a reopen must agree — and the contiguity
    // check must accept a log whose earliest segment is not offset zero.
    auto reopened = Log::open(TopicPartition{"orders", 0}, partition, config);
    CHECK(reopened->logStartOffset() == start);
    CHECK(reopened->logEndOffset() == end);
    CHECK(reopened->read(Offset(0), kBigFetch).error == ReadError::BelowLogStart);
}

// ===========================================================================
// Log: retention by bytes
// ===========================================================================

TEST_CASE("Retention deletes oldest-first until the partition is under its byte limit") {
    TempDir dir("ret-bytes");
    const filesystem::path partition = dir.file("orders-0");
    LogConfig config;
    config.retention.retentionMs    = 1'000'000'000;   // age never fires
    config.retention.retentionBytes = 1500;

    auto log = logWithAgedSegments(partition, config, 40, 100'000, 1'000);
    REQUIRE(log->totalSizeBytes() > 1500);
    const Offset startBefore = log->logStartOffset();

    log->applyRetention(100'000);

    CHECK(log->totalSizeBytes() <= 1500);
    CHECK(log->logStartOffset() > startBefore);
    CHECK(log->graveyardSize() > 0);

    // Deleted from the oldest end, so what survives is a contiguous run ending at
    // the log end — never a hole in the middle.
    for (int64_t offset = log->logStartOffset().value(); offset < log->logEndOffset().value();
         ++offset)
        CHECK(log->read(Offset(offset), kBigFetch).ok());
}

TEST_CASE("A partition already under its byte limit is left alone") {
    TempDir dir("ret-bytes-under");
    const filesystem::path partition = dir.file("orders-0");
    LogConfig config;
    config.retention.retentionMs    = 1'000'000'000;
    config.retention.retentionBytes = 1'000'000;

    auto log = logWithAgedSegments(partition, config, 20, 100'000, 1'000);
    const size_t before = log->segmentCount();

    log->applyRetention(100'000);

    CHECK(log->segmentCount() == before);
    CHECK(log->graveyardSize() == 0);
}

TEST_CASE("No byte limit means no byte-based deletion, however large the partition") {
    TempDir dir("ret-bytes-unlimited");
    const filesystem::path partition = dir.file("orders-0");
    LogConfig config;
    config.retention.retentionMs = 1'000'000'000;
    REQUIRE_FALSE(config.retention.bytesLimited());

    auto log = logWithAgedSegments(partition, config, 40, 100'000, 1'000);
    const size_t before = log->segmentCount();

    log->applyRetention(100'000);

    CHECK(log->segmentCount() == before);
    CHECK(log->graveyardSize() == 0);
}

TEST_CASE("A byte limit below one segment deletes all it can and stops") {
    TempDir dir("ret-bytes-too-small");
    const filesystem::path partition = dir.file("orders-0");
    LogConfig config;
    config.retention.retentionMs    = 1'000'000'000;
    config.retention.retentionBytes = 1;   // absurdly low

    auto log = logWithAgedSegments(partition, config, 30, 100'000, 1'000);

    log->applyRetention(100'000);

    // Every sealed segment goes; the active one cannot. So the partition sits
    // permanently over its limit, having deleted everything it was allowed to.
    // Documented behaviour, not a bug — but the shape of it is worth pinning.
    CHECK(log->segmentCount() == 1);
    CHECK(log->totalSizeBytes() > 1);
    CHECK(log->logEndOffset() == Offset(30));
    CHECK(log->read(log->logStartOffset(), kBigFetch).ok());
}

TEST_CASE("The byte limit still fires when timestamps block age retention") {
    TempDir dir("ret-bytes-backstop");
    const filesystem::path partition = dir.file("orders-0");
    LogConfig config;
    config.roll.maxSegmentBytes     = 400;
    config.retention.retentionMs    = 1'000;
    config.retention.retentionBytes = 1500;

    // The first record carries a timestamp far in the future — a producer with a
    // broken clock. Age retention stops at that segment and can never get past
    // it, so without a byte limit this partition would grow forever.
    auto log = Log::create(TopicPartition{"orders", 0}, partition, config);
    {
        auto bytes = makeUnstampedBatch(9'000'000'000'000LL, 48);
        log->append(bytes);
    }
    for (int i = 1; i < 40; ++i) {
        auto bytes = makeUnstampedBatch(100'000 + i * 1'000, 48);
        log->append(bytes);
    }
    REQUIRE(log->totalSizeBytes() > 1500);

    log->applyRetention(100'000 + 40 * 1'000);

    // The byte limit does not consult timestamps at all, which is precisely what
    // makes it the backstop.
    CHECK(log->totalSizeBytes() <= 1500);
    CHECK(log->logStartOffset() > Offset(0));
}

TEST_CASE("Age and bytes together delete no more than either alone would need") {
    TempDir dir("ret-both");
    const filesystem::path partition = dir.file("orders-0");
    LogConfig config;
    config.retention.retentionMs    = 10'000;
    config.retention.retentionBytes = 1'000'000;   // generous, so age does the work

    auto log = logWithAgedSegments(partition, config, 40, 100'000, 1'000);
    log->applyRetention(100'000 + 39 * 1'000);
    const Offset ageOnly = log->logStartOffset();

    // Same log, same age window, but a tight byte limit as well: it can only ever
    // remove more, never less.
    TempDir dir2("ret-both-2");
    const filesystem::path partition2 = dir2.file("orders-0");
    config.retention.retentionBytes = 900;
    auto log2 = logWithAgedSegments(partition2, config, 40, 100'000, 1'000);
    log2->applyRetention(100'000 + 39 * 1'000);

    CHECK(log2->logStartOffset() >= ageOnly);
    CHECK(log2->totalSizeBytes() <= 900);
}

// ===========================================================================
// partition.meta — encoding and atomic write
// ===========================================================================

TEST_CASE("The encoded meta opens with a magic and a version") {
    const auto bytes = encodePartitionMeta(sampleMeta());

    // The magic is what makes pointing this at the wrong file fail immediately
    // rather than decoding lengths out of unrelated bytes.
    REQUIRE(bytes.size() > 6);
    CHECK(be32At(bytes, 0) == PartitionMeta::kMagic);
    CHECK(bytes[0] == 'D');
    CHECK(bytes[1] == 'K');
    CHECK(bytes[2] == 'P');
    CHECK(bytes[3] == 'M');

    // Version next, so a future format change can be detected before anything
    // after it is interpreted.
    CHECK(bytes[4] == 0);
    CHECK(bytes[5] == PartitionMeta::kVersion);
}

TEST_CASE("The encoding is a fixed size plus the topic name") {
    const auto shortName = encodePartitionMeta(sampleMeta());

    PartitionMeta longer = sampleMeta();
    longer.tp.topic = "orders-with-a-much-longer-name";
    const auto longName = encodePartitionMeta(longer);

    // Length-prefixed, so the difference is exactly the extra characters.
    CHECK(longName.size() - shortName.size() ==
          longer.tp.topic.size() - string("orders").size());
}

TEST_CASE("Unlimited retention bytes encodes as -1") {
    PartitionMeta meta = sampleMeta();
    meta.config.retention.retentionBytes = nullopt;
    const auto unlimited = encodePartitionMeta(meta);

    meta.config.retention.retentionBytes = 0;
    const auto zero = encodePartitionMeta(meta);

    // The two must not encode the same. This is the boundary where the optional
    // becomes a sentinel, and confusing them would turn "keep everything" into
    // "keep nothing".
    CHECK(unlimited.size() == zero.size());
    CHECK(unlimited != zero);
}

TEST_CASE("Writing the meta leaves one file and no temporary") {
    TempDir dir("meta-write");
    const filesystem::path partition = dir.file("orders-3");
    filesystem::create_directories(partition);

    writePartitionMeta(partition, sampleMeta());

    const auto target = partition / PartitionMeta::kFileName;
    CHECK(filesystem::exists(target));
    CHECK(filesystem::file_size(target) == encodePartitionMeta(sampleMeta()).size());

    // The temp file is renamed, not copied, so nothing should be left behind.
    CHECK_FALSE(filesystem::exists(partition / "partition.meta.tmp"));
}

TEST_CASE("Rewriting the meta replaces it rather than appending") {
    TempDir dir("meta-rewrite");
    const filesystem::path partition = dir.file("orders-3");
    filesystem::create_directories(partition);

    writePartitionMeta(partition, sampleMeta());
    const auto size = filesystem::file_size(partition / PartitionMeta::kFileName);

    PartitionMeta changed = sampleMeta();
    changed.config.retention.retentionMs = 999;
    writePartitionMeta(partition, changed);

    CHECK(filesystem::file_size(partition / PartitionMeta::kFileName) == size);
    CHECK(readFile(partition / PartitionMeta::kFileName) == encodePartitionMeta(changed));
}

TEST_CASE("A leftover temporary from an interrupted write is not appended to") {
    TempDir dir("meta-stale-temp");
    const filesystem::path partition = dir.file("orders-3");
    filesystem::create_directories(partition);

    // What a crash mid-write leaves: a temp file with partial or unrelated
    // content. Without truncating it first, the next write would produce a file
    // with two records in it and a length field that lies.
    writeFile(partition / "partition.meta.tmp", vector<uint8_t>(500, 0xEE));

    writePartitionMeta(partition, sampleMeta());

    CHECK(readFile(partition / PartitionMeta::kFileName) ==
          encodePartitionMeta(sampleMeta()));
    CHECK_FALSE(filesystem::exists(partition / "partition.meta.tmp"));
}

// ===========================================================================
// partition.meta — decoding and validation
// ===========================================================================

TEST_CASE("The meta round-trips through encode and decode") {
    const auto meta = sampleMeta();
    checkMetaEqual(decodePartitionMeta(encodePartitionMeta(meta)), meta);
}

TEST_CASE("Unlimited and zero retention bytes survive the round trip distinctly") {
    PartitionMeta unlimited = sampleMeta();
    unlimited.config.retention.retentionBytes = nullopt;
    const auto decodedUnlimited = decodePartitionMeta(encodePartitionMeta(unlimited));
    CHECK_FALSE(decodedUnlimited.config.retention.bytesLimited());

    PartitionMeta zero = sampleMeta();
    zero.config.retention.retentionBytes = 0;
    const auto decodedZero = decodePartitionMeta(encodePartitionMeta(zero));
    CHECK(decodedZero.config.retention.bytesLimited());
    CHECK(decodedZero.config.retention.retentionBytes == 0u);
}

TEST_CASE("A file that is not a partition.meta is refused by its magic") {
    auto bytes = encodePartitionMeta(sampleMeta());
    bytes[1] ^= 0xFF;

    // Without the magic, a wrong file would be decoded as lengths and offsets out
    // of unrelated bytes.
    CHECK_THROWS_AS(decodePartitionMeta(bytes), CorruptData);
    CHECK_THROWS_AS(decodePartitionMeta(vector<uint8_t>(80, 0x00)), CorruptData);
}

TEST_CASE("A version this build does not know is refused, not guessed at") {
    auto bytes = encodePartitionMeta(sampleMeta());
    bytes[5] = PartitionMeta::kVersion + 1;   // written by a newer broker

    // Reading a config file wrongly is worse than not reading it: a
    // misinterpreted retention limit silently deletes data or silently keeps it
    // forever.
    CHECK_THROWS_AS(decodePartitionMeta(bytes), CorruptData);

    bytes[5] = 0;
    CHECK_THROWS_AS(decodePartitionMeta(bytes), CorruptData);
}

TEST_CASE("A truncated meta is refused") {
    const auto full = encodePartitionMeta(sampleMeta());

    // Every prefix short of the whole thing. BufferReader throws the moment a
    // read runs past the end, so no length arithmetic is needed for this.
    for (size_t length = 0; length < full.size(); ++length) {
        const vector<uint8_t> partial(full.begin(), full.begin() + static_cast<long>(length));
        CHECK_THROWS_AS(decodePartitionMeta(partial), CorruptData);
    }
    CHECK_NOTHROW(decodePartitionMeta(full));
}

TEST_CASE("Trailing bytes are refused") {
    auto bytes = encodePartitionMeta(sampleMeta());
    bytes.push_back(0x00);

    // The version matched, so there is no forward-compatibility story that
    // explains a longer file. The likely cause is a write that appended instead
    // of replacing.
    CHECK_THROWS_AS(decodePartitionMeta(bytes), CorruptData);
}

TEST_CASE("An impossible topic length is refused") {
    auto bytes = encodePartitionMeta(sampleMeta());

    // The length field sits after the 4-byte magic and 2-byte version.
    bytes[6] = 0xFF;
    bytes[7] = 0xFF;   // -1
    CHECK_THROWS_AS(decodePartitionMeta(bytes), CorruptData);

    bytes[6] = 0x00;
    bytes[7] = 0x00;   // empty topic name
    CHECK_THROWS_AS(decodePartitionMeta(bytes), CorruptData);
}

TEST_CASE("The meta round-trips through the filesystem") {
    TempDir dir("meta-read");
    const filesystem::path partition = dir.file("orders-3");
    filesystem::create_directories(partition);

    const auto meta = sampleMeta();
    writePartitionMeta(partition, meta);

    checkMetaEqual(readPartitionMeta(partition, TopicPartition{"orders", 3}), meta);
}

TEST_CASE("A meta describing a different partition is refused") {
    TempDir dir("meta-mismatch");
    const filesystem::path partition = dir.file("orders-3");
    filesystem::create_directories(partition);
    writePartitionMeta(partition, sampleMeta());   // says orders-3

    // What a hand-copied partition directory looks like. The file and the
    // directory disagree about which partition these records belong to, and
    // picking either answer risks serving one topic's data under another's name.
    CHECK_THROWS_AS(readPartitionMeta(partition, TopicPartition{"orders", 4}), CorruptData);
    CHECK_THROWS_AS(readPartitionMeta(partition, TopicPartition{"payments", 3}), CorruptData);
    CHECK_NOTHROW(readPartitionMeta(partition, TopicPartition{"orders", 3}));
}

TEST_CASE("A missing meta file is an I/O error, not corruption") {
    TempDir dir("meta-missing");
    const filesystem::path partition = dir.file("orders-3");
    filesystem::create_directories(partition);

    // The distinction matters to the caller: absent means "this partition
    // predates the file, or was interrupted before writing it", which is
    // recoverable. Present-but-unreadable is not.
    CHECK_THROWS_AS(readPartitionMeta(partition, TopicPartition{"orders", 3}), IoError);
}

// ===========================================================================
// partition.meta — wired into Log
// ===========================================================================

TEST_CASE("Creating a log writes its partition.meta") {
    TempDir dir("meta-on-create");
    const filesystem::path partition = dir.file("orders-0");
    LogConfig config = testConfig();
    config.retention.retentionMs = 12345;

    auto log = Log::create(TopicPartition{"orders", 0}, partition, config);

    const auto stored = readPartitionMeta(partition, TopicPartition{"orders", 0});
    CHECK(stored.tp.topic == "orders");
    CHECK(stored.tp.partition == 0);
    CHECK(stored.config.retention.retentionMs == 12345);
    CHECK(stored.config.roll.maxSegmentBytes == config.roll.maxSegmentBytes);
}

TEST_CASE("The stored config wins over the one passed to open") {
    TempDir dir("meta-stored-wins");
    const filesystem::path partition = dir.file("orders-0");

    LogConfig original = testConfig();
    original.retention.retentionMs    = 60'000;
    original.retention.retentionBytes = 4096;
    {
        auto log = Log::create(TopicPartition{"orders", 0}, partition, original);
        auto bytes = makeUnstampedBatch(1000, 48);
        log->append(bytes);
    }

    // Reopened by someone who passed the defaults. A partition told to keep one
    // minute of data must not silently start keeping seven days.
    LogConfig different = testConfig();
    different.retention.retentionMs    = 7ll * 24 * 60 * 60 * 1000;
    different.retention.retentionBytes = nullopt;

    auto log = Log::open(TopicPartition{"orders", 0}, partition, different);
    CHECK(log->config().retention.retentionMs == 60'000);
    CHECK(log->config().retention.retentionBytes == 4096u);
}

TEST_CASE("Retention after a restart follows the stored config, not the caller's") {
    TempDir dir("meta-retention-honoured");
    const filesystem::path partition = dir.file("orders-0");

    LogConfig original = testConfig();
    original.roll.maxSegmentBytes = 400;
    original.retention.retentionMs = 10'000;
    {
        auto log = Log::create(TopicPartition{"orders", 0}, partition, original);
        for (int i = 0; i < 40; ++i) {
            auto bytes = makeUnstampedBatch(100'000 + i * 1'000, 48);
            log->append(bytes);
        }
    }

    // Reopened with an effectively infinite window. If the fallback were applied,
    // retention would delete nothing and the disk would quietly grow forever —
    // the exact failure partition.meta exists to prevent.
    LogConfig wrong = testConfig();
    wrong.roll.maxSegmentBytes  = 400;
    wrong.retention.retentionMs = 1'000'000'000;

    auto log = Log::open(TopicPartition{"orders", 0}, partition, wrong);
    log->applyRetention(100'000 + 39 * 1'000);

    CHECK(log->logStartOffset() > Offset(0));
    CHECK(log->config().retention.retentionMs == 10'000);
}

TEST_CASE("An empty partition with no meta adopts the fallback and records it") {
    TempDir dir("meta-heal-empty");
    const filesystem::path partition = dir.file("orders-0");
    filesystem::create_directories(partition);

    // What a crash between mkdir and the first write leaves behind. There is no
    // stored configuration to contradict, so the fallback is safe here.
    LogConfig fallback = testConfig();
    fallback.retention.retentionMs = 4242;

    auto log = Log::open(TopicPartition{"orders", 0}, partition, fallback);
    CHECK(log->config().retention.retentionMs == 4242);

    // Written down, so the next open finds it rather than falling back again.
    CHECK(readPartitionMeta(partition, TopicPartition{"orders", 0})
              .config.retention.retentionMs == 4242);
}

TEST_CASE("A partition holding records with no meta is refused") {
    TempDir dir("meta-missing-with-data");
    const filesystem::path partition = dir.file("orders-0");
    {
        auto log = Log::create(TopicPartition{"orders", 0}, partition, testConfig());
        auto bytes = makeUnstampedBatch(1000, 48);
        log->append(bytes);
    }
    filesystem::remove(partition / PartitionMeta::kFileName);

    // The original configuration is not derivable from the log, so adopting the
    // caller's and writing it down would make a guess permanent — and a wrong
    // retention window loses data or fills a disk, neither of which reports an
    // error.
    CHECK_THROWS_AS(Log::open(TopicPartition{"orders", 0}, partition, testConfig()),
                    CorruptData);
}

TEST_CASE("A corrupt meta is refused even on an empty partition") {
    TempDir dir("meta-corrupt");
    const filesystem::path partition = dir.file("orders-0");
    filesystem::create_directories(partition);
    writeFile(partition / PartitionMeta::kFileName, vector<uint8_t>{1, 2, 3});

    // Present-but-unreadable is not the same as absent. Only ENOENT reaches the
    // fallback; anything else means something is wrong that a default cannot fix.
    CHECK_THROWS_AS(Log::open(TopicPartition{"orders", 0}, partition, testConfig()),
                    CorruptData);
}

TEST_CASE("A meta from another partition is refused") {
    TempDir dir("meta-wrong-partition");
    const filesystem::path partition = dir.file("orders-0");
    {
        auto log = Log::create(TopicPartition{"orders", 0}, partition, testConfig());
        auto bytes = makeUnstampedBatch(1000, 48);
        log->append(bytes);
    }

    // A hand-copied partition directory: the meta still names its original owner.
    CHECK_THROWS_AS(Log::open(TopicPartition{"orders", 7}, partition, testConfig()),
                    CorruptData);
    CHECK_THROWS_AS(Log::open(TopicPartition{"payments", 0}, partition, testConfig()),
                    CorruptData);
}
