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
// LogManager: registry
// ===========================================================================

static_assert(!is_copy_constructible_v<LogManager>,
              "a LogManager owns every partition on the broker; copying one would mean two "
              "owners of the same files");

TEST_CASE("A new manager creates its data directory") {
    TempDir dir("mgr-create-dir");
    const filesystem::path dataDir = dir.file("data/broker-1");
    REQUIRE_FALSE(filesystem::exists(dataDir));

    // First boot creates it rather than refusing to start: an empty data
    // directory and a missing one describe the same situation.
    LogManager manager(dataDir, testConfig());

    CHECK(filesystem::is_directory(dataDir));
    CHECK(manager.dataDir() == dataDir);
    CHECK(manager.partitionCount() == 0);
}

TEST_CASE("An existing data directory is adopted, not replaced") {
    TempDir dir("mgr-existing-dir");
    const filesystem::path dataDir = dir.file("data");
    filesystem::create_directories(dataDir / "orders-0");

    LogManager manager(dataDir, testConfig());

    // Nothing is scanned yet — that is loadAll — but nothing is destroyed either.
    CHECK(filesystem::is_directory(dataDir / "orders-0"));
    CHECK(manager.partitionCount() == 0);
}

TEST_CASE("A partition this broker does not host resolves to nullptr") {
    TempDir dir("mgr-get-missing");
    LogManager manager(dir.file("data"), testConfig());

    // Not an exception: "I do not host that" is a routine answer a broker gives
    // constantly, and M4 maps it to a NOT_LEADER error code rather than a failure.
    CHECK(manager.get(TopicPartition{"orders", 0}) == nullptr);
    CHECK(manager.get(TopicPartition{"", 0}) == nullptr);
    CHECK(manager.get(TopicPartition{"orders", -1}) == nullptr);
}

TEST_CASE("A manager keeps the defaults it was given") {
    TempDir dir("mgr-defaults");
    LogConfig defaults = testConfig();
    defaults.retention.retentionMs    = 3600'000;
    defaults.retention.retentionBytes = 65536;

    LogManager manager(dir.file("data"), defaults);

    // These apply to partitions created from here on, and to one found on disk
    // with no partition.meta of its own — never to one that has its own.
    CHECK(manager.defaults().retention.retentionMs == 3600'000);
    CHECK(manager.defaults().retention.retentionBytes == 65536u);
}

TEST_CASE("Topic and partition are both part of a partition's identity") {
    TempDir dir("mgr-identity");
    LogManager manager(dir.file("data"), testConfig());

    // Same topic different partition, and same partition different topic, are
    // different partitions. hash<TopicPartition> mixes both, because a broker's
    // keys are dominated by one topic with many partitions — without mixing they
    // would all land in adjacent buckets.
    CHECK(manager.get(TopicPartition{"orders", 0}) == nullptr);
    CHECK(manager.get(TopicPartition{"orders", 1}) == nullptr);
    CHECK(manager.get(TopicPartition{"payments", 0}) == nullptr);

    const TopicPartition a{"orders", 3};
    const TopicPartition b{"orders", 3};
    CHECK(a == b);
    CHECK(std::hash<TopicPartition>{}(a) == std::hash<TopicPartition>{}(b));
    CHECK(std::hash<TopicPartition>{}(a) != std::hash<TopicPartition>{}(TopicPartition{"orders", 4}));
}

// ===========================================================================
// LogManager: creating partitions
// ===========================================================================

TEST_CASE("Creating a partition registers it and lays it out on disk") {
    TempDir dir("mgr-create-partition");
    const filesystem::path dataDir = dir.file("data");
    LogManager manager(dataDir, testConfig());

    Log& log = manager.createPartition(TopicPartition{"orders", 3});

    CHECK(manager.partitionCount() == 1);
    CHECK(manager.get(TopicPartition{"orders", 3}) == &log);
    CHECK(log.logEndOffset() == Offset(0));

    // The directory name IS the partition identity, so this doubles as the
    // on-disk layout rather than being a debug convenience.
    const filesystem::path partition = dataDir / "orders-3";
    CHECK(filesystem::is_directory(partition));
    CHECK(filesystem::exists(segmentLogPath(partition, Offset(0))));
    CHECK(filesystem::exists(segmentIndexPath(partition, Offset(0))));
    CHECK(filesystem::exists(partition / PartitionMeta::kFileName));
}

TEST_CASE("A created partition is immediately usable") {
    TempDir dir("mgr-create-usable");
    LogManager manager(dir.file("data"), testConfig());

    Log& log = manager.createPartition(TopicPartition{"orders", 0});
    auto bytes = makeUnstampedBatch(1000, 48);

    CHECK(log.append(bytes) == Offset(0));
    CHECK(log.read(Offset(0), kBigFetch).ok());
    CHECK(manager.get(TopicPartition{"orders", 0})->logEndOffset() == Offset(1));
}

TEST_CASE("Partitions of the same topic are separate logs") {
    TempDir dir("mgr-many-partitions");
    const filesystem::path dataDir = dir.file("data");
    LogManager manager(dataDir, testConfig());

    for (int p = 0; p < 4; ++p) manager.createPartition(TopicPartition{"orders", p});
    manager.createPartition(TopicPartition{"payments", 0});

    CHECK(manager.partitionCount() == 5);

    // Ordering is guaranteed within a partition and nowhere else, which is only
    // true because each is a physically separate log.
    Log* a = manager.get(TopicPartition{"orders", 0});
    Log* b = manager.get(TopicPartition{"orders", 1});
    REQUIRE(a != nullptr);
    REQUIRE(b != nullptr);
    CHECK(a != b);

    auto bytes = makeUnstampedBatch(1000, 48);
    a->append(bytes);
    CHECK(a->logEndOffset() == Offset(1));
    CHECK(b->logEndOffset() == Offset(0));   // untouched

    CHECK(filesystem::is_directory(dataDir / "orders-3"));
    CHECK(filesystem::is_directory(dataDir / "payments-0"));
}

TEST_CASE("Creating a partition twice is refused") {
    TempDir dir("mgr-create-twice");
    LogManager manager(dir.file("data"), testConfig());
    manager.createPartition(TopicPartition{"orders", 0});

    // The controller decides where partitions live, so asking twice means its
    // view and the broker's have diverged. Quietly returning the existing log
    // would hide that.
    CHECK_THROWS_AS(manager.createPartition(TopicPartition{"orders", 0}), Error);
    CHECK(manager.partitionCount() == 1);
}

TEST_CASE("A partition takes the manager's defaults unless given its own config") {
    TempDir dir("mgr-create-config");
    LogConfig defaults = testConfig();
    defaults.retention.retentionMs = 111'000;
    LogManager manager(dir.file("data"), defaults);

    Log& fromDefaults = manager.createPartition(TopicPartition{"orders", 0});
    CHECK(fromDefaults.config().retention.retentionMs == 111'000);

    LogConfig explicitConfig = testConfig();
    explicitConfig.retention.retentionMs = 222'000;
    Log& fromExplicit = manager.createPartition(TopicPartition{"orders", 1}, explicitConfig);
    CHECK(fromExplicit.config().retention.retentionMs == 222'000);

    // And each partition's own config is what lands in its partition.meta.
    CHECK(readPartitionMeta(dir.file("data") / "orders-0", TopicPartition{"orders", 0})
              .config.retention.retentionMs == 111'000);
    CHECK(readPartitionMeta(dir.file("data") / "orders-1", TopicPartition{"orders", 1})
              .config.retention.retentionMs == 222'000);
}

TEST_CASE("A topic name that cannot be a directory name is refused") {
    TempDir dir("mgr-bad-names");
    const filesystem::path dataDir = dir.file("data");
    LogManager manager(dataDir, testConfig());

    // This is the boundary where a string chosen by a client becomes a
    // filesystem path. '/' would place the partition outside the data directory
    // entirely and ".." would climb out of it — a path-traversal bug waiting for
    // the first CreateTopic request M4 serves.
    CHECK_THROWS_AS(manager.createPartition(TopicPartition{"../escape", 0}), Error);
    CHECK_THROWS_AS(manager.createPartition(TopicPartition{"a/b", 0}), Error);
    CHECK_THROWS_AS(manager.createPartition(TopicPartition{"..", 0}), Error);
    CHECK_THROWS_AS(manager.createPartition(TopicPartition{".", 0}), Error);
    CHECK_THROWS_AS(manager.createPartition(TopicPartition{"", 0}), Error);
    CHECK_THROWS_AS(manager.createPartition(TopicPartition{"has space", 0}), Error);
    // An embedded NUL needs an explicit length. Written as a bare literal,
    // TopicPartition{"orders\0hidden", 0} goes through std::string's const char*
    // constructor, which stops at the NUL — so the topic would just be "orders",
    // which is perfectly valid, and the test would pass while checking nothing.
    CHECK_THROWS_AS(manager.createPartition(TopicPartition{string("orders\0hidden", 13), 0}),
                    Error);
    CHECK_THROWS_AS(manager.createPartition(TopicPartition{"orders", -1}), Error);

    CHECK(manager.partitionCount() == 0);

    // Nothing was created anywhere, including outside the data directory.
    CHECK_FALSE(filesystem::exists(dir.file("escape-0")));
    CHECK(filesystem::is_empty(dataDir));
}

TEST_CASE("The permitted topic name characters are accepted") {
    TempDir dir("mgr-good-names");
    LogManager manager(dir.file("data"), testConfig());

    CHECK_NOTHROW(manager.createPartition(TopicPartition{"orders", 0}));
    CHECK_NOTHROW(manager.createPartition(TopicPartition{"my-topic", 0}));
    CHECK_NOTHROW(manager.createPartition(TopicPartition{"my_topic", 0}));
    CHECK_NOTHROW(manager.createPartition(TopicPartition{"my.topic", 0}));
    CHECK_NOTHROW(manager.createPartition(TopicPartition{"Topic123", 0}));
    CHECK_NOTHROW(manager.createPartition(TopicPartition{"__offsets", 12}));
    CHECK(manager.partitionCount() == 6);
}

// ===========================================================================
// LogManager: directory names
// ===========================================================================

TEST_CASE("A partition directory name round-trips") {
    const auto tp = topicPartitionFromDirName("orders-3");
    CHECK(tp.topic == "orders");
    CHECK(tp.partition == 3);
    CHECK(tp.toString() == "orders-3");

    CHECK(topicPartitionFromDirName("__offsets-12").topic == "__offsets");
    CHECK(topicPartitionFromDirName("__offsets-12").partition == 12);
    CHECK(topicPartitionFromDirName("a-0").partition == 0);
}

TEST_CASE("A topic name containing dashes splits at the last one") {
    // The ambiguity types.hpp warns about: "my-topic-5" could split three ways.
    const auto tp = topicPartitionFromDirName("my-topic-5");
    CHECK(tp.topic == "my-topic");
    CHECK(tp.partition == 5);
    CHECK(tp.toString() == "my-topic-5");

    const auto deeper = topicPartitionFromDirName("a-b-c-d-11");
    CHECK(deeper.topic == "a-b-c-d");
    CHECK(deeper.partition == 11);
}

TEST_CASE("A non-canonical directory name is refused") {
    // "orders-03" parses as partition 3, but re-encoding gives "orders-3" — so
    // the same partition would have two valid directory names, and whichever the
    // scan met first would win while the other sat holding unreadable records.
    CHECK_THROWS_AS(topicPartitionFromDirName("orders-03"), CorruptData);
    CHECK_THROWS_AS(topicPartitionFromDirName("orders-000"), CorruptData);
    CHECK_NOTHROW(topicPartitionFromDirName("orders-0"));
}

TEST_CASE("A directory name that is not a partition is refused") {
    CHECK_THROWS_AS(topicPartitionFromDirName("orders"), CorruptData);      // no dash
    CHECK_THROWS_AS(topicPartitionFromDirName("-3"), CorruptData);          // no topic
    CHECK_THROWS_AS(topicPartitionFromDirName("orders-"), CorruptData);     // no number
    CHECK_THROWS_AS(topicPartitionFromDirName("orders-abc"), CorruptData);  // not a number
    CHECK_THROWS_AS(topicPartitionFromDirName("orders-99999999999"), CorruptData);  // > int32
    CHECK_THROWS_AS(topicPartitionFromDirName(""), CorruptData);
}

TEST_CASE("A negative partition number cannot be expressed as a directory name") {
    // "orders--1" looks like partition -1, and is not. Splitting at the last dash
    // makes it topic "orders-" with partition 1 — and since a hyphen is a legal
    // topic character, that is a genuine canonical name which round-trips.
    const auto tp = topicPartitionFromDirName("orders--1");
    CHECK(tp.topic == "orders-");
    CHECK(tp.partition == 1);
    CHECK(tp.toString() == "orders--1");

    // So a negative partition has no directory name at all: toString() would
    // produce something that parses back as a different, positive partition. The
    // two validators cover each other — createPartition refuses negatives before
    // one can ever reach the disk, and this parser could not represent one if it
    // did.
    CHECK(TopicPartition{"orders", -1}.toString() == "orders--1");
    CHECK(topicPartitionFromDirName(TopicPartition{"orders", -1}.toString()).partition == 1);
}

// ===========================================================================
// LogManager: startup scan
// ===========================================================================

TEST_CASE("Scanning an empty data directory finds nothing") {
    TempDir dir("mgr-load-empty");
    LogManager manager(dir.file("data"), testConfig());

    manager.loadAll();
    CHECK(manager.partitionCount() == 0);
}

TEST_CASE("Every partition on disk comes back with its data") {
    TempDir dir("mgr-load-all");
    const filesystem::path dataDir = dir.file("data");

    {
        LogManager manager(dataDir, testConfig());
        for (int p = 0; p < 3; ++p) {
            Log& log = manager.createPartition(TopicPartition{"orders", p});
            for (int i = 0; i <= p; ++i) {   // partition p gets p+1 records
                auto bytes = makeUnstampedBatch(1000 + i, 48);
                log.append(bytes);
            }
        }
        manager.createPartition(TopicPartition{"payments", 7});
    }

    LogManager reopened(dataDir, testConfig());
    reopened.loadAll();

    CHECK(reopened.partitionCount() == 4);
    for (int p = 0; p < 3; ++p) {
        Log* log = reopened.get(TopicPartition{"orders", p});
        REQUIRE(log != nullptr);
        CHECK(log->logEndOffset() == Offset(p + 1));
        CHECK(log->read(Offset(0), kBigFetch).ok());
    }
    CHECK(reopened.get(TopicPartition{"payments", 7})->logEndOffset() == Offset(0));
    CHECK(reopened.get(TopicPartition{"orders", 9}) == nullptr);
}

TEST_CASE("Each partition comes back with its own config, not the defaults") {
    TempDir dir("mgr-load-configs");
    const filesystem::path dataDir = dir.file("data");

    {
        LogManager manager(dataDir, testConfig());
        LogConfig tight = testConfig();
        tight.retention.retentionMs = 60'000;
        manager.createPartition(TopicPartition{"orders", 0}, tight);

        LogConfig loose = testConfig();
        loose.retention.retentionMs = 900'000;
        manager.createPartition(TopicPartition{"orders", 1}, loose);
    }

    // Reopened by a broker whose defaults match neither.
    LogConfig otherDefaults = testConfig();
    otherDefaults.retention.retentionMs = 1;
    LogManager reopened(dataDir, otherDefaults);
    reopened.loadAll();

    CHECK(reopened.get(TopicPartition{"orders", 0})->config().retention.retentionMs == 60'000);
    CHECK(reopened.get(TopicPartition{"orders", 1})->config().retention.retentionMs == 900'000);
}

TEST_CASE("The scan steps over files and the reserved meta directory") {
    TempDir dir("mgr-load-other");
    const filesystem::path dataDir = dir.file("data");
    {
        LogManager manager(dataDir, testConfig());
        manager.createPartition(TopicPartition{"orders", 0});
    }

    // The controller's own state, and the debris a real data directory collects.
    filesystem::create_directories(dataDir / kMetaDirName);
    writeFile(dataDir / kMetaDirName / "cluster.meta", vector<uint8_t>{1, 2, 3});
    writeFile(dataDir / ".DS_Store", vector<uint8_t>{4});
    writeFile(dataDir / "notes.txt", vector<uint8_t>{5});

    LogManager manager(dataDir, testConfig());
    manager.loadAll();
    CHECK(manager.partitionCount() == 1);
}

TEST_CASE("A directory that should be a partition but is not aborts the scan") {
    TempDir dir("mgr-load-bad-dir");
    const filesystem::path dataDir = dir.file("data");
    {
        LogManager manager(dataDir, testConfig());
        manager.createPartition(TopicPartition{"orders", 0});
    }
    filesystem::create_directories(dataDir / "orders-not-a-number");

    // Skipping it would leave its records on disk, invisible, while consumers got
    // "not hosted here" forever with nothing to explain it. Loud is better.
    LogManager manager(dataDir, testConfig());
    CHECK_THROWS_AS(manager.loadAll(), CorruptData);
}

TEST_CASE("Scanning twice is refused") {
    TempDir dir("mgr-load-twice");
    const filesystem::path dataDir = dir.file("data");
    {
        LogManager manager(dataDir, testConfig());
        manager.createPartition(TopicPartition{"orders", 0});
    }

    LogManager manager(dataDir, testConfig());
    manager.loadAll();

    // A second scan would open partitions that are already open, putting two
    // writable handles on one active segment.
    CHECK_THROWS_AS(manager.loadAll(), Error);
    CHECK(manager.partitionCount() == 1);
}

TEST_CASE("A reopened partition can be written to and rolls as before") {
    TempDir dir("mgr-load-writable");
    const filesystem::path dataDir = dir.file("data");
    LogConfig config = testConfig();
    config.roll.maxSegmentBytes = 400;

    Offset end{0};
    {
        LogManager manager(dataDir, config);
        Log& log = manager.createPartition(TopicPartition{"orders", 0}, config);
        for (int i = 0; i < 20; ++i) {
            auto bytes = makeUnstampedBatch(1000 + i, 48);
            log.append(bytes);
        }
        end = log.logEndOffset();
    }

    LogManager manager(dataDir, config);
    manager.loadAll();
    Log* log = manager.get(TopicPartition{"orders", 0});
    REQUIRE(log != nullptr);
    REQUIRE(log->logEndOffset() == end);

    // A broker has to restart into service, not into read-only mode.
    const size_t segmentsBefore = log->segmentCount();
    for (int i = 0; i < 20; ++i) {
        auto bytes = makeUnstampedBatch(2000 + i, 48);
        log->append(bytes);
    }
    CHECK(log->logEndOffset() == end + 20);
    CHECK(log->segmentCount() > segmentsBefore);
}

// ===========================================================================
// LogManager: removing partitions
// ===========================================================================

TEST_CASE("Removing a partition unregisters it and renames its directory away") {
    TempDir dir("mgr-remove");
    const filesystem::path dataDir = dir.file("data");
    LogManager manager(dataDir, testConfig());
    manager.createPartition(TopicPartition{"orders", 0});
    manager.createPartition(TopicPartition{"orders", 1});

    manager.removePartition(TopicPartition{"orders", 0}, 1000);

    CHECK(manager.partitionCount() == 1);
    CHECK(manager.get(TopicPartition{"orders", 0}) == nullptr);
    CHECK(manager.get(TopicPartition{"orders", 1}) != nullptr);

    // Renamed, not yet deleted: the descriptors are still open.
    CHECK_FALSE(filesystem::exists(dataDir / "orders-0"));
    CHECK(deletedDirCount(dataDir) == 1);
    CHECK(manager.removedPartitionCount() == 1);
}

TEST_CASE("A read in flight when a partition is removed still completes") {
    TempDir dir("mgr-remove-inflight");
    const filesystem::path dataDir = dir.file("data");
    LogManager manager(dataDir, testConfig());
    Log& log = manager.createPartition(TopicPartition{"orders", 0});
    for (int i = 0; i < 5; ++i) {
        auto bytes = makeUnstampedBatch(1000 + i, 48);
        log.append(bytes);
    }

    // Resolved but not yet sent — the network layer would hand this to sendfile
    // on an I/O thread some time later.
    const auto inflight = log.read(Offset(0), kBigFetch);
    REQUIRE(inflight.ok());
    REQUIRE_FALSE(inflight.range.empty());

    manager.removePartition(TopicPartition{"orders", 0}, 1000);

    // Same reasoning as retention's graveyard, one level up: closing the
    // descriptor early would fail this read, or let its number be reused by
    // another file and send a consumer someone else's bytes.
    const auto bytes = pullRange(inflight.range);
    CHECK(RecordBatch::verifyCrc(bytes));
    CHECK(RecordBatch::parseHeader(bytes).baseOffset == Offset(0));
}

TEST_CASE("A removed partition's files go once its delay elapses") {
    TempDir dir("mgr-remove-sweep");
    const filesystem::path dataDir = dir.file("data");
    LogConfig config = testConfig();
    config.retention.segmentDeleteDelayMs = 60'000;
    LogManager manager(dataDir, config);
    manager.createPartition(TopicPartition{"orders", 0});

    manager.removePartition(TopicPartition{"orders", 0}, 1000);
    REQUIRE(manager.removedPartitionCount() == 1);

    manager.sweepDeletedPartitions(1000 + 59'000);
    CHECK(manager.removedPartitionCount() == 1);
    CHECK(deletedDirCount(dataDir) == 1);

    manager.sweepDeletedPartitions(1000 + 60'000);
    CHECK(manager.removedPartitionCount() == 0);
    CHECK(deletedDirCount(dataDir) == 0);
    CHECK(filesystem::is_empty(dataDir));
}

TEST_CASE("Removing a partition this broker does not host is refused") {
    TempDir dir("mgr-remove-missing");
    LogManager manager(dir.file("data"), testConfig());

    CHECK_THROWS_AS(manager.removePartition(TopicPartition{"orders", 0}, 1000), Error);
}

TEST_CASE("A partition can be recreated after being removed") {
    TempDir dir("mgr-remove-recreate");
    const filesystem::path dataDir = dir.file("data");
    LogManager manager(dataDir, testConfig());

    Log& first = manager.createPartition(TopicPartition{"orders", 0});
    auto bytes = makeUnstampedBatch(1000, 48);
    first.append(bytes);
    REQUIRE(first.logEndOffset() == Offset(1));

    manager.removePartition(TopicPartition{"orders", 0}, 1000);

    // The rename freed the name, so this does not collide with the corpse.
    Log& second = manager.createPartition(TopicPartition{"orders", 0});
    CHECK(second.logEndOffset() == Offset(0));
    CHECK(manager.partitionCount() == 1);
    CHECK(filesystem::is_directory(dataDir / "orders-0"));
}

TEST_CASE("Deleting, recreating and deleting again does not collide") {
    TempDir dir("mgr-remove-twice");
    const filesystem::path dataDir = dir.file("data");
    LogManager manager(dataDir, testConfig());

    manager.createPartition(TopicPartition{"orders", 0});
    manager.removePartition(TopicPartition{"orders", 0}, 1000);
    manager.createPartition(TopicPartition{"orders", 0});

    // The timestamp in the renamed name is what keeps the second corpse from
    // landing on the first.
    CHECK_NOTHROW(manager.removePartition(TopicPartition{"orders", 0}, 2000));
    CHECK(manager.removedPartitionCount() == 2);
    CHECK(deletedDirCount(dataDir) == 2);
}

TEST_CASE("An interrupted deletion is finished at startup") {
    TempDir dir("mgr-remove-interrupted");
    const filesystem::path dataDir = dir.file("data");
    {
        LogManager manager(dataDir, testConfig());
        manager.createPartition(TopicPartition{"orders", 0});
        manager.createPartition(TopicPartition{"orders", 1});
        manager.removePartition(TopicPartition{"orders", 0}, 1000);
        // Destroyed without sweeping — the renamed directory is still on disk.
    }
    REQUIRE(deletedDirCount(dataDir) == 1);

    LogManager reopened(dataDir, testConfig());
    reopened.loadAll();

    // A restart means there are no in-flight reads to protect, so the delay
    // serves no purpose and the deletion is completed rather than deferred again.
    CHECK(reopened.partitionCount() == 1);
    CHECK(reopened.get(TopicPartition{"orders", 1}) != nullptr);
    CHECK(reopened.get(TopicPartition{"orders", 0}) == nullptr);
    CHECK(deletedDirCount(dataDir) == 0);
    CHECK(partitionDirCount(dataDir) == 1);
}

TEST_CASE("A renamed directory is never parsed as a partition") {
    TempDir dir("mgr-remove-not-parsed");
    const filesystem::path dataDir = dir.file("data");
    filesystem::create_directories(dataDir);

    // Without the suffix skip, this name reaches topicPartitionFromDirName, which
    // would refuse it and abort the whole scan — turning one interrupted deletion
    // into a broker that will not start.
    filesystem::create_directories(dataDir / ("orders-0.1000" + string(kDeletedSuffix)));
    CHECK_THROWS_AS(topicPartitionFromDirName("orders-0.1000.delete"), CorruptData);

    LogManager manager(dataDir, testConfig());
    CHECK_NOTHROW(manager.loadAll());
    CHECK(manager.partitionCount() == 0);
}

// ===========================================================================
// LogManager: the maintenance sweep
// ===========================================================================

TEST_CASE("The sweep rolls an idle partition so retention has something to delete") {
    TempDir dir("mgr-sweep-idle");
    LogConfig config = testConfig();
    config.roll.maxSegmentBytes = 1ull << 30;   // size will never trigger
    config.roll.maxSegmentAgeMs = 1'000;
    config.retention.retentionMs = 1'000;

    LogManager manager(dir.file("data"), config);
    Log& log = manager.createPartition(TopicPartition{"orders", 0});

    // Times here must be on the WALL clock, not synthetic. shouldRoll compares
    // the nowMs it is handed against firstAppendMs_, which append captures from
    // wallClockMillis() itself — so a caller passing made-up numbers gets an
    // enormous negative difference and no roll ever fires. The two are implicitly
    // required to be on the same clock; see the note in the review.
    const int64_t now = wallClockMillis();
    auto bytes = makeUnstampedBatch(now, 48);
    log.append(bytes);
    REQUIRE(log.segmentCount() == 1);

    // This is the trap DESIGN.md decision 11 names. With no further writes,
    // append is never called again, so nothing re-evaluates age — the active
    // segment never seals, and retention only ever deletes sealed segments.
    manager.runMaintenance(now + 5'000);

    // Rolled, then the sealed segment aged out and was collected.
    CHECK(log.logStartOffset() > Offset(0));
    CHECK(log.logEndOffset() == Offset(1));
    CHECK(log.segmentCount() == 1);   // just the fresh active one
}

TEST_CASE("The sweep applies retention across every partition") {
    TempDir dir("mgr-sweep-retention");
    LogConfig config = testConfig();
    config.roll.maxSegmentBytes  = 400;
    config.retention.retentionMs = 10'000;

    LogManager manager(dir.file("data"), config);
    for (int p = 0; p < 3; ++p) {
        Log& log = manager.createPartition(TopicPartition{"orders", p}, config);
        for (int i = 0; i < 40; ++i) {
            auto bytes = makeUnstampedBatch(100'000 + i * 1'000, 48);
            log.append(bytes);
        }
    }

    manager.runMaintenance(100'000 + 39 * 1'000);

    // Every partition, not just the first — a sweep that stopped early would
    // leave later partitions growing forever.
    for (int p = 0; p < 3; ++p) {
        Log* log = manager.get(TopicPartition{"orders", p});
        REQUIRE(log != nullptr);
        CHECK(log->logStartOffset() > Offset(0));
        CHECK(log->logEndOffset() == Offset(40));
    }
}

TEST_CASE("The sweep drains the segment graveyard") {
    TempDir dir("mgr-sweep-graveyard");
    LogConfig config = testConfig();
    config.roll.maxSegmentBytes            = 400;
    // Small enough that the FIRST sweep ages out every sealed segment. With a
    // larger window, advancing the clock to reach the delete delay would age out
    // more segments on the way and bury them fresh — so the graveyard would never
    // be observed empty, and the test would be measuring the wrong thing.
    config.retention.retentionMs           = 1;
    config.retention.segmentDeleteDelayMs  = 60'000;

    LogManager manager(dir.file("data"), config);
    Log& log = manager.createPartition(TopicPartition{"orders", 0}, config);
    for (int i = 0; i < 40; ++i) {
        auto bytes = makeUnstampedBatch(100'000 + i * 1'000, 48);
        log.append(bytes);
    }

    const int64_t now = 200'000;
    manager.runMaintenance(now);
    const size_t buried = log.graveyardSize();
    REQUIRE(buried > 0);           // deleted, descriptors still open
    REQUIRE(log.logStartOffset() > Offset(0));

    // Nothing is freed until the delay elapses — that window is what keeps an
    // already-issued FileRange sendable.
    manager.runMaintenance(now + 59'000);
    CHECK(log.graveyardSize() == buried);

    manager.runMaintenance(now + 60'000);
    CHECK(log.graveyardSize() == 0);
}

TEST_CASE("The sweep drains removed partitions") {
    TempDir dir("mgr-sweep-removed");
    const filesystem::path dataDir = dir.file("data");
    LogConfig config = testConfig();
    config.retention.segmentDeleteDelayMs = 60'000;

    LogManager manager(dataDir, config);
    manager.createPartition(TopicPartition{"orders", 0});
    manager.createPartition(TopicPartition{"orders", 1});
    manager.removePartition(TopicPartition{"orders", 0}, 1'000);
    REQUIRE(manager.removedPartitionCount() == 1);

    manager.runMaintenance(1'000 + 59'000);
    CHECK(manager.removedPartitionCount() == 1);

    manager.runMaintenance(1'000 + 60'000);
    CHECK(manager.removedPartitionCount() == 0);
    CHECK(manager.partitionCount() == 1);
    CHECK(deletedDirCount(dataDir) == 0);
}

TEST_CASE("A sweep over no partitions is harmless") {
    TempDir dir("mgr-sweep-empty");
    LogManager manager(dir.file("data"), testConfig());

    CHECK_NOTHROW(manager.runMaintenance(1'000));
    CHECK_NOTHROW(manager.runMaintenance(1'000'000'000));
    CHECK(manager.partitionCount() == 0);
}

TEST_CASE("Repeated sweeps on a quiet broker change nothing") {
    TempDir dir("mgr-sweep-repeat");
    const filesystem::path dataDir = dir.file("data");
    LogConfig config = testConfig();
    config.roll.maxSegmentAgeMs  = 1'000;
    config.retention.retentionMs = 1'000'000'000;

    LogManager manager(dataDir, config);
    manager.createPartition(TopicPartition{"orders", 0}, config);

    // A partition that has never been written to must not accumulate segments,
    // however often it is swept — otherwise a quiet topic grows a file per pass.
    for (int i = 1; i <= 10; ++i) manager.runMaintenance(1'000'000 * i);

    Log* log = manager.get(TopicPartition{"orders", 0});
    REQUIRE(log != nullptr);
    CHECK(log->segmentCount() == 1);
    CHECK(log->logEndOffset() == Offset(0));
}

TEST_CASE("A sweep running against a live appender is safe") {
    TempDir dir("mgr-sweep-concurrent");
    LogConfig config = testConfig();
    config.roll.maxSegmentBytes            = 400;
    config.roll.maxSegmentAgeMs            = 1;
    config.retention.retentionMs           = 5'000;
    config.retention.segmentDeleteDelayMs  = 0;

    LogManager manager(dir.file("data"), config);
    Log& log = manager.createPartition(TopicPartition{"orders", 0}, config);

    constexpr int kBatches = 1500;
    atomic<bool> done{false};
    atomic<int>  sweeps{0};

    // Both threads now roll: the appender through append, this one through the
    // sweep. Before step 13 they raced — shouldRoll was read outside the lock, so
    // both could pass it and both try to create the same segment.
    thread sweeper([&] {
        while (!done.load(memory_order_acquire) || sweeps.load(memory_order_relaxed) == 0) {
            manager.runMaintenance(100'000 + sweeps.load(memory_order_relaxed) * 10);
            sweeps.fetch_add(1, memory_order_relaxed);
        }
    });

    for (int i = 0; i < kBatches; ++i) {
        auto bytes = makeUnstampedBatch(100'000 + i, 48);
        log.append(bytes);
    }
    done.store(true, memory_order_release);
    sweeper.join();

    CHECK(sweeps.load() > 0);
    CHECK(log.logEndOffset() == Offset(kBatches));

    // Whatever the sweep deleted, what remains must still be a contiguous,
    // readable run ending at the log end.
    for (int64_t offset = log.logStartOffset().value(); offset < kBatches; ++offset)
        CHECK(log.read(Offset(offset), kBigFetch).ok());
}

// ===========================================================================
// LogManager: the maintenance thread
// ===========================================================================

namespace {

// Spins until `predicate` holds or the deadline passes. Used instead of sleeping
// for a fixed time: a fixed sleep is either too short on a loaded machine (flaky)
// or too long everywhere else (slow).
template <typename Predicate>
bool waitFor(Predicate predicate, chrono::milliseconds limit = chrono::milliseconds(5000)) {
    const auto deadline = chrono::steady_clock::now() + limit;
    while (chrono::steady_clock::now() < deadline) {
        if (predicate()) return true;
        this_thread::sleep_for(chrono::milliseconds(1));
    }
    return predicate();
}

}  // namespace

TEST_CASE("The maintenance thread sweeps on its interval") {
    TempDir dir("mgr-thread-sweeps");
    LogManager manager(dir.file("data"), testConfig());
    manager.createPartition(TopicPartition{"orders", 0});

    CHECK(manager.sweepCount() == 0);

    manager.startMaintenance(1);
    CHECK(waitFor([&] { return manager.sweepCount() >= 3; }));
    manager.stopMaintenance();

    CHECK(manager.sweepCount() >= 3);
    CHECK(manager.sweepFailures() == 0);
}

TEST_CASE("Stopping is immediate rather than waiting out the interval") {
    TempDir dir("mgr-thread-prompt-stop");
    LogManager manager(dir.file("data"), testConfig());

    // A ten-minute interval. With a plain sleep, this stop would take ten
    // minutes — long enough that whatever supervises the broker SIGKILLs it, and
    // every clean shutdown becomes a crash-recovery path on the next boot.
    manager.startMaintenance(600'000);

    const auto before = chrono::steady_clock::now();
    manager.stopMaintenance();
    const auto elapsed = chrono::steady_clock::now() - before;

    CHECK(elapsed < chrono::seconds(2));
}

TEST_CASE("Stopping twice, and stopping without starting, are both harmless") {
    TempDir dir("mgr-thread-stop-idempotent");
    LogManager manager(dir.file("data"), testConfig());

    CHECK_NOTHROW(manager.stopMaintenance());   // never started

    manager.startMaintenance(1);
    CHECK_NOTHROW(manager.stopMaintenance());
    CHECK_NOTHROW(manager.stopMaintenance());
}

TEST_CASE("Starting twice is refused, and a non-positive interval is refused") {
    TempDir dir("mgr-thread-start-twice");
    LogManager manager(dir.file("data"), testConfig());

    CHECK_THROWS_AS(manager.startMaintenance(0), Error);
    CHECK_THROWS_AS(manager.startMaintenance(-1), Error);

    manager.startMaintenance(50);
    CHECK_THROWS_AS(manager.startMaintenance(50), Error);
    manager.stopMaintenance();

    // And it can be started again afterwards.
    CHECK_NOTHROW(manager.startMaintenance(50));
    manager.stopMaintenance();
}

TEST_CASE("The destructor stops the thread") {
    TempDir dir("mgr-thread-destructor");
    {
        LogManager manager(dir.file("data"), testConfig());
        manager.createPartition(TopicPartition{"orders", 0});
        manager.startMaintenance(1);
        REQUIRE(waitFor([&] { return manager.sweepCount() >= 1; }));
        // No stopMaintenance() — the destructor has to do it. A thread that
        // outlived this object would be sweeping partitions that no longer exist.
    }
    // Reaching here without a crash or a std::terminate from an unjoined thread
    // is the assertion.
    CHECK(true);
}

TEST_CASE("The thread keeps retention working while a partition is written to") {
    TempDir dir("mgr-thread-live");
    LogConfig config = testConfig();
    config.roll.maxSegmentBytes           = 400;
    config.retention.retentionMs          = 1;
    config.retention.segmentDeleteDelayMs = 0;

    LogManager manager(dir.file("data"), config);
    Log& log = manager.createPartition(TopicPartition{"orders", 0}, config);

    manager.startMaintenance(1);

    constexpr int kBatches = 1500;
    for (int i = 0; i < kBatches; ++i) {
        auto bytes = makeUnstampedBatch(100'000 + i, 48);
        log.append(bytes);
    }

    // Retention on a live partition: the sweep must have collected something
    // without ever tripping over the appender.
    CHECK(waitFor([&] { return log.logStartOffset() > Offset(0); }));
    manager.stopMaintenance();

    CHECK(manager.sweepFailures() == 0);
    CHECK(log.logEndOffset() == Offset(kBatches));

    // Whatever was collected, what remains is a contiguous readable run.
    for (int64_t offset = log.logStartOffset().value(); offset < kBatches; ++offset)
        CHECK(log.read(Offset(offset), kBigFetch).ok());
}

TEST_CASE("Partitions can be created and removed while the thread runs") {
    TempDir dir("mgr-thread-churn");
    LogConfig config = testConfig();
    config.retention.segmentDeleteDelayMs = 0;

    LogManager manager(dir.file("data"), config);
    manager.startMaintenance(1);

    // The sweep holds a shared lock over the registry; these take it exclusively.
    for (int i = 0; i < 40; ++i) {
        manager.createPartition(TopicPartition{"orders", i});
        if (i % 2 == 0) manager.removePartition(TopicPartition{"orders", i}, wallClockMillis());
    }

    CHECK(waitFor([&] { return manager.sweepCount() >= 2; }));
    manager.stopMaintenance();

    CHECK(manager.partitionCount() == 20);
    CHECK(manager.sweepFailures() == 0);
}

// ===========================================================================
// M3 end to end: the thread alone, with no manual calls
// ===========================================================================

namespace {

uint64_t bytesUnder(const filesystem::path& root) {
    uint64_t total = 0;
    for (const auto& entry : filesystem::recursive_directory_iterator(root))
        if (entry.is_regular_file()) total += filesystem::file_size(entry.path());
    return total;
}

}  // namespace

TEST_CASE("A partition is rolled, aged out and freed with no manual intervention") {
    TempDir dir("m3-end-to-end");
    const filesystem::path dataDir = dir.file("data");

    LogConfig config = testConfig();
    config.roll.maxSegmentBytes           = 400;   // roll often
    config.roll.maxSegmentAgeMs           = 1;     // and on age, so idle rolls too
    config.retention.retentionMs          = 1'000;
    config.retention.segmentDeleteDelayMs = 0;     // free as soon as swept

    LogManager manager(dataDir, config);
    Log& log = manager.createPartition(TopicPartition{"orders", 0}, config);

    // Records timestamped ten seconds ago, so they are already past a one-second
    // window. Real wall-clock values, because rolling compares against a time
    // append captured from the same clock.
    const int64_t past = wallClockMillis() - 10'000;
    for (int i = 0; i < 40; ++i) {
        auto bytes = makeUnstampedBatch(past + i, 48);
        log.append(bytes);
    }
    const uint64_t bytesBefore = bytesUnder(dataDir);
    REQUIRE(log.logEndOffset() == Offset(40));
    REQUIRE(log.logStartOffset() == Offset(0));

    // From here on nothing is called by hand. Everything below is the thread.
    manager.startMaintenance(1);

    // Rolling, then retention, then the graveyard drain — three steps the sweep
    // does in that order, and the last offset only becomes unreachable once all
    // three have happened.
    CHECK(waitFor([&] { return log.logStartOffset() == log.logEndOffset(); }));
    CHECK(waitFor([&] { return log.graveyardSize() == 0; }));

    manager.stopMaintenance();

    CHECK(manager.sweepFailures() == 0);
    CHECK(log.logEndOffset() == Offset(40));      // offsets are never reused
    CHECK(log.logStartOffset() == Offset(40));    // but nothing is left to read
    CHECK(log.segmentCount() == 1);               // just a fresh active segment
    CHECK(log.totalSizeBytes() == 0);

    // The disk actually came back — the graveyard drained rather than just the
    // map being emptied.
    CHECK(bytesUnder(dataDir) < bytesBefore);

    // And what a consumer sees: the checklist's last line, reached by the sweep
    // rather than by a test calling applyRetention.
    CHECK(log.read(Offset(0), kBigFetch).error == ReadError::BelowLogStart);
    CHECK(log.read(Offset(39), kBigFetch).error == ReadError::BelowLogStart);
    CHECK(log.read(Offset(40), kBigFetch).ok());
    CHECK(log.read(Offset(40), kBigFetch).range.empty());
    CHECK(log.read(Offset(41), kBigFetch).error == ReadError::AboveLogEnd);
}

TEST_CASE("A partition kept inside its window survives the sweep intact") {
    TempDir dir("m3-end-to-end-kept");
    LogConfig config = testConfig();
    config.roll.maxSegmentBytes  = 400;
    config.roll.maxSegmentAgeMs  = 1;
    config.retention.retentionMs = 1'000'000;   // nothing is old enough

    LogManager manager(dir.file("data"), config);
    Log& log = manager.createPartition(TopicPartition{"orders", 0}, config);

    const int64_t now = wallClockMillis();
    for (int i = 0; i < 40; ++i) {
        auto bytes = makeUnstampedBatch(now + i, 48);
        log.append(bytes);
    }

    manager.startMaintenance(1);
    REQUIRE(waitFor([&] { return manager.sweepCount() >= 5; }));
    manager.stopMaintenance();

    // A sweep that deleted data inside its retention window would be the worst
    // possible bug in this milestone, so it gets its own test rather than being
    // implied by the aggressive one above.
    CHECK(log.logStartOffset() == Offset(0));
    CHECK(log.logEndOffset() == Offset(40));
    for (int64_t offset = 0; offset < 40; ++offset)
        CHECK(log.read(Offset(offset), kBigFetch).ok());
}

TEST_CASE("A whole broker restarts into the state the sweep left it in") {
    TempDir dir("m3-end-to-end-restart");
    const filesystem::path dataDir = dir.file("data");

    LogConfig config = testConfig();
    config.roll.maxSegmentBytes           = 400;
    config.roll.maxSegmentAgeMs           = 1;
    config.retention.retentionMs          = 1'000;
    config.retention.segmentDeleteDelayMs = 0;

    Offset start{0};
    Offset end{0};
    {
        LogManager manager(dataDir, config);
        for (int p = 0; p < 3; ++p) {
            Log& log = manager.createPartition(TopicPartition{"orders", p}, config);
            const int64_t past = wallClockMillis() - 10'000;
            for (int i = 0; i < 30; ++i) {
                auto bytes = makeUnstampedBatch(past + i, 48);
                log.append(bytes);
            }
        }
        manager.startMaintenance(1);
        Log* first = manager.get(TopicPartition{"orders", 0});
        REQUIRE(waitFor([&] { return first->logStartOffset() > Offset(0); }));
        manager.stopMaintenance();

        start = first->logStartOffset();
        end   = first->logEndOffset();
    }

    // Everything the sweep did was to the files, so a fresh manager scanning the
    // same directory must agree — including a log whose earliest segment is no
    // longer offset zero, and each partition's own config coming back from its
    // partition.meta.
    LogManager reopened(dataDir, testConfig());
    reopened.loadAll();

    CHECK(reopened.partitionCount() == 3);
    Log* first = reopened.get(TopicPartition{"orders", 0});
    REQUIRE(first != nullptr);
    CHECK(first->logStartOffset() == start);
    CHECK(first->logEndOffset() == end);
    CHECK(first->config().retention.retentionMs == 1'000);
    CHECK(first->read(Offset(0), kBigFetch).error == ReadError::BelowLogStart);
}
