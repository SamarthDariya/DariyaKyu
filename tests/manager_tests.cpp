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
