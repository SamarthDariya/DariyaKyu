#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include <limits>
#include <map>
#include <set>
#include <string>

#include "common/buffer.hpp"
#include "group/commit_record.hpp"
#include "group/offsets_topic.hpp"
#include "test_support.hpp"

using namespace std;
using namespace dariyakyu;
using namespace dariyakyu::group;
using namespace dariyakyu::test;

// ===========================================================================
// The offsets topic
// ===========================================================================

TEST_CASE("A group always maps to the same coordinator partition") {
    // The mapping has to be stable across processes and restarts, or a group
    // would find its coordinator somewhere new each time and its offsets nowhere.
    for (const string group : {"payments", "orders-consumer", "a", ""}) {
        const PartitionId first = coordinatorPartition(group, 8);
        for (int i = 0; i < 10; ++i) CHECK(coordinatorPartition(group, 8) == first);
    }
}

TEST_CASE("Coordinator partitions are in range and never negative") {
    // std::hash returns an unsigned size_t. Casting to a signed PartitionId
    // before the modulus could give a negative remainder, and a negative
    // partition number is not a partition.
    for (int i = 0; i < 500; ++i) {
        const PartitionId partition = coordinatorPartition("group-" + to_string(i), 8);
        CHECK(partition >= 0);
        CHECK(partition < 8);
    }
}

TEST_CASE("Groups spread across the partitions") {
    set<PartitionId> used;
    for (int i = 0; i < 200; ++i) used.insert(coordinatorPartition("group-" + to_string(i), 8));

    // Not a property of the design so much as a check the hash is mixing at all:
    // if every group landed on one partition, one broker would coordinate
    // everything and the whole scheme would buy nothing.
    CHECK(used.size() == 8);
}

TEST_CASE("A partition count of zero or less is refused") {
    CHECK_THROWS_AS(coordinatorPartition("orders", 0), Error);
    CHECK_THROWS_AS(coordinatorPartition("orders", -1), Error);
}

TEST_CASE("The offsets topic is created once") {
    TempDir             dir("offsets-create");
    storage::LogManager logs(dir.file("data"), testConfig());

    CHECK(ensureOffsetsTopic(logs, 8) == 8);
    CHECK(logs.partitionCount() == 8);
    for (PartitionId p = 0; p < 8; ++p)
        CHECK(logs.get(TopicPartition{kOffsetsTopic, p}) != nullptr);

    // Idempotent: a broker restarting must not try to create it again.
    CHECK(ensureOffsetsTopic(logs, 8) == 8);
    CHECK(logs.partitionCount() == 8);
}

TEST_CASE("The offsets topic never deletes what it holds") {
    TempDir             dir("offsets-retention");
    storage::LogManager logs(dir.file("data"), testConfig());
    ensureOffsetsTopic(logs, 4);

    // Retention would drop an old commit, and a group's position would fall back
    // to whatever older commit happened to survive — moving a consumer
    // BACKWARDS, which is worse than it never having committed at all. The topic
    // is bounded by compaction at M6, not by deletion.
    storage::Log* log = logs.get(TopicPartition{kOffsetsTopic, 0});
    REQUIRE(log != nullptr);
    CHECK(log->config().retention.retentionMs == numeric_limits<int64_t>::max());
    CHECK_FALSE(log->config().retention.bytesLimited());
}

TEST_CASE("A different partition count on an existing topic is refused") {
    TempDir             dir("offsets-immutable");
    storage::LogManager logs(dir.file("data"), testConfig());
    ensureOffsetsTopic(logs, 8);

    // Both counts describe perfectly valid logs, so nothing downstream would
    // notice — it would just start answering FindCoordinator with different
    // partitions and reading offsets that are not there.
    CHECK_THROWS_AS(ensureOffsetsTopic(logs, 4), CorruptData);
    CHECK_THROWS_AS(ensureOffsetsTopic(logs, 16), CorruptData);
    CHECK(logs.partitionCount() == 8);
}

TEST_CASE("The partition count survives a restart without being stored") {
    TempDir dir("offsets-restart");
    const filesystem::path dataDir = dir.file("data");
    {
        storage::LogManager logs(dataDir, testConfig());
        ensureOffsetsTopic(logs, 8);
    }

    storage::LogManager reopened(dataDir, testConfig());
    reopened.loadAll();

    // Nothing wrote the number down. The count of partitions on disk IS N,
    // found by the startup scan that already runs.
    CHECK(ensureOffsetsTopic(reopened, 8) == 8);
    CHECK_THROWS_AS(ensureOffsetsTopic(reopened, 16), CorruptData);
}

// ===========================================================================
// Commit records
// ===========================================================================

namespace {

CommitKey sampleKey() { return CommitKey{"payments-consumer", "orders", 3}; }

CommitValue sampleValue() {
    CommitValue value;
    value.nextOffset        = Offset(510);
    value.metadata          = "deploy-42";
    value.commitTimestampMs = 1'700'000'000'000;
    return value;
}

}  // namespace

TEST_CASE("A commit key round-trips") {
    const auto decoded = decodeCommitKey(encodeCommitKey(sampleKey()));
    CHECK(decoded.group == "payments-consumer");
    CHECK(decoded.topic == "orders");
    CHECK(decoded.partition == 3);
    CHECK(decoded == sampleKey());
}

TEST_CASE("Two groups reading one partition have different keys") {
    // The key is what compaction collapses on, so it has to identify exactly one
    // consumer position. Sharing one would make two independent groups overwrite
    // each other's progress.
    const auto a = encodeCommitKey({"group-a", "orders", 0});
    const auto b = encodeCommitKey({"group-b", "orders", 0});
    CHECK(a != b);

    const auto p0 = encodeCommitKey({"group-a", "orders", 0});
    const auto p1 = encodeCommitKey({"group-a", "orders", 1});
    CHECK(p0 != p1);

    const auto t0 = encodeCommitKey({"group-a", "orders", 0});
    const auto t1 = encodeCommitKey({"group-a", "payments", 0});
    CHECK(t0 != t1);
}

TEST_CASE("The same key encodes identically every time") {
    // Compaction compares keys as BYTES, so two encodings of the same key that
    // differed by a byte would be two keys, and the older commit would survive
    // forever beside the newer one.
    for (int i = 0; i < 20; ++i) CHECK(encodeCommitKey(sampleKey()) == encodeCommitKey(sampleKey()));
}

TEST_CASE("A commit value round-trips") {
    const auto decoded = decodeCommitValue(encodeCommitValue(sampleValue()));
    CHECK(decoded.nextOffset == Offset(510));
    CHECK(decoded.metadata == "deploy-42");
    CHECK(decoded.commitTimestampMs == 1'700'000'000'000);
}

TEST_CASE("The stored offset is the next one to read") {
    // Handle 500 through 509, commit 510. Off by one here reprocesses or skips
    // exactly one message per restart — it survives every test written by hand,
    // and shows up in production.
    CommitValue value;
    value.nextOffset = Offset(510);

    const auto decoded = decodeCommitValue(encodeCommitValue(value));
    CHECK(decoded.nextOffset == Offset(510));
    CHECK(decoded.nextOffset != Offset(509));
}

TEST_CASE("An empty metadata and an absent one are the same thing") {
    CommitValue value = sampleValue();
    value.metadata    = "";
    CHECK(decodeCommitValue(encodeCommitValue(value)).metadata.empty());
}

TEST_CASE("A negative committed offset is refused") {
    BufferWriter out;
    out.writeInt16(1);
    out.writeInt64(-5);
    out.writeInt16(0);
    out.writeInt64(0);
    const auto bytes = out.take();

    // Not a position a consumer can have reached. Accepting it would make the
    // next fetch ask for something before the log began, and the group would
    // silently restart from the earliest record.
    CHECK_THROWS_AS(decodeCommitValue(bytes), CorruptData);
}

TEST_CASE("A version this build does not know is refused") {
    auto key = encodeCommitKey(sampleKey());
    key[1]   = 99;
    CHECK_THROWS_AS(decodeCommitKey(key), CorruptData);

    auto value = encodeCommitValue(sampleValue());
    value[1]   = 99;
    CHECK_THROWS_AS(decodeCommitValue(value), CorruptData);
}

TEST_CASE("Trailing bytes are refused") {
    auto key = encodeCommitKey(sampleKey());
    key.push_back(0);
    CHECK_THROWS_AS(decodeCommitKey(key), CorruptData);

    auto value = encodeCommitValue(sampleValue());
    value.push_back(0);
    CHECK_THROWS_AS(decodeCommitValue(value), CorruptData);
}

TEST_CASE("A truncated commit record is refused at every length") {
    const auto key = encodeCommitKey(sampleKey());
    for (size_t length = 0; length < key.size(); ++length)
        CHECK_THROWS_AS(decodeCommitKey(vector<uint8_t>(key.begin(),
                                                        key.begin() + static_cast<long>(length))),
                        CorruptData);

    const auto value = encodeCommitValue(sampleValue());
    for (size_t length = 0; length < value.size(); ++length)
        CHECK_THROWS_AS(
            decodeCommitValue(vector<uint8_t>(value.begin(),
                                              value.begin() + static_cast<long>(length))),
            CorruptData);
}
