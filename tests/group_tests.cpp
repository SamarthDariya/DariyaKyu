#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include <limits>
#include <map>
#include <set>
#include <string>

#include "common/buffer.hpp"
#include "group/commit_record.hpp"
#include "group/group_coordinator.hpp"
#include "group/offset_store.hpp"
#include "group/offsets_topic.hpp"
#include "test_support.hpp"

using namespace std;
using namespace dariyakyu;
using namespace dariyakyu::group;
using namespace dariyakyu::protocol;
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

// ===========================================================================
// The offset store
// ===========================================================================

namespace {

// A broker's worth of storage with __offsets in it.
struct StoreFixture {
    TempDir             dir;
    storage::LogManager logs;
    OffsetStore         store;

    explicit StoreFixture(const string& name, int32_t partitions = 8)
        : dir(name), logs(dir.file("data"), testConfig()), store(logs, partitions) {
        ensureOffsetsTopic(logs, partitions);
    }
};

CommitValue valueAt(int64_t offset) {
    CommitValue value;
    value.nextOffset        = Offset(offset);
    value.commitTimestampMs = storage::wallClockMillis();
    return value;
}

}  // namespace

TEST_CASE("A committed offset reads back") {
    StoreFixture fixture("store-commit");

    CHECK_FALSE(fixture.store.fetch({"g", "orders", 0}).has_value());

    fixture.store.commit({"g", "orders", 0}, valueAt(510));

    const auto found = fixture.store.fetch({"g", "orders", 0});
    REQUIRE(found.has_value());
    CHECK(found->nextOffset == Offset(510));
    CHECK(fixture.store.size() == 1);
}

TEST_CASE("A later commit replaces an earlier one") {
    StoreFixture fixture("store-overwrite");

    fixture.store.commit({"g", "orders", 0}, valueAt(100));
    fixture.store.commit({"g", "orders", 0}, valueAt(200));

    CHECK(fixture.store.fetch({"g", "orders", 0})->nextOffset == Offset(200));
    CHECK(fixture.store.size() == 1);   // one position, not two
}

TEST_CASE("Groups, topics and partitions are independent positions") {
    StoreFixture fixture("store-independent");

    fixture.store.commit({"a", "orders", 0}, valueAt(10));
    fixture.store.commit({"b", "orders", 0}, valueAt(20));
    fixture.store.commit({"a", "payments", 0}, valueAt(30));
    fixture.store.commit({"a", "orders", 1}, valueAt(40));

    CHECK(fixture.store.size() == 4);
    CHECK(fixture.store.fetch({"a", "orders", 0})->nextOffset == Offset(10));
    CHECK(fixture.store.fetch({"b", "orders", 0})->nextOffset == Offset(20));
    CHECK(fixture.store.fetch({"a", "payments", 0})->nextOffset == Offset(30));
    CHECK(fixture.store.fetch({"a", "orders", 1})->nextOffset == Offset(40));
}

TEST_CASE("A commit lands in the group's coordinator partition and nowhere else") {
    StoreFixture fixture("store-routing");

    const PartitionId expected = coordinatorPartition("payments-consumer", 8);
    fixture.store.commit({"payments-consumer", "orders", 0}, valueAt(7));

    // Exactly one __offsets partition grew — the one hash(group) % N names.
    for (PartitionId p = 0; p < 8; ++p) {
        storage::Log* log = fixture.logs.get(TopicPartition{kOffsetsTopic, p});
        REQUIRE(log != nullptr);
        if (p == expected) CHECK(log->logEndOffset() > Offset(0));
        else CHECK(log->logEndOffset() == Offset(0));
    }
}

TEST_CASE("A forgotten position is gone") {
    StoreFixture fixture("store-forget");

    fixture.store.commit({"g", "orders", 0}, valueAt(100));
    fixture.store.forget({"g", "orders", 0});

    CHECK_FALSE(fixture.store.fetch({"g", "orders", 0}).has_value());
    CHECK(fixture.store.size() == 0);
}

TEST_CASE("Replay rebuilds what was committed") {
    StoreFixture fixture("store-replay");

    for (int i = 0; i < 20; ++i)
        fixture.store.commit({"g", "orders", i % 4}, valueAt(100 + i));
    const size_t before = fixture.store.size();

    // The map is thrown away and rebuilt from the topic alone — which is what a
    // coordinator does on failover.
    fixture.store.replay();

    CHECK(fixture.store.size() == before);
    CHECK(fixture.store.fetch({"g", "orders", 3})->nextOffset == Offset(119));
}

TEST_CASE("Replay keeps the last commit, not the first") {
    StoreFixture fixture("store-replay-last-wins");

    fixture.store.commit({"g", "orders", 0}, valueAt(100));
    fixture.store.commit({"g", "orders", 0}, valueAt(200));
    fixture.store.commit({"g", "orders", 0}, valueAt(300));

    fixture.store.replay();

    // Last write wins, in log order — which is exactly what compaction will
    // collapse the topic down to, so a replayed map and a compacted topic agree
    // by construction rather than by agreement.
    CHECK(fixture.store.fetch({"g", "orders", 0})->nextOffset == Offset(300));
    CHECK(fixture.store.size() == 1);
}

TEST_CASE("Replay honours a tombstone") {
    StoreFixture fixture("store-replay-tombstone");

    fixture.store.commit({"g", "orders", 0}, valueAt(100));
    fixture.store.commit({"g", "orders", 1}, valueAt(200));
    fixture.store.forget({"g", "orders", 0});

    fixture.store.replay();

    // A null value, not an empty one. An empty value would be a commit of offset
    // zero, and the position would come back rather than going away.
    CHECK_FALSE(fixture.store.fetch({"g", "orders", 0}).has_value());
    CHECK(fixture.store.fetch({"g", "orders", 1})->nextOffset == Offset(200));
    CHECK(fixture.store.size() == 1);
}

TEST_CASE("Offsets survive the broker being restarted") {
    TempDir dir("store-restart");
    const filesystem::path dataDir = dir.file("data");

    {
        storage::LogManager logs(dataDir, testConfig());
        ensureOffsetsTopic(logs, 8);
        OffsetStore store(logs, 8);
        for (int i = 0; i < 12; ++i) store.commit({"g", "orders", i % 3}, valueAt(500 + i));
    }

    // A fresh manager over the same directory, and a fresh store that has never
    // seen a commit. Everything below is the log doing the durability.
    storage::LogManager reopened(dataDir, testConfig());
    reopened.loadAll();
    OffsetStore restored(reopened, 8);
    restored.replay();

    CHECK(restored.size() == 3);
    CHECK(restored.fetch({"g", "orders", 0})->nextOffset == Offset(509));
    CHECK(restored.fetch({"g", "orders", 1})->nextOffset == Offset(510));
    CHECK(restored.fetch({"g", "orders", 2})->nextOffset == Offset(511));
}

TEST_CASE("Replaying an empty topic finds nothing") {
    StoreFixture fixture("store-replay-empty");
    fixture.store.replay();
    CHECK(fixture.store.size() == 0);
}

TEST_CASE("Metadata rides along with the offset") {
    StoreFixture fixture("store-metadata");

    CommitValue value       = valueAt(42);
    value.metadata          = "deploy-7";
    fixture.store.commit({"g", "orders", 0}, value);
    fixture.store.replay();

    // Opaque to the broker, like everything a client attaches — somewhere to put
    // a deploy id for whoever reads the topic later.
    CHECK(fixture.store.fetch({"g", "orders", 0})->metadata == "deploy-7");
}

TEST_CASE("Many commits across many partitions all come back") {
    StoreFixture fixture("store-replay-many");

    for (int g = 0; g < 5; ++g)
        for (int p = 0; p < 6; ++p)
            fixture.store.commit({"group-" + to_string(g), "orders", p}, valueAt(g * 100 + p));

    fixture.store.replay();

    CHECK(fixture.store.size() == 30);
    for (int g = 0; g < 5; ++g)
        for (int p = 0; p < 6; ++p)
            CHECK(fixture.store.fetch({"group-" + to_string(g), "orders", p})->nextOffset ==
                  Offset(g * 100 + p));
}

// ===========================================================================
// The group coordinator
// ===========================================================================

namespace {

const vector<string> kTopics    = {"orders"};
const vector<string> kProtocols = {"range", "roundrobin"};

JoinResult joinAs(GroupCoordinator& coordinator, const string& group, const string& memberId,
                  const string& clientId, int64_t nowMs = 1000) {
    return coordinator.join(group, memberId, clientId, kTopics, kProtocols, 10'000, nowMs);
}

// Brings a whole group to a settled generation.
//
// A single member cannot do this alone once the group has more than one: every
// member must rejoin before the membership settles, which is exactly what makes a
// rebalance stop-the-world. So this drives them all, in rounds, the way a set of
// real consumers retrying their own JoinGroup would.
vector<JoinResult> settle(GroupCoordinator& coordinator, const string& group,
                          vector<pair<string, string>>& members, int64_t nowMs = 1000) {
    vector<JoinResult> results(members.size());

    for (int round = 0; round < 5; ++round) {
        bool allSettled = true;
        for (size_t i = 0; i < members.size(); ++i) {
            results[i] = coordinator.join(group, members[i].first, members[i].second, kTopics,
                                          kProtocols, 10'000, nowMs);
            members[i].first = results[i].memberId;   // keep the issued id
            if (results[i].error == ErrorCode::RebalanceInProgress) allSettled = false;
        }
        if (allSettled) return results;
    }
    FAIL("group never settled");
    return results;
}

}  // namespace

TEST_CASE("A new group starts empty") {
    GroupCoordinator coordinator;
    CHECK(coordinator.groupCount() == 0);
    CHECK(coordinator.stateOf("g") == GroupState::Empty);
    CHECK(coordinator.memberCount("g") == 0);
}

TEST_CASE("The first member to join becomes the leader") {
    GroupCoordinator coordinator;

    const auto joined = joinAs(coordinator, "g", "", "consumer-a");

    CHECK(joined.error == ErrorCode::None);
    CHECK_FALSE(joined.memberId.empty());
    CHECK(joined.generation == 1);
    CHECK(joined.leaderId == joined.memberId);
    CHECK(joined.isLeader());
    CHECK(joined.members.size() == 1);
    CHECK(coordinator.stateOf("g") == GroupState::CompletingRebalance);
}

TEST_CASE("A member id is issued by the coordinator, not chosen by the client") {
    GroupCoordinator coordinator;

    vector<pair<string, string>> members{{"", "consumer"}, {"", "consumer"}};
    const auto                   results = settle(coordinator, "g", members);

    // Same client id, two members. Two consumers that picked their own and
    // collided would be indistinguishable, and one could commit for partitions
    // the other owns.
    CHECK(results[0].memberId != results[1].memberId);
    CHECK(coordinator.memberCount("g") == 2);
}

TEST_CASE("An id the group never issued is refused") {
    GroupCoordinator coordinator;
    joinAs(coordinator, "g", "", "consumer-a");

    const auto imposter = joinAs(coordinator, "g", "made-up-id", "consumer-b");
    CHECK(imposter.error == ErrorCode::UnknownMemberId);
}

TEST_CASE("An empty group id is refused") {
    GroupCoordinator coordinator;
    CHECK(joinAs(coordinator, "", "", "consumer").error == ErrorCode::InvalidGroupId);
}

TEST_CASE("Only the leader receives the member list") {
    GroupCoordinator coordinator;

    vector<pair<string, string>> members{{"", "consumer-a"}, {"", "consumer-b"}};
    const auto                   results = settle(coordinator, "g", members);

    REQUIRE(results[0].error == ErrorCode::None);
    REQUIRE(results[1].error == ErrorCode::None);

    // Exactly one leader, and only it is told who the others are. Everyone else
    // has nothing to compute with the list, and sending it would tell every
    // consumer who its peers are for no reason.
    const int leaders = (results[0].isLeader() ? 1 : 0) + (results[1].isLeader() ? 1 : 0);
    CHECK(leaders == 1);

    const auto& leader   = results[0].isLeader() ? results[0] : results[1];
    const auto& follower = results[0].isLeader() ? results[1] : results[0];
    CHECK(leader.members.size() == 2);
    CHECK(follower.members.empty());
    CHECK(follower.leaderId == leader.memberId);
    CHECK(leader.leaderId == leader.memberId);
}

TEST_CASE("A second member joining rebalances the group") {
    GroupCoordinator coordinator;

    const auto a = joinAs(coordinator, "g", "", "consumer-a");
    CHECK(a.generation == 1);

    // Both members must rejoin before the group settles, which is what makes
    // this stop-the-world.
    const auto b = joinAs(coordinator, "g", "", "consumer-b");
    CHECK(b.error == ErrorCode::RebalanceInProgress);
    CHECK(coordinator.stateOf("g") == GroupState::PreparingRebalance);

    const auto aRejoined = joinAs(coordinator, "g", a.memberId, "consumer-a");
    CHECK(aRejoined.error == ErrorCode::None);
    CHECK(aRejoined.generation == 2);   // bumped when the membership settled
    CHECK(coordinator.memberCount("g") == 2);
}

TEST_CASE("A heartbeat is how a member learns of a rebalance") {
    GroupCoordinator coordinator;

    const auto a = joinAs(coordinator, "g", "", "consumer-a");
    coordinator.sync("g", a.memberId, a.generation, {{a.memberId, {1, 2, 3}}}, 1000);
    REQUIRE(coordinator.stateOf("g") == GroupState::Stable);
    CHECK(coordinator.heartbeat("g", a.memberId, a.generation, 1100) == ErrorCode::None);

    // Somebody else arrives.
    joinAs(coordinator, "g", "", "consumer-b");

    // Not a failure — the signal, delivered on the heartbeat this member was
    // making anyway. That is why heartbeats exist rather than the coordinator
    // simply timing members out.
    CHECK(coordinator.heartbeat("g", a.memberId, a.generation, 1200) ==
          ErrorCode::RebalanceInProgress);
}

TEST_CASE("A stale generation is fenced out") {
    GroupCoordinator coordinator;

    const auto a = joinAs(coordinator, "g", "", "consumer-a");
    coordinator.sync("g", a.memberId, a.generation, {{a.memberId, {}}}, 1000);

    // Force a new generation.
    joinAs(coordinator, "g", "", "consumer-b");
    joinAs(coordinator, "g", a.memberId, "consumer-a");

    // A zombie from generation 1 waking up. Without fencing it would supply an
    // assignment, or commit, for partitions it no longer owns.
    CHECK(coordinator.heartbeat("g", a.memberId, 1, 1300) == ErrorCode::IllegalGeneration);
    CHECK(coordinator.sync("g", a.memberId, 1, {}, 1300).error == ErrorCode::IllegalGeneration);
}

TEST_CASE("The leader's assignment reaches each member and nobody else's") {
    GroupCoordinator coordinator;

    vector<pair<string, string>> members{{"", "consumer-a"}, {"", "consumer-b"}};
    const auto                   results = settle(coordinator, "g", members);

    const auto& leader = results[0].isLeader() ? results[0] : results[1];
    const auto& other  = results[0].isLeader() ? results[1] : results[0];

    map<string, vector<uint8_t>> assignments;
    assignments[leader.memberId] = {0xAA};
    assignments[other.memberId]  = {0xBB};

    const auto leaderSync = coordinator.sync("g", leader.memberId, leader.generation,
                                             assignments, 2000);
    CHECK(leaderSync.error == ErrorCode::None);
    CHECK(leaderSync.assignment == vector<uint8_t>{0xAA});
    CHECK(coordinator.stateOf("g") == GroupState::Stable);

    const auto otherSync = coordinator.sync("g", other.memberId, other.generation, {}, 2000);
    CHECK(otherSync.error == ErrorCode::None);
    CHECK(otherSync.assignment == vector<uint8_t>{0xBB});
}

TEST_CASE("A follower syncing before the leader is told to retry") {
    GroupCoordinator coordinator;

    vector<pair<string, string>> members{{"", "consumer-a"}, {"", "consumer-b"}};
    const auto                   results  = settle(coordinator, "g", members);
    const auto&                  follower = results[0].isLeader() ? results[1] : results[0];
    CHECK(coordinator.sync("g", follower.memberId, follower.generation, {}, 2000).error ==
          ErrorCode::RebalanceInProgress);
}

TEST_CASE("A protocol every member supports is chosen") {
    GroupCoordinator coordinator;

    // One member supports both strategies, the other only round-robin.
    string aId;
    string bId;
    for (int round = 0; round < 4; ++round) {
        const auto a = coordinator.join("g", aId, "a", kTopics, {"range", "roundrobin"},
                                        10'000, 1000);
        aId          = a.memberId;
        const auto b = coordinator.join("g", bId, "b", kTopics, {"roundrobin"}, 10'000, 1000);
        bId          = b.memberId;
        if (a.error == ErrorCode::None && b.error == ErrorCode::None) {
            // Not the leader's first choice — the one everyone can actually do.
            CHECK(a.protocolName == "roundrobin");
            CHECK(b.protocolName == "roundrobin");
            return;
        }
    }
    FAIL("group never settled");
}

TEST_CASE("A member leaving rebalances the rest") {
    GroupCoordinator coordinator;

    const auto a = joinAs(coordinator, "g", "", "consumer-a");
    const auto b = joinAs(coordinator, "g", "", "consumer-b");
    joinAs(coordinator, "g", a.memberId, "consumer-a");
    REQUIRE(coordinator.memberCount("g") == 2);

    CHECK(coordinator.leave("g", b.memberId, 3000) == ErrorCode::None);
    CHECK(coordinator.memberCount("g") == 1);

    // A departure changes the assignment, so what is left has to REJOIN — the
    // same stop-the-world path a join takes, not a quiet fix-up.
    CHECK(coordinator.stateOf("g") == GroupState::PreparingRebalance);
}

TEST_CASE("The last member leaving removes the group") {
    GroupCoordinator coordinator;

    const auto a = joinAs(coordinator, "g", "", "consumer-a");
    CHECK(coordinator.groupCount() == 1);

    CHECK(coordinator.leave("g", a.memberId, 2000) == ErrorCode::None);
    CHECK(coordinator.groupCount() == 0);
}

TEST_CASE("Leaving a group you are not in is refused") {
    GroupCoordinator coordinator;
    CHECK(coordinator.leave("g", "nobody", 1000) == ErrorCode::UnknownMemberId);

    joinAs(coordinator, "g", "", "consumer-a");
    CHECK(coordinator.leave("g", "nobody", 1000) == ErrorCode::UnknownMemberId);
}

TEST_CASE("A member that stops heartbeating is expired") {
    GroupCoordinator coordinator;

    const auto a = coordinator.join("g", "", "a", kTopics, kProtocols, 5'000, 1000);
    const auto b = coordinator.join("g", "", "b", kTopics, kProtocols, 5'000, 1000);
    coordinator.join("g", a.memberId, "a", kTopics, kProtocols, 5'000, 1000);
    REQUIRE(coordinator.memberCount("g") == 2);

    // a keeps heartbeating; b does not. b's last sign of life was at 1000 and its
    // session timeout is 5000, so 6500 is the first sweep that should notice.
    coordinator.heartbeat("g", a.memberId, 2, 4000);

    CHECK(coordinator.expire(6500) == 1);
    CHECK(coordinator.memberCount("g") == 1);

    // The survivor has to rejoin, exactly as it would after a member left
    // politely — an expiry is a departure the member did not announce.
    CHECK(coordinator.stateOf("g") == GroupState::PreparingRebalance);
}

TEST_CASE("A group whose members all vanish is removed") {
    GroupCoordinator coordinator;
    coordinator.join("g", "", "a", kTopics, kProtocols, 5'000, 1000);
    REQUIRE(coordinator.groupCount() == 1);

    // The reason expiry runs on a thread at all: nothing else would ever notice,
    // and the group's partitions would stay assigned to nobody forever.
    CHECK(coordinator.expire(10'000) == 1);
    CHECK(coordinator.groupCount() == 0);
}

TEST_CASE("A live group is not expired") {
    GroupCoordinator coordinator;
    const auto a = coordinator.join("g", "", "a", kTopics, kProtocols, 5'000, 1000);
    coordinator.sync("g", a.memberId, a.generation, {{a.memberId, {}}}, 1000);

    coordinator.heartbeat("g", a.memberId, a.generation, 4000);
    CHECK(coordinator.expire(5000) == 0);
    CHECK(coordinator.memberCount("g") == 1);
}

TEST_CASE("Groups are independent of each other") {
    GroupCoordinator coordinator;

    const auto a = joinAs(coordinator, "group-a", "", "consumer");
    const auto b = joinAs(coordinator, "group-b", "", "consumer");

    CHECK(coordinator.groupCount() == 2);
    CHECK(coordinator.memberCount("group-a") == 1);
    CHECK(coordinator.memberCount("group-b") == 1);

    coordinator.leave("group-a", a.memberId, 2000);
    CHECK(coordinator.groupCount() == 1);
    CHECK(coordinator.memberCount("group-b") == 1);
}
