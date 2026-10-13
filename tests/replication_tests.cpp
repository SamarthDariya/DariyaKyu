#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include <fstream>
#include <thread>

#include "common/errors.hpp"
#include "replication/partition_replicas.hpp"
#include "replication/replica_fetcher.hpp"
#include "replication/replica_manager.hpp"
#include "server/broker.hpp"
#include "test_support.hpp"

using namespace std;
using namespace dariyakyu;
using namespace dariyakyu::replication;
using namespace dariyakyu::test;

// ===========================================================================
// PartitionReplicas: the high watermark
// ===========================================================================

TEST_CASE("The high watermark is the lowest log end across the replicas") {
    PartitionReplicas replicas(1, {1, 2, 3}, Offset(0), 1000);

    // The leader has 10. Until the followers fetch, nothing is committed.
    CHECK(replicas.updateHighWatermark(Offset(10)) == Offset(0));

    // Node 2 has everything below 7, node 3 everything below 4.
    replicas.recordFetch(2, Offset(7), 1000);
    replicas.recordFetch(3, Offset(4), 1000);
    CHECK(replicas.updateHighWatermark(Offset(10)) == Offset(4));

    // The slowest catches up past the other: now node 2 is the limit.
    replicas.recordFetch(3, Offset(9), 1100);
    CHECK(replicas.updateHighWatermark(Offset(10)) == Offset(7));
    CHECK(replicas.followerLogEnd(3) == Offset(9));
}

TEST_CASE("Followers ahead of the leader do not commit what the leader lacks") {
    // Not reachable today — a follower copies the leader — but the minimum has to
    // include the leader for the rule to mean "everyone has it".
    PartitionReplicas replicas(1, {1, 2}, Offset(0), 1000);
    replicas.recordFetch(2, Offset(8), 1000);
    CHECK(replicas.updateHighWatermark(Offset(5)) == Offset(5));
}

TEST_CASE("A partition with no followers commits what the leader has") {
    PartitionReplicas replicas(1, {1}, Offset(0), 1000);
    CHECK(replicas.updateHighWatermark(Offset(12)) == Offset(12));
}

TEST_CASE("The high watermark never moves backwards") {
    // A consumer may already have read below it and acted — charged a card,
    // sent an email. A follower restarting and fetching from lower down must not
    // make those records look uncommitted again.
    PartitionReplicas replicas(1, {1, 2}, Offset(0), 1000);
    replicas.recordFetch(2, Offset(6), 1000);
    REQUIRE(replicas.updateHighWatermark(Offset(6)) == Offset(6));

    replicas.recordFetch(2, Offset(2), 1100);
    CHECK(replicas.updateHighWatermark(Offset(6)) == Offset(6));
    CHECK(replicas.followerLogEnd(2) == Offset(2));   // the follower's truth is kept
}

TEST_CASE("Taking leadership starts from the log's watermark, not from zero") {
    PartitionReplicas replicas(1, {1, 2}, Offset(40), 1000);
    CHECK(replicas.highWatermark() == Offset(40));
    CHECK(replicas.updateHighWatermark(Offset(50)) == Offset(40));   // node 2 unheard
}

TEST_CASE("A fetch from a node that is not a follower is a bug, and says so") {
    PartitionReplicas replicas(1, {1, 2}, Offset(0), 1000);
    CHECK_THROWS_AS(replicas.recordFetch(3, Offset(1), 1000), Error);
    CHECK_THROWS_AS(replicas.recordFetch(1, Offset(1), 1000), Error);   // the leader itself
    CHECK_THROWS_AS(replicas.followerLogEnd(3), Error);
}

// ===========================================================================
// PartitionReplicas: the in-sync set
// ===========================================================================

TEST_CASE("A follower that stops fetching drops out, and stops holding the watermark") {
    PartitionReplicas replicas(1, {1, 2, 3}, Offset(0), 1000, /*lagTimeMaxMs=*/100);
    replicas.recordFetch(2, Offset(8), 1050);
    replicas.recordFetch(3, Offset(3), 1050);
    REQUIRE(replicas.updateHighWatermark(Offset(10)) == Offset(3));

    // Node 2 keeps fetching; node 3 goes quiet.
    replicas.recordFetch(2, Offset(10), 1140);
    CHECK(replicas.shrinkInSync(1140).empty());     // 90 ms: still inside the window
    CHECK(replicas.shrinkInSync(1151) == vector<NodeId>{3});   // 101 ms: out
    CHECK(replicas.inSync() == vector<NodeId>{1, 2});

    // Without node 3, everything node 2 has is committed. One dead follower no
    // longer freezes every consumer.
    CHECK(replicas.updateHighWatermark(Offset(10)) == Offset(10));
}

TEST_CASE("A follower far behind but still fetching stays in sync") {
    // The time rule, not a distance rule: a follower thousands of records back,
    // catching up after a burst, is healthy and must keep its place.
    PartitionReplicas replicas(1, {1, 2}, Offset(0), 1000, 100);
    for (int64_t now = 1050; now <= 1500; now += 50) replicas.recordFetch(2, Offset(5), now);

    CHECK(replicas.shrinkInSync(1500).empty());
    CHECK(replicas.updateHighWatermark(Offset(50'000)) == Offset(5));
}

TEST_CASE("A dropped follower rejoins only once it holds everything committed") {
    PartitionReplicas replicas(1, {1, 2}, Offset(0), 1000, 100);
    REQUIRE(replicas.shrinkInSync(1200) == vector<NodeId>{2});
    REQUIRE(replicas.updateHighWatermark(Offset(20)) == Offset(20));

    // Fetching again, but below the watermark: counting it now would mean a
    // watermark of 12 — lower than 20, which consumers may already have read to.
    replicas.recordFetch(2, Offset(12), 1250);
    CHECK(replicas.inSync() == vector<NodeId>{1});

    replicas.recordFetch(2, Offset(20), 1300);
    CHECK(replicas.inSync() == vector<NodeId>{1, 2});
}

TEST_CASE("A follower that has never fetched gets a full window first") {
    // A leader that has only just started tracking a follower has no evidence it
    // is dead, so it is in sync until the window runs out from that moment.
    PartitionReplicas replicas(1, {1, 2}, Offset(0), 1000, 100);
    CHECK(replicas.shrinkInSync(1100).empty());
    CHECK(replicas.shrinkInSync(1101) == vector<NodeId>{2});
}

// ===========================================================================
// ReplicaManager: keeping each Log's watermark in step
// ===========================================================================

namespace {

unique_ptr<storage::Log> logWithReplicas(TempDir& dir, const string& name, vector<NodeId> replicas) {
    storage::LogConfig config   = testConfig();
    config.replication.replicas = std::move(replicas);
    return storage::Log::create(TopicPartition{name, 0}, dir.file(name + "-0"), config);
}

void appendRecords(storage::Log& log, int count) {
    for (int i = 0; i < count; ++i) {
        auto bytes = makeUnstampedBatch(1000 + i);
        log.append(bytes);
    }
}

}  // namespace

TEST_CASE("Only partitions led here with followers are tracked") {
    TempDir        dir("manager-track");
    ReplicaManager manager(1);

    auto alone     = logWithReplicas(dir, "alone", {});       // predates replication
    auto following = logWithReplicas(dir, "following", {2, 1});   // node 2 leads
    manager.track(*alone, 1000);
    manager.track(*following, 1000);

    CHECK_FALSE(manager.inSync(TopicPartition{"alone", 0}).has_value());
    CHECK_FALSE(manager.inSync(TopicPartition{"following", 0}).has_value());

    // Untracked, so they still commit on append as they always have.
    appendRecords(*alone, 2);
    CHECK(alone->highWatermark() == Offset(2));
}

TEST_CASE("A led partition commits what its followers have fetched") {
    TempDir        dir("manager-fetch");
    ReplicaManager manager(1);
    auto           log = logWithReplicas(dir, "orders", {1, 2});
    manager.track(*log, 1000);

    // Written, not committed: node 2 has none of it yet.
    appendRecords(*log, 3);
    manager.logAppended(TopicPartition{"orders", 0});
    CHECK(log->highWatermark() == Offset(0));

    manager.recordFetch(TopicPartition{"orders", 0}, 2, Offset(2), 1100);
    CHECK(log->highWatermark() == Offset(2));
    CHECK(manager.inSync(TopicPartition{"orders", 0}) == vector<NodeId>{1, 2});
}

TEST_CASE("With every follower gone, the leader's own writes commit at once") {
    TempDir        dir("manager-alone");
    ReplicaManager manager(1, /*lagTimeMaxMs=*/100);
    auto           log = logWithReplicas(dir, "orders", {1, 2});
    manager.track(*log, 1000);
    appendRecords(*log, 3);

    // Node 2 never fetches. The timer drops it, and the watermark moves past it.
    manager.shrinkInSync(1101);
    CHECK(manager.inSync(TopicPartition{"orders", 0}) == vector<NodeId>{1});
    CHECK(log->highWatermark() == Offset(3));

    // From now on the leader alone is the in-sync set: an append is committed
    // as soon as it is reported, not at the next fetch or timer.
    appendRecords(*log, 2);
    manager.logAppended(TopicPartition{"orders", 0});
    CHECK(log->highWatermark() == Offset(5));
}

TEST_CASE("Fetches, appends, the timer and readers can all race on one partition") {
    // Each runs on its own connection's thread in the broker. TSan is the real
    // assertion here; the final check is that nothing was lost in the scramble.
    TempDir        dir("manager-race");
    ReplicaManager manager(1);
    auto           log = logWithReplicas(dir, "orders", {1, 2});
    const TopicPartition tp{"orders", 0};
    manager.track(*log, 1000);

    thread appender([&] {
        for (int i = 0; i < 200; ++i) {
            auto bytes = makeUnstampedBatch(i);
            log->append(bytes);
            manager.logAppended(tp);
        }
    });
    thread follower([&] {
        for (int i = 0; i < 200; ++i) manager.recordFetch(tp, 2, log->logEndOffset(), 1000 + i);
    });
    thread timerAndReader([&] {
        for (int i = 0; i < 200; ++i) {
            manager.shrinkInSync(1000 + i);
            CHECK(manager.inSync(tp).has_value());
        }
    });
    appender.join();
    follower.join();
    timerAndReader.join();

    // The follower catches up for good, and the watermark lands on the end.
    manager.recordFetch(tp, 2, log->logEndOffset(), 2000);
    CHECK(log->highWatermark() == Offset(200));
}

// ===========================================================================
// fetchOnce: one round of following, against a real leader
// ===========================================================================

namespace {

// Broker 1 leading orders-0, followed by node 2, with `batches` one-record
// batches already written. Node 2 has fetched nothing, so none is committed.
struct LeaderAndFollower {
    TempDir                   dir;
    unique_ptr<server::Broker> leader;
    unique_ptr<storage::Log>   follower;   // node 2's copy, on its own disk

    // `port` 0 lets the OS pick; a fixed one lets a test start a leader where a
    // fetcher was already looking for one.
    LeaderAndFollower(const string& name, int batches, int32_t port = 0) : dir(name) {
        storage::LogConfig config   = testConfig();
        config.replication.replicas = {1, 2};
        {
            storage::LogManager logs(dir.file("leader"), testConfig());
            logs.createPartition(TopicPartition{"orders", 0}, config);
        }

        server::Broker::Options options;
        options.dataDir  = dir.file("leader");
        options.defaults = testConfig();
        options.host     = "127.0.0.1";
        options.port     = port;
        options.nodeId   = 1;
        leader           = make_unique<server::Broker>(options);
        leader->start();

        for (int i = 0; i < batches; ++i) {
            auto bytes = makeUnstampedBatch(1000 + i, 48);
            leaderLog().append(bytes);
        }
        follower = storage::Log::create(TopicPartition{"orders", 0}, dir.file("follower/orders-0"),
                                        config);
    }
    ~LeaderAndFollower() { leader->stop(); }

    storage::Log&    leaderLog() { return *leader->logs().get(TopicPartition{"orders", 0}); }
    protocol::Client connect() { return protocol::Client("127.0.0.1", leader->port()); }
};

}  // namespace

TEST_CASE("A follower copies the leader's batches, byte for byte, at the same offsets") {
    LeaderAndFollower cluster("fetch-once-copies", 3);
    auto              client = cluster.connect();

    const auto round = fetchOnce(client, *cluster.follower, 2);
    CHECK(round.error == protocol::ErrorCode::None);
    CHECK(round.batches == 3);
    CHECK(cluster.follower->logEndOffset() == Offset(3));

    // The same bytes: what a consumer would read from either is identical.
    const auto onLeader   = pullRange(cluster.leaderLog().read(Offset(0), kBigFetch).range);
    const auto onFollower = pullRange(cluster.follower->read(Offset(0), kBigFetch).range);
    CHECK(onLeader == onFollower);
}

TEST_CASE("The watermark reaches the follower one round after it reaches the leader") {
    LeaderAndFollower cluster("fetch-once-watermark", 3);
    auto              client = cluster.connect();

    // Round one fetches at 0: the leader learns nothing new, commits nothing.
    fetchOnce(client, *cluster.follower, 2);
    CHECK(cluster.leaderLog().highWatermark() == Offset(0));
    CHECK(cluster.follower->highWatermark() == Offset(0));

    // Round two fetches at 3: the leader commits 3 and says so in the reply.
    const auto round = fetchOnce(client, *cluster.follower, 2);
    CHECK(round.batches == 0);   // nothing new to copy
    CHECK(cluster.leaderLog().highWatermark() == Offset(3));
    CHECK(cluster.follower->highWatermark() == Offset(3));
}

TEST_CASE("A response cut mid-batch leaves the partial batch for the next round") {
    LeaderAndFollower cluster("fetch-once-partial", 3);
    auto              client = cluster.connect();
    const auto        batch  = static_cast<int32_t>(makeUnstampedBatch(1000, 48).size());

    // One and a half batches' worth: the second arrives cut short.
    CHECK(fetchOnce(client, *cluster.follower, 2, batch + batch / 2).batches == 1);
    CHECK(cluster.follower->logEndOffset() == Offset(1));

    // Nothing lost: the next round starts at 1 and gets the rest whole.
    CHECK(fetchOnce(client, *cluster.follower, 2).batches == 2);
    CHECK(cluster.follower->logEndOffset() == Offset(3));
}

TEST_CASE("A leader's refusal comes back as an error, and writes nothing") {
    LeaderAndFollower cluster("fetch-once-refused", 2);
    auto              client = cluster.connect();

    // Node 3 holds no copy, so the leader will not let it read past the
    // watermark — the follower's log must stay exactly as it was.
    const auto round = fetchOnce(client, *cluster.follower, 3);
    CHECK(round.error == protocol::ErrorCode::UnknownTopicOrPartition);
    CHECK(round.batches == 0);
    CHECK(cluster.follower->logEndOffset() == Offset(0));
}

TEST_CASE("A batch that fails its checksum in transit is refused, not copied") {
    LeaderAndFollower cluster("fetch-once-corrupt", 1);

    // Damage the record body on the leader's disk. The leader serves file bytes
    // without reading them, so the follower is the first to look — and the last
    // chance before those bytes would be served to consumers after a failover.
    const auto segment = segmentLogPath(cluster.dir.file("leader") / "orders-0", Offset(0));
    {
        fstream file(segment, ios::in | ios::out | ios::binary);
        file.seekp(-1, ios::end);
        file.put('\x5A');
    }

    auto client = cluster.connect();
    CHECK_THROWS_AS(fetchOnce(client, *cluster.follower, 2), CorruptData);
    CHECK(cluster.follower->logEndOffset() == Offset(0));
}

// ===========================================================================
// ReplicaFetcher: the thread
// ===========================================================================

namespace {

// Polls `done` every 10 ms for up to two seconds.
template <typename Done>
bool eventually(Done done) {
    for (int waited = 0; waited < 200; ++waited) {
        if (done()) return true;
        this_thread::sleep_for(chrono::milliseconds(10));
    }
    return done();
}

ReplicaFetcher::Options followNode1(int32_t port) {
    ReplicaFetcher::Options options;
    options.self       = 2;
    options.leaderHost = "127.0.0.1";
    options.leaderPort = port;
    options.timeoutMs  = 500;
    options.idleWaitMs = 10;
    options.backoffMs  = 20;
    return options;
}

}  // namespace

TEST_CASE("A fetcher catches up, commits, and keeps up as the leader writes") {
    LeaderAndFollower cluster("fetcher-keeps-up", 3);
    ReplicaFetcher    fetcher(followNode1(cluster.leader->port()), {cluster.follower.get()});
    fetcher.start();

    // Copied, then committed on both sides: the second pass tells the leader the
    // follower has all 3, and the reply tells the follower they are committed.
    CHECK(eventually([&] { return cluster.follower->highWatermark() == Offset(3); }));
    CHECK(cluster.leaderLog().highWatermark() == Offset(3));

    for (int i = 0; i < 2; ++i) {
        auto bytes = makeUnstampedBatch(5000 + i, 48);
        cluster.leaderLog().append(bytes);
    }
    CHECK(eventually([&] { return cluster.follower->highWatermark() == Offset(5); }));
    CHECK(fetcher.failures() == 0);

    fetcher.stop();
}

TEST_CASE("A fetcher stops promptly, even in the middle of a long wait") {
    LeaderAndFollower cluster("fetcher-stops", 1);
    auto              options = followNode1(cluster.leader->port());
    options.idleWaitMs        = 10'000;
    ReplicaFetcher fetcher(options, {cluster.follower.get()});
    fetcher.start();
    REQUIRE(eventually([&] { return cluster.follower->logEndOffset() == Offset(1); }));

    // Caught up, so it is now sitting in a ten-second idle wait.
    const auto start = chrono::steady_clock::now();
    fetcher.stop();
    CHECK(chrono::steady_clock::now() - start < chrono::milliseconds(1'000));
}

TEST_CASE("A fetcher backs off while the leader is gone, and catches up when it returns") {
    // A port with nothing on it — yet.
    int32_t port = 0;
    {
        protocol::Socket probe = protocol::Socket::listenOn("127.0.0.1", 0);
        port                   = probe.localPort();
    }

    TempDir                 followerDir("fetcher-waits-follower");
    storage::LogConfig      config   = testConfig();
    config.replication.replicas      = {1, 2};
    auto follower = storage::Log::create(TopicPartition{"orders", 0},
                                         followerDir.file("orders-0"), config);

    ReplicaFetcher fetcher(followNode1(port), {follower.get()});
    fetcher.start();
    CHECK(eventually([&] { return fetcher.failures() >= 2; }));   // refused, backing off

    // The leader comes up on that port with records the follower never saw.
    LeaderAndFollower cluster("fetcher-waits-leader", 3, port);
    CHECK(eventually([&] { return follower->logEndOffset() == Offset(3); }));

    fetcher.stop();
}
