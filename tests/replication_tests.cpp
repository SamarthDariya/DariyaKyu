#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include "common/errors.hpp"
#include "replication/partition_replicas.hpp"

using namespace std;
using namespace dariyakyu;
using namespace dariyakyu::replication;

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
