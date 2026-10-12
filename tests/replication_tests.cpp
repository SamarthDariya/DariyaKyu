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
    PartitionReplicas replicas(1, {1, 2, 3}, Offset(0));

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
    PartitionReplicas replicas(1, {1, 2}, Offset(0));
    replicas.recordFetch(2, Offset(8), 1000);
    CHECK(replicas.updateHighWatermark(Offset(5)) == Offset(5));
}

TEST_CASE("A partition with no followers commits what the leader has") {
    PartitionReplicas replicas(1, {1}, Offset(0));
    CHECK(replicas.updateHighWatermark(Offset(12)) == Offset(12));
}

TEST_CASE("The high watermark never moves backwards") {
    // A consumer may already have read below it and acted — charged a card,
    // sent an email. A follower restarting and fetching from lower down must not
    // make those records look uncommitted again.
    PartitionReplicas replicas(1, {1, 2}, Offset(0));
    replicas.recordFetch(2, Offset(6), 1000);
    REQUIRE(replicas.updateHighWatermark(Offset(6)) == Offset(6));

    replicas.recordFetch(2, Offset(2), 1100);
    CHECK(replicas.updateHighWatermark(Offset(6)) == Offset(6));
    CHECK(replicas.followerLogEnd(2) == Offset(2));   // the follower's truth is kept
}

TEST_CASE("Taking leadership starts from the log's watermark, not from zero") {
    PartitionReplicas replicas(1, {1, 2}, Offset(40));
    CHECK(replicas.highWatermark() == Offset(40));
    CHECK(replicas.updateHighWatermark(Offset(50)) == Offset(40));   // node 2 unheard
}

TEST_CASE("A fetch from a node that is not a follower is a bug, and says so") {
    PartitionReplicas replicas(1, {1, 2}, Offset(0));
    CHECK_THROWS_AS(replicas.recordFetch(3, Offset(1), 1000), Error);
    CHECK_THROWS_AS(replicas.recordFetch(1, Offset(1), 1000), Error);   // the leader itself
    CHECK_THROWS_AS(replicas.followerLogEnd(3), Error);
}
