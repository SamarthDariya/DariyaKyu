#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include <set>

#include "cluster/cluster_config.hpp"
#include "cluster/replica_assignment.hpp"
#include "common/errors.hpp"

using namespace std;
using namespace dariyakyu;
using namespace dariyakyu::cluster;

// ===========================================================================
// ClusterConfig
// ===========================================================================

TEST_CASE("A cluster spec parses into its nodes") {
    const auto config = parseClusterConfig("1@127.0.0.1:9092,2@localhost:9093");

    REQUIRE(config.nodes.size() == 2);
    CHECK(config.nodes[0] == NodeAddress{1, "127.0.0.1", 9092});
    CHECK(config.nodes[1] == NodeAddress{2, "localhost", 9093});
}

TEST_CASE("Nodes are sorted by id, whatever order they were typed in") {
    // Two brokers given the same cluster in a different order must still agree on
    // which node leads each partition. So the order is the ids', not the flag's.
    const auto config = parseClusterConfig("3@h:3,1@h:1,2@h:2");
    CHECK(config.nodes[0].id == 1);
    CHECK(config.nodes[1].id == 2);
    CHECK(config.nodes[2].id == 3);
}

TEST_CASE("A node is found by id, and a stranger is not") {
    const auto config = parseClusterConfig("1@h:9092,2@h:9093");
    REQUIRE(config.find(2) != nullptr);
    CHECK(config.find(2)->port == 9093);
    CHECK(config.find(7) == nullptr);
}

TEST_CASE("A malformed cluster spec is refused, not guessed at") {
    for (const char* bad : {"",                  // no nodes
                            "1@h:9092,1@h:9093", // duplicate id
                            "h:9092",            // no id
                            "1@h",               // no port
                            "1@:9092",           // no host
                            "x@h:9092",          // id is not a number
                            "1@h:92x",           // trailing junk in the port
                            "1@h:0",             // port out of range
                            "-1@h:9092"})        // negative id
        CHECK_THROWS_AS(parseClusterConfig(bad), Error);
}

// ===========================================================================
// Replica assignment
// ===========================================================================

TEST_CASE("Replicas rotate through the nodes, leader first") {
    const auto cluster = parseClusterConfig("1@h:1,2@h:2,3@h:3");

    CHECK(assignReplicas(0, 3, cluster) == vector<NodeId>{1, 2, 3});
    CHECK(assignReplicas(1, 3, cluster) == vector<NodeId>{2, 3, 1});
    CHECK(assignReplicas(2, 3, cluster) == vector<NodeId>{3, 1, 2});
    CHECK(assignReplicas(3, 3, cluster) == vector<NodeId>{1, 2, 3});   // round again
    CHECK(assignReplicas(1, 2, cluster) == vector<NodeId>{2, 3});
}

TEST_CASE("Leadership spreads across the brokers") {
    // The reason for rotating at all. Led by the first node every time, one
    // broker would take every write while the rest only copied.
    const auto  cluster = parseClusterConfig("1@h:1,2@h:2,3@h:3");
    set<NodeId> leaders;
    for (PartitionId p = 0; p < 3; ++p) leaders.insert(assignReplicas(p, 2, cluster).front());
    CHECK(leaders == set<NodeId>{1, 2, 3});
}

TEST_CASE("Every broker computes the same assignment") {
    // No controller hands these out at M7, so agreement comes only from the
    // inputs. The same cluster typed in another order must give the same answer.
    const auto a = parseClusterConfig("1@h:1,2@h:2,3@h:3");
    const auto b = parseClusterConfig("3@h:3,1@h:1,2@h:2");
    for (PartitionId p = 0; p < 6; ++p) CHECK(assignReplicas(p, 2, a) == assignReplicas(p, 2, b));
}

TEST_CASE("A replication factor the cluster cannot hold is refused") {
    const auto cluster = parseClusterConfig("1@h:1,2@h:2");
    CHECK_THROWS_AS(assignReplicas(0, 0, cluster), Error);
    CHECK_THROWS_AS(assignReplicas(0, 3, cluster), Error);   // two copies on one broker
    CHECK_THROWS_AS(assignReplicas(-1, 1, cluster), Error);
}
