#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include "cluster/cluster_config.hpp"
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
