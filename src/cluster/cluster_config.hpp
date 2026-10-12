#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "common/types.hpp"

namespace dariyakyu::cluster {

// One broker, as every other broker knows it.
struct NodeAddress {
    NodeId       id = 0;
    std::string  host;
    std::int32_t port = 0;

    bool operator==(const NodeAddress&) const = default;
};

// Every broker in the cluster, fixed at startup (M7 — M8's controller makes
// membership dynamic).
//
// Every broker is given the same list, and `nodes` is kept SORTED BY ID. That is
// what lets each broker compute the same replica assignment on its own: the
// order the flag was typed in must not change which node leads a partition.
struct ClusterConfig {
    std::vector<NodeAddress> nodes;

    // The node with this id, or nullptr if it is not a member.
    const NodeAddress* find(NodeId id) const;
};

// Parses "1@127.0.0.1:9092,2@127.0.0.1:9093".
//
// Throws Error on anything malformed — an empty list, a duplicate id, a missing
// port. A broker started with a cluster it misread would agree with nobody about
// who leads what, so refusing to start is the only safe answer.
ClusterConfig parseClusterConfig(std::string_view spec);

}  // namespace dariyakyu::cluster
