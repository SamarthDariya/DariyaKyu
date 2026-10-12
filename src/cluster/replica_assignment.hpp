#pragma once

#include <cstdint>
#include <vector>

#include "cluster/cluster_config.hpp"
#include "common/types.hpp"

namespace dariyakyu::cluster {

// Which nodes hold `partition`, leader first.
//
// Replica i goes to node (partition + i) % n, counting through the cluster's
// nodes in id order. Starting one node further along for each partition is what
// spreads leadership: with three nodes, partitions 0, 1 and 2 are led by three
// different brokers instead of all by the first.
//
// A pure function of its arguments, and that is the point. At M7 there is no
// controller to hand out assignments, so every broker computes them itself and
// they agree because the inputs are the same — ClusterConfig sorts its nodes
// for exactly this reason.
//
// Throws Error if replicationFactor is below one or above the node count: two
// replicas on one broker are one copy that is counted twice.
std::vector<NodeId> assignReplicas(PartitionId partition, std::int32_t replicationFactor,
                                   const ClusterConfig& cluster);

}  // namespace dariyakyu::cluster
