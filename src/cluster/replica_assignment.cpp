#include "cluster/replica_assignment.hpp"

#include <string>

#include "common/errors.hpp"

using namespace std;

namespace dariyakyu::cluster {

vector<NodeId> assignReplicas(PartitionId partition, int32_t replicationFactor,
                              const ClusterConfig& cluster) {
    const auto nodeCount = static_cast<int32_t>(cluster.nodes.size());
    if (replicationFactor < 1 || replicationFactor > nodeCount)
        throw Error("replication factor " + to_string(replicationFactor) + " needs between 1 and " +
                    to_string(nodeCount) + " nodes in this cluster");
    if (partition < 0) throw Error("partition " + to_string(partition) + " is negative");

    // Start at node (partition % n) and take the next replicationFactor nodes,
    // wrapping round. Each node appears at most once because replicationFactor
    // is at most n.
    vector<NodeId> replicas;
    replicas.reserve(static_cast<size_t>(replicationFactor));
    for (int32_t i = 0; i < replicationFactor; ++i)
        replicas.push_back(cluster.nodes[static_cast<size_t>((partition + i) % nodeCount)].id);
    return replicas;
}

}  // namespace dariyakyu::cluster
