#include "replication/partition_replicas.hpp"

#include <algorithm>
#include <string>

#include "common/errors.hpp"

using namespace std;

namespace dariyakyu::replication {

PartitionReplicas::PartitionReplicas(NodeId leader, const vector<NodeId>& replicas,
                                     Offset highWatermark)
    : highWatermark_(highWatermark) {
    // Everyone but the leader. The leader's own log end is not tracked here — it
    // is the Log's, passed in fresh each time the watermark is recomputed.
    for (const NodeId node : replicas)
        if (node != leader) followers_.emplace(node, Follower{});
}

namespace {

[[noreturn]] void notAFollower(NodeId node) {
    throw Error("replication: node " + to_string(node) + " does not follow this partition");
}

}  // namespace

void PartitionReplicas::recordFetch(NodeId follower, Offset fetchOffset, int64_t nowMs) {
    const auto found = followers_.find(follower);
    if (found == followers_.end()) notAFollower(follower);
    found->second.logEnd      = fetchOffset;
    found->second.lastFetchMs = nowMs;
}

Offset PartitionReplicas::updateHighWatermark(Offset leaderLogEnd) {
    // The lowest log end anyone in the set has. Starting from the leader's means
    // a partition with no followers commits exactly what it has written.
    Offset lowest = leaderLogEnd;
    for (const auto& [node, state] : followers_) lowest = min(lowest, state.logEnd);

    highWatermark_ = max(highWatermark_, lowest);
    return highWatermark_;
}

Offset PartitionReplicas::followerLogEnd(NodeId follower) const {
    const auto found = followers_.find(follower);
    if (found == followers_.end()) notAFollower(follower);
    return found->second.logEnd;
}

}  // namespace dariyakyu::replication
