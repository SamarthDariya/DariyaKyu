#include "replication/partition_replicas.hpp"

#include <algorithm>
#include <string>

#include "common/errors.hpp"

using namespace std;

namespace dariyakyu::replication {

PartitionReplicas::PartitionReplicas(NodeId leader, const vector<NodeId>& replicas,
                                     Offset highWatermark, int64_t nowMs, int64_t lagTimeMaxMs)
    : leader_(leader), lagTimeMaxMs_(lagTimeMaxMs), highWatermark_(highWatermark) {
    // Everyone but the leader. The leader's own log end is not tracked here — it
    // is the Log's, passed in fresh each time the watermark is recomputed.
    for (const NodeId node : replicas)
        if (node != leader) followers_.emplace(node, Follower{Offset{0}, nowMs, true});
}

namespace {

[[noreturn]] void notAFollower(NodeId node) {
    throw Error("replication: node " + to_string(node) + " does not follow this partition");
}

}  // namespace

void PartitionReplicas::recordFetch(NodeId follower, Offset fetchOffset, int64_t nowMs) {
    const auto found = followers_.find(follower);
    if (found == followers_.end()) notAFollower(follower);
    Follower& state   = found->second;
    state.logEnd      = fetchOffset;
    state.lastFetchMs = nowMs;

    // Back in once it holds everything committed. Below the watermark it would
    // drag the minimum down; at or above it, it cannot.
    if (!state.inSync && fetchOffset >= highWatermark_) state.inSync = true;
}

vector<NodeId> PartitionReplicas::shrinkInSync(int64_t nowMs) {
    vector<NodeId> dropped;
    for (auto& [node, state] : followers_) {
        if (state.inSync && nowMs - state.lastFetchMs > lagTimeMaxMs_) {
            state.inSync = false;
            dropped.push_back(node);
        }
    }
    return dropped;
}

vector<NodeId> PartitionReplicas::inSync() const {
    vector<NodeId> nodes{leader_};
    for (const auto& [node, state] : followers_)
        if (state.inSync) nodes.push_back(node);
    return nodes;
}

Offset PartitionReplicas::updateHighWatermark(Offset leaderLogEnd) {
    // The lowest log end anyone in sync has. Starting from the leader's means a
    // partition with no in-sync followers commits exactly what it has written.
    Offset lowest = leaderLogEnd;
    for (const auto& [node, state] : followers_)
        if (state.inSync) lowest = min(lowest, state.logEnd);

    highWatermark_ = max(highWatermark_, lowest);
    return highWatermark_;
}

Offset PartitionReplicas::followerLogEnd(NodeId follower) const {
    const auto found = followers_.find(follower);
    if (found == followers_.end()) notAFollower(follower);
    return found->second.logEnd;
}

}  // namespace dariyakyu::replication
