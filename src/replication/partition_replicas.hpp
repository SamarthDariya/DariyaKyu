#pragma once

#include <cstdint>
#include <map>
#include <vector>

#include "common/types.hpp"

namespace dariyakyu::replication {

// The leader's view of one partition's followers: how far each has got, and so
// how far the partition is committed (DESIGN.md Part II chunk 6).
//
// Fed by the Fetch handler. A follower fetching at offset 400 is saying "I have
// everything below 400" — there is no separate acknowledgement — so its fetch
// offset IS its log end offset, as far as the leader can know.
//
// Plain state, no locking and no clock of its own: times are passed in, which is
// what lets the tests drive it with a fake one. The ReplicaManager that owns one
// per led partition (step 15) does the locking.
//
// Every follower counts as in sync for now. Shrinking the set when one stops
// fetching comes next (step 14), and is the reason recordFetch takes a time.
class PartitionReplicas {
public:
    // `replicas` as partition.meta recorded them, leader included. The watermark
    // starts where the log's already is, so taking leadership never lowers it.
    PartitionReplicas(NodeId leader, const std::vector<NodeId>& replicas, Offset highWatermark);

    // `follower` fetched at `fetchOffset`. Throws Error for a node that is not a
    // follower of this partition — the handler checked already, so reaching here
    // with one is a bug, not a client mistake.
    void recordFetch(NodeId follower, Offset fetchOffset, std::int64_t nowMs);

    // Recomputes the watermark — the lowest log end across leader and followers
    // — and returns it. Never lower than before: a follower's fetch offset can go
    // DOWN (it restarted, or truncated), and a consumer must still never see the
    // log move backwards.
    Offset updateHighWatermark(Offset leaderLogEnd);

    Offset highWatermark() const { return highWatermark_; }

    // What the leader last heard from `follower`. Offset 0 before its first fetch.
    Offset followerLogEnd(NodeId follower) const;

private:
    struct Follower {
        Offset       logEnd{0};
        std::int64_t lastFetchMs = -1;   // never fetched
    };

    std::map<NodeId, Follower> followers_;
    Offset                     highWatermark_;
};

}  // namespace dariyakyu::replication
