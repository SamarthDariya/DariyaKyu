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
// # The in-sync set
//
// A follower is IN SYNC while it keeps fetching: no more than
// replicaLagTimeMaxMs since its last fetch. A time, not a distance in records,
// and that is load-bearing — a distance rule ejects a healthy follower during a
// burst, exactly when the cluster can least afford to lose one, while a time
// rule keeps a follower that is far behind but catching up and drops one that
// has stopped.
//
// Only the in-sync set counts toward the watermark. That is the whole point of
// having one: a single dead follower must not freeze every consumer.
//
// A dropped follower rejoins when a fetch shows it has reached the watermark —
// it holds everything committed, so counting it again cannot pull the watermark
// down.
class PartitionReplicas {
public:
    // Kafka's default. Long enough to ride out a GC pause or a slow disk.
    static constexpr std::int64_t kDefaultLagTimeMaxMs = 30'000;

    // `replicas` as partition.meta recorded them, leader included. The watermark
    // starts where the log's already is, so taking leadership never lowers it.
    //
    // Every follower starts in sync, as if it had fetched at `nowMs`: a leader
    // that has only just started tracking a follower owes it a full lag window
    // before calling it dead.
    PartitionReplicas(NodeId leader, const std::vector<NodeId>& replicas, Offset highWatermark,
                      std::int64_t nowMs, std::int64_t lagTimeMaxMs = kDefaultLagTimeMaxMs);

    // `follower` fetched at `fetchOffset`. Throws Error for a node that is not a
    // follower of this partition — the handler checked already, so reaching here
    // with one is a bug, not a client mistake.
    void recordFetch(NodeId follower, Offset fetchOffset, std::int64_t nowMs);

    // Drops every follower that has not fetched within the lag window, and
    // returns the ones dropped. Called on a timer (step 16) as well as on fetch:
    // a dead follower never fetches, so it would never be noticed otherwise.
    std::vector<NodeId> shrinkInSync(std::int64_t nowMs);

    // The leader, then every follower currently in sync.
    std::vector<NodeId> inSync() const;

    // Recomputes the watermark — the lowest log end across the leader and the
    // in-sync followers — and returns it. Never lower than before: a follower's fetch offset can go
    // DOWN (it restarted, or truncated), and a consumer must still never see the
    // log move backwards.
    Offset updateHighWatermark(Offset leaderLogEnd);

    Offset highWatermark() const { return highWatermark_; }

    // What the leader last heard from `follower`. Offset 0 before its first fetch.
    Offset followerLogEnd(NodeId follower) const;

private:
    struct Follower {
        Offset       logEnd{0};
        std::int64_t lastFetchMs = 0;
        bool         inSync      = true;
    };

    NodeId                     leader_;
    std::int64_t               lagTimeMaxMs_;
    std::map<NodeId, Follower> followers_;
    Offset                     highWatermark_;
};

}  // namespace dariyakyu::replication
