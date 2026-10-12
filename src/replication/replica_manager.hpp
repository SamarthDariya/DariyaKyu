#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

#include "common/types.hpp"
#include "replication/partition_replicas.hpp"
#include "storage/log.hpp"

namespace dariyakyu::replication {

// Every partition this broker leads WITH followers, and the one place their
// high watermarks are moved (M7).
//
// The Log holds the watermark; a PartitionReplicas decides it. This keeps the
// two in step: every event that can change the decision — a follower fetching,
// the leader appending, a follower timing out — comes through here, and ends
// with Log::setHighWatermark.
//
// One mutex for every partition. A fetch handler holds it for a map lookup and a
// handful of comparisons, so contention is not a problem at M7's scale — and a
// lock per partition is M9's to want, once threads are not per connection.
class ReplicaManager {
public:
    explicit ReplicaManager(NodeId self,
                            std::int64_t lagTimeMaxMs = PartitionReplicas::kDefaultLagTimeMaxMs);

    // Starts tracking `log`, if this broker leads it and it has followers. Safe
    // to call for every log: one led alone, or followed here, is ignored.
    //
    // Tracking switches off the log's own watermark-follows-append rule, so from
    // here on only this class moves it.
    void track(storage::Log& log, std::int64_t nowMs);

    // Stops tracking. Must be called before the Log is destroyed — the manager
    // holds a pointer to it, as handlers do. Nothing removes partitions at M7.
    void forget(const TopicPartition& tp);

    // A follower fetched at `fetchOffset`: it holds everything below it.
    void recordFetch(const TopicPartition& tp, NodeId follower, Offset fetchOffset,
                     std::int64_t nowMs);

    // The leader appended. Needed when every follower has dropped out: the
    // leader alone is the in-sync set, and its writes are committed at once
    // rather than at the next fetch or timer.
    void logAppended(const TopicPartition& tp);

    // The timer's pass: drops followers that stopped fetching, everywhere, and
    // lets each watermark move past them.
    void shrinkInSync(std::int64_t nowMs);

    // The leader and its in-sync followers, or nothing for a partition not
    // tracked here.
    std::optional<std::vector<NodeId>> inSync(const TopicPartition& tp) const;

private:
    struct Led {
        storage::Log*     log;
        PartitionReplicas replicas;
    };

    // Recomputes and publishes one partition's watermark. Assumes mutex_ held.
    static void publishLocked(Led& led);

    NodeId       self_;
    std::int64_t lagTimeMaxMs_;

    mutable std::mutex                     mutex_;
    std::unordered_map<TopicPartition, Led> led_;
};

}  // namespace dariyakyu::replication
