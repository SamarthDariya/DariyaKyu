#include "replication/replica_manager.hpp"

using namespace std;

namespace dariyakyu::replication {

ReplicaManager::ReplicaManager(NodeId self, int64_t lagTimeMaxMs)
    : self_(self), lagTimeMaxMs_(lagTimeMaxMs) {}

void ReplicaManager::track(storage::Log& log, int64_t nowMs) {
    const auto& policy = log.config().replication;
    if (!policy.replicated() || policy.replicas.front() != self_) return;

    lock_guard lock(mutex_);
    log.setHighWatermarkTracksLogEnd(false);
    led_.try_emplace(log.topicPartition(), Led{&log, PartitionReplicas(self_, policy.replicas,
                                                                       log.highWatermark(), nowMs,
                                                                       lagTimeMaxMs_)});
}

void ReplicaManager::forget(const TopicPartition& tp) {
    lock_guard lock(mutex_);
    led_.erase(tp);
}

void ReplicaManager::publishLocked(Led& led) {
    led.log->setHighWatermark(led.replicas.updateHighWatermark(led.log->logEndOffset()));
}

void ReplicaManager::recordFetch(const TopicPartition& tp, NodeId follower, Offset fetchOffset,
                                 int64_t nowMs) {
    lock_guard lock(mutex_);
    const auto found = led_.find(tp);
    if (found == led_.end()) return;   // not led here with followers: nothing to track
    found->second.replicas.recordFetch(follower, fetchOffset, nowMs);
    publishLocked(found->second);
}

void ReplicaManager::logAppended(const TopicPartition& tp) {
    lock_guard lock(mutex_);
    const auto found = led_.find(tp);
    if (found != led_.end()) publishLocked(found->second);
}

void ReplicaManager::shrinkInSync(int64_t nowMs) {
    lock_guard lock(mutex_);
    for (auto& [tp, led] : led_) {
        led.replicas.shrinkInSync(nowMs);
        publishLocked(led);
    }
}

optional<vector<NodeId>> ReplicaManager::inSync(const TopicPartition& tp) const {
    lock_guard lock(mutex_);
    const auto found = led_.find(tp);
    if (found == led_.end()) return nullopt;
    return found->second.replicas.inSync();
}

}  // namespace dariyakyu::replication
