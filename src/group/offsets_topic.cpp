#include "group/offsets_topic.hpp"

#include <functional>
#include <limits>
#include <optional>
#include <string>

#include "common/errors.hpp"

using namespace std;

namespace dariyakyu::group {

PartitionId coordinatorPartition(const string& group, int32_t partitions) {
    if (partitions <= 0)
        throw Error("coordinatorPartition: " + to_string(partitions) + " partitions");

    // Masked to 63 bits before the modulus. hash returns size_t, which is
    // unsigned, but casting to a signed PartitionId first could give a negative
    // remainder — and a negative partition number is not a partition.
    const size_t hashed = hash<string>{}(group);
    return static_cast<PartitionId>(hashed % static_cast<size_t>(partitions));
}

int32_t ensureOffsetsTopic(storage::LogManager& logs, int32_t partitions) {
    if (partitions <= 0)
        throw Error("__offsets: " + to_string(partitions) + " partitions requested");

    // Count what is already there. loadAll has run by now, so this sees whatever
    // a previous life of this broker created.
    int32_t existing = 0;
    for (const auto& tp : logs.hostedPartitions())
        if (tp.topic == kOffsetsTopic) ++existing;

    if (existing > 0) {
        // Refused rather than adopted or quietly corrected. Both counts describe
        // perfectly valid logs, so nothing downstream would notice — it would
        // just start answering FindCoordinator with different partitions and
        // reading offsets that are not there.
        if (existing != partitions)
            throw CorruptData("__offsets has " + to_string(existing) +
                              " partitions but " + to_string(partitions) +
                              " were requested — the count is immutable, because changing it "
                              "moves every group's coordinator and orphans its offsets");
        return existing;
    }

    // Compacted, which is the whole reason this topic has the shape it does.
    //
    // A group committing once a second writes 86,400 records a day for one key,
    // and all but the last is garbage the moment the next one lands. Compaction
    // keeps the newest value per key and keeps it forever, so the topic stays
    // proportional to the number of (group, topic, partition) triples rather than
    // to how often anybody commits.
    //
    // The retention policy is neutered rather than merely left alone. Deleting by
    // age from this topic would move a group's position BACKWARDS to whatever
    // older commit happened to survive, which is worse than losing it outright:
    // the group would silently reprocess, and nothing would report an error.
    storage::LogConfig config       = logs.defaults();
    config.cleanup                  = storage::CleanupPolicy::Compact;
    config.retention.retentionMs    = numeric_limits<int64_t>::max();
    config.retention.retentionBytes = nullopt;

    for (PartitionId p = 0; p < partitions; ++p)
        logs.createPartition(TopicPartition{kOffsetsTopic, p}, config);

    return partitions;
}

}  // namespace dariyakyu::group
