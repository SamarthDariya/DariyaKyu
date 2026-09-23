#pragma once

#include <cstdint>
#include <string>

#include "common/types.hpp"
#include "storage/log_manager.hpp"

namespace dariyakyu::group {

// The internal topic offsets live in.
//
// An ordinary compacted topic, not a special store (DESIGN.md decision 21):
// offsets need durability, replication and recovery, which describes a log, and
// there is already one of those.
inline constexpr const char* kOffsetsTopic = "__offsets";

// Partitions of __offsets, and therefore the number of possible coordinators.
//
// IMMUTABLE once the topic exists. Changing it moves every group to a different
// coordinator AND orphans its committed offsets, which are keyed into the old
// partition — silently, because both the old and new partitions are perfectly
// valid logs. Kafka's default is 50; eight is plenty for one node and keeps a
// test's data directory small.
inline constexpr std::int32_t kDefaultOffsetsPartitions = 8;

// Which partition — and so which broker — coordinates `group`.
//
// hash(group) % N, as decision 21 says. Uses the same std::hash the partition
// registry uses, so there is one hashing story in the codebase rather than two.
PartitionId coordinatorPartition(const std::string& group, std::int32_t partitions);

// Creates __offsets if it is absent, and reports how many partitions it has.
//
// Nothing stores N: the number of partitions on disk IS N, discovered by the
// startup scan that already runs. Finding an existing topic with a different
// count than requested is REFUSED rather than adopted — silently accepting it
// would move every group's coordinator and orphan every committed offset.
std::int32_t ensureOffsetsTopic(storage::LogManager& logs, std::int32_t partitions);

}  // namespace dariyakyu::group
