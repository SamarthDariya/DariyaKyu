#pragma once

#include <cstdint>
#include <map>
#include <span>
#include <string>
#include <vector>

#include "common/types.hpp"

namespace dariyakyu::cli {

// Which partitions a member should read.
//
// CLIENT-SIDE, and that is decision 22's whole point. Assignment strategies are
// application-specific and unpredictable — range, round-robin, sticky,
// rack-aware, capacity-weighted, co-partitioned. Broker-side logic would mean a
// cluster upgrade and a rolling restart for every new one, coordinated with every
// team using it. Here, a strategy is a class in one application and rolling it
// out is one deploy.
//
// It works because the broker does not need to UNDERSTAND an assignment. It
// relays opaque bytes, which is why this codec lives with the client and not with
// the protocol.
std::vector<std::uint8_t> encodeAssignment(const std::vector<TopicPartition>& assigned);
std::vector<TopicPartition> decodeAssignment(std::span<const std::uint8_t> bytes);

// What the leader is given: who is in the group, and how many partitions each
// subscribed topic has.
struct AssignmentInput {
    // Member ids, and what each subscribed to.
    std::map<std::string, std::vector<std::string>> members;

    // Topic name to partition count, from Metadata.
    std::map<std::string, std::int32_t> partitionCounts;
};

// Contiguous blocks, per topic.
//
// Three partitions between two members gives the first two and the second one —
// the extra always goes to the earlier member, so the result depends only on the
// sorted member list and not on who joined first. With several topics of the same
// shape, the same member gets partition 0 of each, which is what makes range the
// strategy for co-partitioned joins.
std::map<std::string, std::vector<TopicPartition>> assignRange(const AssignmentInput& input);

// One partition at a time, round the members.
//
// Spreads more evenly than range when partition counts do not divide, at the cost
// of the co-partitioning property above: a member holding partition 0 of one
// topic may hold partition 1 of the next.
std::map<std::string, std::vector<TopicPartition>> assignRoundRobin(
    const AssignmentInput& input);

}  // namespace dariyakyu::cli
