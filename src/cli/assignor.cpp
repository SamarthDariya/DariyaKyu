#include "cli/assignor.hpp"

#include <algorithm>
#include <utility>

#include "common/buffer.hpp"
#include "common/errors.hpp"
#include "protocol/wire.hpp"

using namespace std;

namespace dariyakyu::cli {

namespace {

constexpr int16_t kVersion = 1;

}  // namespace

vector<uint8_t> encodeAssignment(const vector<TopicPartition>& assigned) {
    // Grouped by topic, like everything else on this wire: a member holding
    // twelve partitions of one topic sends its name once.
    map<string, vector<PartitionId>> byTopic;
    for (const auto& tp : assigned) byTopic[tp.topic].push_back(tp.partition);

    BufferWriter out;
    out.writeInt16(kVersion);
    out.writeInt32(static_cast<int32_t>(byTopic.size()));
    for (const auto& [topic, partitions] : byTopic) {
        protocol::writeString(out, topic);
        out.writeInt32(static_cast<int32_t>(partitions.size()));
        for (const PartitionId partition : partitions) out.writeInt32(partition);
    }
    return out.take();
}

vector<TopicPartition> decodeAssignment(span<const uint8_t> bytes) {
    vector<TopicPartition> assigned;

    // An empty assignment is legal and common: a group with more members than
    // partitions leaves some of them holding nothing, and they heartbeat on
    // waiting for the next rebalance rather than being an error.
    if (bytes.empty()) return assigned;

    BufferReader in(bytes);

    const int16_t version = in.readInt16();
    if (version != kVersion)
        throw CorruptData("assignment: version " + to_string(version) + ", this client knows " +
                          to_string(kVersion));

    const int32_t topicCount = protocol::readElementCount(in, "topic");
    for (int32_t t = 0; t < topicCount; ++t) {
        const string  topic          = protocol::readString(in);
        const int32_t partitionCount = protocol::readElementCount(in, "partition");
        for (int32_t p = 0; p < partitionCount; ++p)
            assigned.push_back(TopicPartition{topic, in.readInt32()});
    }

    if (!in.empty())
        throw CorruptData("assignment: " + to_string(in.remaining()) + " trailing byte(s)");

    return assigned;
}

map<string, vector<TopicPartition>> assignRange(const AssignmentInput& input) {
    map<string, vector<TopicPartition>> assignment;
    for (const auto& [memberId, subscription] : input.members) assignment[memberId];

    for (const auto& [topic, count] : input.partitionCounts) {
        // Only members that asked for this topic. A member subscribed to nothing
        // in common with the rest still belongs to the group and still
        // heartbeats — it simply holds nothing.
        vector<string> interested;
        for (const auto& [memberId, subscription] : input.members)
            if (find(subscription.begin(), subscription.end(), topic) != subscription.end())
                interested.push_back(memberId);

        if (interested.empty() || count <= 0) continue;

        const int32_t each  = count / static_cast<int32_t>(interested.size());
        const int32_t extra = count % static_cast<int32_t>(interested.size());

        PartitionId next = 0;
        for (size_t i = 0; i < interested.size(); ++i) {
            // The remainder goes to the EARLIEST members, so the result depends
            // only on the sorted member list — not on who joined first, and not
            // on which coordinator computed it.
            const int32_t take = each + (static_cast<int32_t>(i) < extra ? 1 : 0);
            for (int32_t n = 0; n < take; ++n)
                assignment[interested[i]].push_back(TopicPartition{topic, next++});
        }
    }

    return assignment;
}

map<string, vector<TopicPartition>> assignRoundRobin(const AssignmentInput& input) {
    map<string, vector<TopicPartition>> assignment;
    for (const auto& [memberId, subscription] : input.members) assignment[memberId];

    // Every partition of every subscribed topic, in a stable order — the map is
    // ordered by topic and the partitions count up, so two leaders computing the
    // same group reach the same answer.
    vector<TopicPartition> all;
    for (const auto& [topic, count] : input.partitionCounts)
        for (PartitionId p = 0; p < count; ++p) all.push_back(TopicPartition{topic, p});

    vector<string> members;
    for (const auto& [memberId, subscription] : input.members) members.push_back(memberId);
    if (members.empty()) return assignment;

    size_t next = 0;
    for (const auto& tp : all) {
        // Deal it to the next member that actually wants this topic. Skipping the
        // uninterested rather than giving them a partition they never asked for,
        // which they would fetch and not understand.
        for (size_t tried = 0; tried < members.size(); ++tried) {
            const string& candidate    = members[(next + tried) % members.size()];
            const auto&   subscription = input.members.at(candidate);
            if (find(subscription.begin(), subscription.end(), tp.topic) != subscription.end()) {
                assignment[candidate].push_back(tp);
                next = (next + tried + 1) % members.size();
                break;
            }
        }
    }

    return assignment;
}

}  // namespace dariyakyu::cli
