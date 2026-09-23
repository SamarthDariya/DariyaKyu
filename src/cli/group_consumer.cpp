#include "cli/group_consumer.hpp"

#include <unistd.h>

#include <map>
#include <utility>

#include "cli/assignor.hpp"
#include "common/buffer.hpp"
#include "common/errors.hpp"
#include "protocol/group_apis.hpp"
#include "protocol/metadata.hpp"

using namespace std;
using namespace dariyakyu::protocol;

namespace dariyakyu::cli {

namespace {

constexpr int kMaxRounds = 50;

template <typename Request, typename Encoder>
vector<uint8_t> body(const Request& request, Encoder encoder) {
    BufferWriter out;
    encoder(out, request);
    return out.take();
}

}  // namespace

GroupConsumer::GroupConsumer(Client& client, string groupId, vector<string> topics,
                             string strategy, int32_t sessionTimeoutMs)
    : client_(client),
      groupId_(std::move(groupId)),
      topics_(std::move(topics)),
      strategy_(std::move(strategy)),
      sessionTimeoutMs_(sessionTimeoutMs) {}

optional<vector<TopicPartition>> GroupConsumer::syncAsLeader(
    const vector<pair<string, vector<string>>>& members) {
    // The leader needs partition counts, which only Metadata knows. This is the
    // one place a client has to ask the broker something in order to compute an
    // assignment — and it asks for topics, not for an assignment.
    MetadataRequest metadata;
    metadata.topics = topics_;

    const auto   metadataBody = client_.call(ApiKey::Metadata, body(metadata, encodeMetadataRequest));
    BufferReader metadataIn(metadataBody);
    const auto   described = decodeMetadataResponse(metadataIn);

    AssignmentInput input;
    for (const auto& [memberId, subscription] : members) input.members[memberId] = subscription;
    for (const auto& topic : described.topics)
        if (topic.error == ErrorCode::None)
            input.partitionCounts[topic.name] = static_cast<int32_t>(topic.partitions.size());

    const auto computed =
        strategy_ == "roundrobin" ? assignRoundRobin(input) : assignRange(input);

    SyncGroupRequest sync;
    sync.groupId    = groupId_;
    sync.generation = generation_;
    sync.memberId   = memberId_;
    for (const auto& [memberId, assigned] : computed)
        sync.assignments[memberId] = encodeAssignment(assigned);

    const auto   syncBody = client_.call(ApiKey::SyncGroup, body(sync, encodeSyncGroupRequest));
    BufferReader syncIn(syncBody);
    const auto   result = decodeSyncGroupResponse(syncIn);

    if (result.error != ErrorCode::None) {
        forgetIfExpired(result.error);
        return nullopt;
    }

    return decodeAssignment(result.assignment);
}

void GroupConsumer::forgetIfExpired(ErrorCode error) {
    if (error == ErrorCode::UnknownMemberId) memberId_.clear();
}

vector<TopicPartition> GroupConsumer::join() {
    for (int round = 0; round < kMaxRounds; ++round) {
        JoinGroupRequest request;
        request.groupId          = groupId_;
        request.memberId         = memberId_;
        request.sessionTimeoutMs = sessionTimeoutMs_;
        request.subscription     = topics_;
        request.protocols        = {strategy_};

        const auto   joinBody = client_.call(ApiKey::JoinGroup, body(request, encodeJoinGroupRequest));
        BufferReader joinIn(joinBody);
        const auto   joined = decodeJoinGroupResponse(joinIn);

        // Keep whatever id the coordinator gave us, even on a retry — a member
        // that forgot it would be issued another and the group would grow a
        // phantom.
        if (!joined.memberId.empty()) memberId_ = joined.memberId;

        if (joined.error == ErrorCode::RebalanceInProgress) {
            // Not a failure. Others have not rejoined yet, and every member sees
            // this on the way into a stable generation.
            ::usleep(20 * 1000);
            continue;
        }

        if (joined.error == ErrorCode::UnknownMemberId) {
            // Expired while we were away. Start over with no id.
            forgetIfExpired(joined.error);
            continue;
        }

        if (joined.error != ErrorCode::None)
            throw Error(string("join failed: ") + describe(joined.error));

        generation_ = joined.generation;
        isLeader_   = joined.memberId == joined.leaderId;

        if (isLeader_) {
            vector<pair<string, vector<string>>> members;
            for (const auto& member : joined.members)
                members.emplace_back(member.memberId, member.subscription);
            if (auto assigned = syncAsLeader(members)) {
                assignment_ = *assigned;
                return assignment_;
            }
            ::usleep(20 * 1000);
            continue;
        }

        // A follower asks for its slice, and waits for the leader to supply one.
        for (int wait = 0; wait < kMaxRounds; ++wait) {
            SyncGroupRequest sync;
            sync.groupId    = groupId_;
            sync.generation = generation_;
            sync.memberId   = memberId_;

            const auto   syncBody = client_.call(ApiKey::SyncGroup,
                                                 body(sync, encodeSyncGroupRequest));
            BufferReader syncIn(syncBody);
            const auto   result = decodeSyncGroupResponse(syncIn);

            if (result.error == ErrorCode::RebalanceInProgress) {
                ::usleep(20 * 1000);
                continue;
            }
            if (result.error != ErrorCode::None) {
                forgetIfExpired(result.error);
                break;   // rejoin from the top
            }

            assignment_ = decodeAssignment(result.assignment);
            return assignment_;
        }
    }

    throw Error("group '" + groupId_ + "' never settled");
}

const vector<TopicPartition>& GroupConsumer::ensureJoined() {
    if (generation_ < 0 || !heartbeat()) join();
    return assignment_;
}

bool GroupConsumer::heartbeat() {
    HeartbeatRequest request;
    request.groupId    = groupId_;
    request.generation = generation_;
    request.memberId   = memberId_;

    const auto   responseBody = client_.call(ApiKey::Heartbeat,
                                             body(request, encodeHeartbeatRequest));
    BufferReader in(responseBody);
    const auto   response = decodeHeartbeatResponse(in);

    // Rebalancing, a stale generation and an expired member all mean the same
    // thing to a member: rejoin. Only the first is routine, but none of the three
    // is recoverable by carrying on.
    return response.error == ErrorCode::None;
}

void GroupConsumer::commit(const TopicPartition& tp, Offset nextOffset) {
    OffsetCommitRequest request;
    request.groupId    = groupId_;
    request.generation = generation_;
    request.memberId   = memberId_;
    request.topics.push_back({tp.topic, {{tp.partition, nextOffset, ""}}});

    const auto   responseBody = client_.call(ApiKey::OffsetCommit,
                                             body(request, encodeOffsetCommitRequest));
    BufferReader in(responseBody);
    const auto   response = decodeOffsetCommitResponse(in);

    const auto error = response.topics.at(0).partitions.at(0).error;
    if (error != ErrorCode::None) throw Error(string("commit failed: ") + describe(error));
}

optional<Offset> GroupConsumer::committed(const TopicPartition& tp) {
    OffsetFetchRequest request;
    request.groupId = groupId_;
    request.topics.push_back({tp.topic, {tp.partition}});

    const auto   responseBody = client_.call(ApiKey::OffsetFetch,
                                             body(request, encodeOffsetFetchRequest));
    BufferReader in(responseBody);
    const auto   response = decodeOffsetFetchResponse(in);

    const auto& partition = response.topics.at(0).partitions.at(0);
    if (partition.error != ErrorCode::None) return nullopt;

    // -1 means never committed, which is NOT offset zero. The caller decides what
    // to do instead, because only it knows its reset policy.
    if (partition.nextOffset < Offset(0)) return nullopt;
    return partition.nextOffset;
}

void GroupConsumer::leave() {
    if (memberId_.empty()) return;

    LeaveGroupRequest request;
    request.groupId  = groupId_;
    request.memberId = memberId_;

    // Leaving politely lets the rest of the group rebalance immediately instead
    // of waiting out a session timeout — seconds of a stalled group, for one
    // request on the way out.
    const auto responseBody = client_.call(ApiKey::LeaveGroup,
                                           body(request, encodeLeaveGroupRequest));
    (void)responseBody;

    memberId_.clear();
    generation_ = -1;
    isLeader_   = false;
    assignment_.clear();
}

}  // namespace dariyakyu::cli
