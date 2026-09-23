#include "protocol/group_apis.hpp"

#include <utility>

#include "common/errors.hpp"
#include "protocol/wire.hpp"

using namespace std;

namespace dariyakyu::protocol {

namespace {

void writeStrings(BufferWriter& out, const vector<string>& values) {
    out.writeInt32(static_cast<int32_t>(values.size()));
    for (const auto& value : values) writeString(out, value);
}

vector<string> readStrings(BufferReader& in, const char* what) {
    const int32_t count = readElementCount(in, what);
    vector<string> values;
    values.reserve(static_cast<size_t>(count));
    for (int32_t i = 0; i < count; ++i) values.push_back(readString(in));
    return values;
}

void writeBytes(BufferWriter& out, const vector<uint8_t>& bytes) {
    out.writeInt32(static_cast<int32_t>(bytes.size()));
    out.writeBytes(bytes);
}

vector<uint8_t> readBytes(BufferReader& in, const char* what) {
    const int32_t length = in.readInt32();
    if (length < 0) throw CorruptData(string(what) + ": length " + to_string(length));
    const auto bytes = in.readBytes(static_cast<size_t>(length));
    return vector<uint8_t>(bytes.begin(), bytes.end());
}

}  // namespace

// --------------------------------------------------------------------------
// FindCoordinator
// --------------------------------------------------------------------------

void encodeFindCoordinatorRequest(BufferWriter& out, const FindCoordinatorRequest& request) {
    writeString(out, request.groupId);
}

FindCoordinatorRequest decodeFindCoordinatorRequest(BufferReader& in) {
    FindCoordinatorRequest request;
    request.groupId = readString(in);
    return request;
}

void encodeFindCoordinatorResponse(BufferWriter& out, const FindCoordinatorResponse& response) {
    out.writeInt16(static_cast<int16_t>(response.error));
    out.writeInt32(response.nodeId);
    writeString(out, response.host);
    out.writeInt32(response.port);
}

FindCoordinatorResponse decodeFindCoordinatorResponse(BufferReader& in) {
    FindCoordinatorResponse response;
    response.error  = static_cast<ErrorCode>(in.readInt16());
    response.nodeId = in.readInt32();
    response.host   = readString(in);
    response.port   = in.readInt32();
    return response;
}

// --------------------------------------------------------------------------
// JoinGroup
// --------------------------------------------------------------------------

void encodeJoinGroupRequest(BufferWriter& out, const JoinGroupRequest& request) {
    writeString(out, request.groupId);
    out.writeInt32(request.sessionTimeoutMs);
    writeString(out, request.memberId);
    writeStrings(out, request.subscription);
    writeStrings(out, request.protocols);
}

JoinGroupRequest decodeJoinGroupRequest(BufferReader& in) {
    JoinGroupRequest request;
    request.groupId          = readString(in);
    request.sessionTimeoutMs = in.readInt32();
    request.memberId         = readString(in);
    request.subscription     = readStrings(in, "subscription");
    request.protocols        = readStrings(in, "protocol");
    return request;
}

void encodeJoinGroupResponse(BufferWriter& out, const JoinGroupResponse& response) {
    out.writeInt16(static_cast<int16_t>(response.error));
    out.writeInt32(response.generation);
    writeString(out, response.protocolName);
    writeString(out, response.leaderId);
    writeString(out, response.memberId);

    out.writeInt32(static_cast<int32_t>(response.members.size()));
    for (const auto& member : response.members) {
        writeString(out, member.memberId);
        writeStrings(out, member.subscription);
    }
}

JoinGroupResponse decodeJoinGroupResponse(BufferReader& in) {
    JoinGroupResponse response;
    response.error        = static_cast<ErrorCode>(in.readInt16());
    response.generation   = in.readInt32();
    response.protocolName = readString(in);
    response.leaderId     = readString(in);
    response.memberId     = readString(in);

    const int32_t count = readElementCount(in, "member");
    response.members.reserve(static_cast<size_t>(count));
    for (int32_t i = 0; i < count; ++i) {
        JoinGroupResponse::MemberInfo member;
        member.memberId     = readString(in);
        member.subscription = readStrings(in, "subscription");
        response.members.push_back(std::move(member));
    }
    return response;
}

// --------------------------------------------------------------------------
// SyncGroup
// --------------------------------------------------------------------------

void encodeSyncGroupRequest(BufferWriter& out, const SyncGroupRequest& request) {
    writeString(out, request.groupId);
    out.writeInt32(request.generation);
    writeString(out, request.memberId);

    out.writeInt32(static_cast<int32_t>(request.assignments.size()));
    for (const auto& [memberId, assignment] : request.assignments) {
        writeString(out, memberId);
        writeBytes(out, assignment);
    }
}

SyncGroupRequest decodeSyncGroupRequest(BufferReader& in) {
    SyncGroupRequest request;
    request.groupId    = readString(in);
    request.generation = in.readInt32();
    request.memberId   = readString(in);

    const int32_t count = readElementCount(in, "assignment");
    for (int32_t i = 0; i < count; ++i) {
        const string memberId = readString(in);
        request.assignments[memberId] = readBytes(in, "assignment");
    }
    return request;
}

void encodeSyncGroupResponse(BufferWriter& out, const SyncGroupResponse& response) {
    out.writeInt16(static_cast<int16_t>(response.error));
    writeBytes(out, response.assignment);
}

SyncGroupResponse decodeSyncGroupResponse(BufferReader& in) {
    SyncGroupResponse response;
    response.error      = static_cast<ErrorCode>(in.readInt16());
    response.assignment = readBytes(in, "assignment");
    return response;
}

// --------------------------------------------------------------------------
// Heartbeat and LeaveGroup
// --------------------------------------------------------------------------

void encodeHeartbeatRequest(BufferWriter& out, const HeartbeatRequest& request) {
    writeString(out, request.groupId);
    out.writeInt32(request.generation);
    writeString(out, request.memberId);
}

HeartbeatRequest decodeHeartbeatRequest(BufferReader& in) {
    HeartbeatRequest request;
    request.groupId    = readString(in);
    request.generation = in.readInt32();
    request.memberId   = readString(in);
    return request;
}

void encodeHeartbeatResponse(BufferWriter& out, const HeartbeatResponse& response) {
    out.writeInt16(static_cast<int16_t>(response.error));
}

HeartbeatResponse decodeHeartbeatResponse(BufferReader& in) {
    HeartbeatResponse response;
    response.error = static_cast<ErrorCode>(in.readInt16());
    return response;
}

void encodeLeaveGroupRequest(BufferWriter& out, const LeaveGroupRequest& request) {
    writeString(out, request.groupId);
    writeString(out, request.memberId);
}

LeaveGroupRequest decodeLeaveGroupRequest(BufferReader& in) {
    LeaveGroupRequest request;
    request.groupId  = readString(in);
    request.memberId = readString(in);
    return request;
}

void encodeLeaveGroupResponse(BufferWriter& out, const LeaveGroupResponse& response) {
    out.writeInt16(static_cast<int16_t>(response.error));
}

LeaveGroupResponse decodeLeaveGroupResponse(BufferReader& in) {
    LeaveGroupResponse response;
    response.error = static_cast<ErrorCode>(in.readInt16());
    return response;
}

// --------------------------------------------------------------------------
// OffsetCommit and OffsetFetch
// --------------------------------------------------------------------------

void encodeOffsetCommitRequest(BufferWriter& out, const OffsetCommitRequest& request) {
    writeString(out, request.groupId);
    out.writeInt32(request.generation);
    writeString(out, request.memberId);

    out.writeInt32(static_cast<int32_t>(request.topics.size()));
    for (const auto& topic : request.topics) {
        writeString(out, topic.name);
        out.writeInt32(static_cast<int32_t>(topic.partitions.size()));
        for (const auto& partition : topic.partitions) {
            out.writeInt32(partition.partition);
            out.writeInt64(partition.nextOffset.value());
            writeString(out, partition.metadata);
        }
    }
}

OffsetCommitRequest decodeOffsetCommitRequest(BufferReader& in) {
    OffsetCommitRequest request;
    request.groupId    = readString(in);
    request.generation = in.readInt32();
    request.memberId   = readString(in);

    const int32_t topicCount = readElementCount(in, "topic");
    for (int32_t t = 0; t < topicCount; ++t) {
        OffsetCommitRequest::Topic topic;
        topic.name = readString(in);

        const int32_t partitionCount = readElementCount(in, "partition");
        for (int32_t p = 0; p < partitionCount; ++p) {
            OffsetCommitRequest::Partition partition;
            partition.partition  = in.readInt32();
            partition.nextOffset = Offset{in.readInt64()};

            // A negative committed offset is not a position a consumer reached.
            // Accepting one would make its next fetch ask for something before the
            // log began, and the group would silently restart from the earliest
            // record it could find.
            if (partition.nextOffset < Offset(0))
                throw CorruptData("offset commit: negative offset " +
                                  partition.nextOffset.toString());

            partition.metadata = readString(in);
            topic.partitions.push_back(std::move(partition));
        }
        request.topics.push_back(std::move(topic));
    }
    return request;
}

void encodeOffsetCommitResponse(BufferWriter& out, const OffsetCommitResponse& response) {
    out.writeInt32(static_cast<int32_t>(response.topics.size()));
    for (const auto& topic : response.topics) {
        writeString(out, topic.name);
        out.writeInt32(static_cast<int32_t>(topic.partitions.size()));
        for (const auto& partition : topic.partitions) {
            out.writeInt32(partition.partition);
            out.writeInt16(static_cast<int16_t>(partition.error));
        }
    }
}

OffsetCommitResponse decodeOffsetCommitResponse(BufferReader& in) {
    OffsetCommitResponse response;

    const int32_t topicCount = readElementCount(in, "topic");
    for (int32_t t = 0; t < topicCount; ++t) {
        OffsetCommitResponse::Topic topic;
        topic.name = readString(in);

        const int32_t partitionCount = readElementCount(in, "partition");
        for (int32_t p = 0; p < partitionCount; ++p) {
            OffsetCommitResponse::Partition partition;
            partition.partition = in.readInt32();
            partition.error     = static_cast<ErrorCode>(in.readInt16());
            topic.partitions.push_back(partition);
        }
        response.topics.push_back(std::move(topic));
    }
    return response;
}

void encodeOffsetFetchRequest(BufferWriter& out, const OffsetFetchRequest& request) {
    writeString(out, request.groupId);
    out.writeInt32(static_cast<int32_t>(request.topics.size()));
    for (const auto& topic : request.topics) {
        writeString(out, topic.name);
        out.writeInt32(static_cast<int32_t>(topic.partitions.size()));
        for (const PartitionId partition : topic.partitions) out.writeInt32(partition);
    }
}

OffsetFetchRequest decodeOffsetFetchRequest(BufferReader& in) {
    OffsetFetchRequest request;
    request.groupId = readString(in);

    const int32_t topicCount = readElementCount(in, "topic");
    for (int32_t t = 0; t < topicCount; ++t) {
        OffsetFetchRequest::Topic topic;
        topic.name = readString(in);

        const int32_t partitionCount = readElementCount(in, "partition");
        for (int32_t p = 0; p < partitionCount; ++p) topic.partitions.push_back(in.readInt32());
        request.topics.push_back(std::move(topic));
    }
    return request;
}

void encodeOffsetFetchResponse(BufferWriter& out, const OffsetFetchResponse& response) {
    out.writeInt32(static_cast<int32_t>(response.topics.size()));
    for (const auto& topic : response.topics) {
        writeString(out, topic.name);
        out.writeInt32(static_cast<int32_t>(topic.partitions.size()));
        for (const auto& partition : topic.partitions) {
            out.writeInt32(partition.partition);
            out.writeInt64(partition.nextOffset.value());
            writeString(out, partition.metadata);
            out.writeInt16(static_cast<int16_t>(partition.error));
        }
    }
}

OffsetFetchResponse decodeOffsetFetchResponse(BufferReader& in) {
    OffsetFetchResponse response;

    const int32_t topicCount = readElementCount(in, "topic");
    for (int32_t t = 0; t < topicCount; ++t) {
        OffsetFetchResponse::Topic topic;
        topic.name = readString(in);

        const int32_t partitionCount = readElementCount(in, "partition");
        for (int32_t p = 0; p < partitionCount; ++p) {
            OffsetFetchResponse::Partition partition;
            partition.partition  = in.readInt32();
            partition.nextOffset = Offset{in.readInt64()};
            partition.metadata   = readString(in);
            partition.error      = static_cast<ErrorCode>(in.readInt16());
            topic.partitions.push_back(std::move(partition));
        }
        response.topics.push_back(std::move(topic));
    }
    return response;
}

}  // namespace dariyakyu::protocol
