#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "common/buffer.hpp"
#include "common/types.hpp"
#include "protocol/error_codes.hpp"

namespace dariyakyu::protocol {

// The seven consumer-group APIs, in one file because they are one conversation:
// find the coordinator, join, sync, heartbeat, commit, fetch offsets, leave.

// "Which broker coordinates this group?"
struct FindCoordinatorRequest {
    std::string groupId;
};

struct FindCoordinatorResponse {
    ErrorCode    error  = ErrorCode::None;
    NodeId       nodeId = -1;
    std::string  host;
    std::int32_t port = 0;
};

struct JoinGroupRequest {
    std::string  groupId;
    std::int32_t sessionTimeoutMs = 10'000;

    // Empty on a first join; the coordinator issues one. A client never chooses
    // its own, because two that collided would be indistinguishable.
    std::string memberId;

    std::vector<std::string> subscription;   // topics
    std::vector<std::string> protocols;      // assignment strategies it supports
};

struct JoinGroupResponse {
    struct MemberInfo {
        std::string              memberId;
        std::vector<std::string> subscription;
    };

    ErrorCode    error      = ErrorCode::None;
    std::int32_t generation = -1;
    std::string  protocolName;
    std::string  leaderId;
    std::string  memberId;

    // Populated only for the leader. Everyone else has nothing to compute.
    std::vector<MemberInfo> members;
};

struct SyncGroupRequest {
    std::string  groupId;
    std::int32_t generation = -1;
    std::string  memberId;

    // Supplied by the leader, empty from everyone else. Opaque bytes the broker
    // stores and hands back — never parses. That is what lets a new assignment
    // strategy ship without the broker knowing it exists.
    std::map<std::string, std::vector<std::uint8_t>> assignments;
};

struct SyncGroupResponse {
    ErrorCode                 error = ErrorCode::None;
    std::vector<std::uint8_t> assignment;
};

struct HeartbeatRequest {
    std::string  groupId;
    std::int32_t generation = -1;
    std::string  memberId;
};

struct HeartbeatResponse {
    ErrorCode error = ErrorCode::None;
};

struct LeaveGroupRequest {
    std::string groupId;
    std::string memberId;
};

struct LeaveGroupResponse {
    ErrorCode error = ErrorCode::None;
};

struct OffsetCommitRequest {
    struct Partition {
        PartitionId partition = 0;

        // The NEXT offset to read. Handle 500 through 509, commit 510.
        Offset      nextOffset{0};
        std::string metadata;
    };
    struct Topic {
        std::string            name;
        std::vector<Partition> partitions;
    };

    std::string  groupId;
    std::int32_t generation = -1;
    std::string  memberId;

    std::vector<Topic> topics;
};

struct OffsetCommitResponse {
    struct Partition {
        PartitionId partition = 0;
        ErrorCode   error     = ErrorCode::None;
    };
    struct Topic {
        std::string            name;
        std::vector<Partition> partitions;
    };

    std::vector<Topic> topics;
};

struct OffsetFetchRequest {
    struct Topic {
        std::string              name;
        std::vector<PartitionId> partitions;
    };

    std::string        groupId;
    std::vector<Topic> topics;
};

struct OffsetFetchResponse {
    struct Partition {
        PartitionId partition = 0;

        // -1 when this group has never committed here, which a consumer reads as
        // "start wherever your reset policy says" rather than "start at zero".
        // Those differ by an entire topic's worth of records.
        Offset      nextOffset{-1};
        std::string metadata;
        ErrorCode   error = ErrorCode::None;
    };
    struct Topic {
        std::string            name;
        std::vector<Partition> partitions;
    };

    std::vector<Topic> topics;
};

void                    encodeFindCoordinatorRequest(BufferWriter&, const FindCoordinatorRequest&);
FindCoordinatorRequest  decodeFindCoordinatorRequest(BufferReader&);
void                    encodeFindCoordinatorResponse(BufferWriter&, const FindCoordinatorResponse&);
FindCoordinatorResponse decodeFindCoordinatorResponse(BufferReader&);

void              encodeJoinGroupRequest(BufferWriter&, const JoinGroupRequest&);
JoinGroupRequest  decodeJoinGroupRequest(BufferReader&);
void              encodeJoinGroupResponse(BufferWriter&, const JoinGroupResponse&);
JoinGroupResponse decodeJoinGroupResponse(BufferReader&);

void              encodeSyncGroupRequest(BufferWriter&, const SyncGroupRequest&);
SyncGroupRequest  decodeSyncGroupRequest(BufferReader&);
void              encodeSyncGroupResponse(BufferWriter&, const SyncGroupResponse&);
SyncGroupResponse decodeSyncGroupResponse(BufferReader&);

void              encodeHeartbeatRequest(BufferWriter&, const HeartbeatRequest&);
HeartbeatRequest  decodeHeartbeatRequest(BufferReader&);
void              encodeHeartbeatResponse(BufferWriter&, const HeartbeatResponse&);
HeartbeatResponse decodeHeartbeatResponse(BufferReader&);

void               encodeLeaveGroupRequest(BufferWriter&, const LeaveGroupRequest&);
LeaveGroupRequest  decodeLeaveGroupRequest(BufferReader&);
void               encodeLeaveGroupResponse(BufferWriter&, const LeaveGroupResponse&);
LeaveGroupResponse decodeLeaveGroupResponse(BufferReader&);

void                 encodeOffsetCommitRequest(BufferWriter&, const OffsetCommitRequest&);
OffsetCommitRequest  decodeOffsetCommitRequest(BufferReader&);
void                 encodeOffsetCommitResponse(BufferWriter&, const OffsetCommitResponse&);
OffsetCommitResponse decodeOffsetCommitResponse(BufferReader&);

void                encodeOffsetFetchRequest(BufferWriter&, const OffsetFetchRequest&);
OffsetFetchRequest  decodeOffsetFetchRequest(BufferReader&);
void                encodeOffsetFetchResponse(BufferWriter&, const OffsetFetchResponse&);
OffsetFetchResponse decodeOffsetFetchResponse(BufferReader&);

}  // namespace dariyakyu::protocol
