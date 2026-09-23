#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "protocol/error_codes.hpp"

namespace dariyakyu::group {

// Where a group is in its lifecycle.
//
//        Empty ──join──▶ PreparingRebalance ──all rejoined──▶ CompletingRebalance
//          ▲                    ▲                                     │
//          │                    │                                leader syncs
//          └──last member──── Stable ◀───────────────────────────────┘
//                             leaves      join / leave / heartbeat expiry
enum class GroupState { Empty, PreparingRebalance, CompletingRebalance, Stable };

const char* describe(GroupState state);

// One consumer in a group.
struct Member {
    std::string              memberId;
    std::string              clientId;
    std::vector<std::string> subscription;   // topics it wants
    std::vector<std::string> protocols;      // assignment strategies it supports

    // Opaque to the coordinator, always. It stores these bytes and hands each
    // member its own, and never looks inside — which is the fifth appearance of
    // deliberate ignorance and the whole reason a new strategy can ship without
    // the broker knowing it exists.
    std::vector<std::uint8_t> assignment;

    std::int32_t sessionTimeoutMs = 10'000;
    std::int64_t lastHeartbeatMs  = 0;

    // Whether this member has rejoined for the rebalance in progress.
    bool rejoined = false;
};

// What a member learns from JoinGroup.
struct JoinResult {
    protocol::ErrorCode error      = protocol::ErrorCode::None;
    std::string         memberId;
    std::int32_t        generation = -1;
    std::string         leaderId;
    std::string         protocolName;

    // The full member list, and ONLY for the leader. Everyone else gets nothing,
    // because everyone else has nothing to compute.
    std::vector<Member> members;

    bool isLeader() const { return !members.empty(); }
};

struct SyncResult {
    protocol::ErrorCode       error = protocol::ErrorCode::None;
    std::vector<std::uint8_t> assignment;
};

// The group coordinator.
//
// NON-BLOCKING, and that is a deliberate departure from Kafka, which parks a
// JoinGroup until every member has rejoined or a timeout fires.
//
// Parking a request means it outlives the thread that read it, which is M9's
// architecture — and with thread-per-connection it would mean holding a thread
// per waiting member. So a member that joins while others have not yet rejoined
// is told RebalanceInProgress and retries. A polling rebalance rather than a
// blocking one: more round trips, no parked state, and the same outcome.
class GroupCoordinator {
public:
    // A member joining for the first time passes an empty memberId and is given
    // one. Every later request carries it, because a coordinator that trusted a
    // client-chosen id could not tell two consumers apart.
    JoinResult join(const std::string& groupId, const std::string& memberId,
                    const std::string& clientId, const std::vector<std::string>& subscription,
                    const std::vector<std::string>& protocols, std::int32_t sessionTimeoutMs,
                    std::int64_t nowMs);

    // The leader supplies an assignment for every member; everyone else supplies
    // nothing and collects their own slice.
    SyncResult sync(const std::string& groupId, const std::string& memberId,
                    std::int32_t generation,
                    const std::map<std::string, std::vector<std::uint8_t>>& assignments,
                    std::int64_t nowMs);

    // Liveness, and the channel a rebalance is announced on. A member learns that
    // the group is rebalancing by having its heartbeat answered with
    // RebalanceInProgress — which is why heartbeats exist at all rather than the
    // coordinator simply timing members out.
    protocol::ErrorCode heartbeat(const std::string& groupId, const std::string& memberId,
                                  std::int32_t generation, std::int64_t nowMs);

    protocol::ErrorCode leave(const std::string& groupId, const std::string& memberId,
                              std::int64_t nowMs);

    // Drops members that have stopped heartbeating, and rebalances what is left.
    // Returns how many were dropped.
    std::size_t expire(std::int64_t nowMs);

    GroupState  stateOf(const std::string& groupId) const;
    std::size_t memberCount(const std::string& groupId) const;
    std::size_t groupCount() const;

private:
    struct Group {
        std::string                  id;
        GroupState                   state      = GroupState::Empty;
        std::int32_t                 generation = 0;
        std::string                  leaderId;
        std::string                  protocolName;
        std::map<std::string, Member> members;   // ordered, so the leader is stable
    };

    // All of these assume mutex_ is held.
    Group& groupFor(const std::string& groupId);
    void   beginRebalanceLocked(Group& group);
    void   maybeCompleteJoinLocked(Group& group);
    JoinResult resultFor(const Group& group, const std::string& memberId) const;

    mutable std::mutex           mutex_;
    std::map<std::string, Group> groups_;
    std::int64_t                 nextMemberId_ = 1;
};

}  // namespace dariyakyu::group
