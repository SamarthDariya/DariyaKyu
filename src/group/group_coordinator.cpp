#include "group/group_coordinator.hpp"

#include <algorithm>
#include <utility>

using namespace std;
using namespace dariyakyu::protocol;

namespace dariyakyu::group {

const char* describe(GroupState state) {
    switch (state) {
        case GroupState::Empty: return "Empty";
        case GroupState::PreparingRebalance: return "PreparingRebalance";
        case GroupState::CompletingRebalance: return "CompletingRebalance";
        case GroupState::Stable: return "Stable";
    }
    return "unrecognised group state";
}

GroupCoordinator::Group& GroupCoordinator::groupFor(const string& groupId) {
    auto found = groups_.find(groupId);
    if (found != groups_.end()) return found->second;

    Group group;
    group.id = groupId;
    return groups_.emplace(groupId, std::move(group)).first->second;
}

void GroupCoordinator::beginRebalanceLocked(Group& group) {
    group.state = GroupState::PreparingRebalance;

    // Everyone must rejoin, including members that were perfectly happy. That is
    // what makes this stop-the-world: every member revokes everything and
    // processing halts until the new assignment lands. Decision 22's accepted
    // cost, and feeling it is the point.
    for (auto& [id, member] : group.members) member.rejoined = false;
}

void GroupCoordinator::maybeCompleteJoinLocked(Group& group) {
    if (group.members.empty()) {
        group.state = GroupState::Empty;
        group.leaderId.clear();
        return;
    }

    for (const auto& [id, member] : group.members)
        if (!member.rejoined) return;   // still waiting for somebody

    // Bumped HERE, when the membership is settled — not when the rebalance
    // started. A member still on the old generation is now stale by exactly one,
    // which is what IllegalGeneration detects.
    ++group.generation;

    // The first member by id, so the choice is stable across coordinators and
    // across restarts rather than depending on who happened to arrive first.
    group.leaderId = group.members.begin()->first;

    // The first strategy every member supports. Members that share none would be
    // an unsatisfiable group; the leader's own list wins by default because it is
    // the one doing the computing.
    group.protocolName.clear();
    for (const auto& candidate : group.members.begin()->second.protocols) {
        const bool everyone = all_of(group.members.begin(), group.members.end(),
                                     [&candidate](const auto& entry) {
                                         const auto& list = entry.second.protocols;
                                         return find(list.begin(), list.end(), candidate) !=
                                                list.end();
                                     });
        if (everyone) {
            group.protocolName = candidate;
            break;
        }
    }

    group.state = GroupState::CompletingRebalance;
}

JoinResult GroupCoordinator::resultFor(const Group& group, const string& memberId) const {
    JoinResult result;
    result.memberId     = memberId;
    result.generation   = group.generation;
    result.leaderId     = group.leaderId;
    result.protocolName = group.protocolName;

    // The member list goes ONLY to the leader. Everyone else has nothing to
    // compute with it, and sending it would tell every consumer who its peers are
    // for no reason.
    if (memberId == group.leaderId)
        for (const auto& [id, member] : group.members) result.members.push_back(member);

    return result;
}

JoinResult GroupCoordinator::join(const string& groupId, const string& memberId,
                                  const string& clientId, const vector<string>& subscription,
                                  const vector<string>& protocols, int32_t sessionTimeoutMs,
                                  int64_t nowMs) {
    lock_guard lock(mutex_);

    JoinResult rejected;
    if (groupId.empty()) {
        rejected.error = ErrorCode::InvalidGroupId;
        return rejected;
    }

    Group& group = groupFor(groupId);

    string id = memberId;
    if (id.empty()) {
        // Assigned by the coordinator, never chosen by the client: two consumers
        // that picked the same id would be indistinguishable, and one could
        // commit for partitions the other owns.
        id = clientId + "-" + to_string(nextMemberId_++);
    } else if (group.members.find(id) == group.members.end()) {
        // An id this group has never issued, or one that was expired while the
        // member was away. Either way it must start over.
        rejected.error = ErrorCode::UnknownMemberId;
        return rejected;
    }

    // A RETRY is not a new rebalance, and telling the two apart is what makes a
    // multi-member group settle at all.
    //
    // A member told RebalanceInProgress has to ask again to learn the outcome. If
    // that second ask started another rebalance — which it would, since the group
    // has by then moved on — every member would restart the cycle the moment the
    // previous one finished it, and a two-member group would never settle. So a
    // member that is already in the group and has already rejoined for the
    // rebalance in flight simply collects the result.
    //
    // Stable counts, not just CompletingRebalance, and that is the whole of the
    // rule rather than a widening of it. A member is told to retry, sleeps, and
    // asks again — and the leader may well have synced in the meantime, which puts
    // the group in Stable before the retry lands. Recognising the retry only in
    // CompletingRebalance makes settling a race the retrying member loses: it
    // starts a rebalance nobody else rejoins for, and waits for it forever.
    //
    // What distinguishes a retry from a real rejoin is that nothing it is asking
    // for has changed. A member that comes back with a different subscription is
    // a genuine event — it wants partitions it does not have — and must rebalance
    // however settled the group looks.
    const auto existing  = group.members.find(id);
    const bool settled   = group.state == GroupState::CompletingRebalance ||
                         group.state == GroupState::Stable;
    const bool unchanged = existing != group.members.end() &&
                           existing->second.subscription == subscription &&
                           existing->second.protocols == protocols;
    const bool isRetry = settled && unchanged && existing->second.rejoined;

    if (!isRetry && (group.state == GroupState::Stable || group.state == GroupState::Empty ||
                     group.state == GroupState::CompletingRebalance))
        beginRebalanceLocked(group);

    if (isRetry) {
        existing->second.lastHeartbeatMs = nowMs;
        return resultFor(group, id);
    }

    Member& member       = group.members[id];
    member.memberId      = id;
    member.clientId      = clientId;
    member.subscription  = subscription;
    member.protocols     = protocols;
    member.sessionTimeoutMs = sessionTimeoutMs > 0 ? sessionTimeoutMs : 10'000;
    member.lastHeartbeatMs  = nowMs;
    member.rejoined         = true;

    maybeCompleteJoinLocked(group);

    if (group.state == GroupState::PreparingRebalance) {
        // Still waiting for somebody else. Told to retry rather than parked,
        // because parking means outliving the thread that read the request.
        JoinResult waiting;
        waiting.error    = ErrorCode::RebalanceInProgress;
        waiting.memberId = id;   // so a retry can identify itself
        return waiting;
    }

    return resultFor(group, id);
}

SyncResult GroupCoordinator::sync(const string& groupId, const string& memberId,
                                  int32_t generation,
                                  const map<string, vector<uint8_t>>& assignments,
                                  int64_t nowMs) {
    lock_guard lock(mutex_);
    SyncResult result;

    const auto foundGroup = groups_.find(groupId);
    if (foundGroup == groups_.end()) {
        result.error = ErrorCode::UnknownMemberId;
        return result;
    }
    Group& group = foundGroup->second;

    const auto foundMember = group.members.find(memberId);
    if (foundMember == group.members.end()) {
        result.error = ErrorCode::UnknownMemberId;
        return result;
    }

    if (generation != group.generation) {
        // A zombie from an earlier generation. Without this it would happily
        // supply an assignment for partitions it no longer owns.
        result.error = ErrorCode::IllegalGeneration;
        return result;
    }

    if (group.state == GroupState::PreparingRebalance) {
        result.error = ErrorCode::RebalanceInProgress;
        return result;
    }

    foundMember->second.lastHeartbeatMs = nowMs;

    if (group.state == GroupState::CompletingRebalance) {
        if (memberId == group.leaderId) {
            // Stored as given and never parsed. A member named in the assignment
            // that is not in the group is ignored rather than refused: the leader
            // computed against the list it was handed, and a member can leave
            // between the two calls.
            for (const auto& [id, bytes] : assignments) {
                const auto member = group.members.find(id);
                if (member != group.members.end()) member->second.assignment = bytes;
            }
            group.state = GroupState::Stable;
        } else {
            // The leader has not synced yet. Retry.
            result.error = ErrorCode::RebalanceInProgress;
            return result;
        }
    }

    result.assignment = foundMember->second.assignment;
    return result;
}

ErrorCode GroupCoordinator::heartbeat(const string& groupId, const string& memberId,
                                      int32_t generation, int64_t nowMs) {
    lock_guard lock(mutex_);

    const auto foundGroup = groups_.find(groupId);
    if (foundGroup == groups_.end()) return ErrorCode::UnknownMemberId;
    Group& group = foundGroup->second;

    const auto foundMember = group.members.find(memberId);
    if (foundMember == group.members.end()) return ErrorCode::UnknownMemberId;

    if (generation != group.generation) return ErrorCode::IllegalGeneration;

    // The signal, not a failure. This is how a member LEARNS the group is
    // rebalancing — on the heartbeat it was making anyway, which is the reason
    // heartbeats exist rather than the coordinator simply timing members out.
    if (group.state == GroupState::PreparingRebalance) return ErrorCode::RebalanceInProgress;

    foundMember->second.lastHeartbeatMs = nowMs;
    return ErrorCode::None;
}

ErrorCode GroupCoordinator::leave(const string& groupId, const string& memberId, int64_t nowMs) {
    (void)nowMs;
    lock_guard lock(mutex_);

    const auto foundGroup = groups_.find(groupId);
    if (foundGroup == groups_.end()) return ErrorCode::UnknownMemberId;
    Group& group = foundGroup->second;

    if (group.members.erase(memberId) == 0) return ErrorCode::UnknownMemberId;

    // A departure changes the assignment, so the rest of the group has to
    // recompute — the same path a join takes.
    beginRebalanceLocked(group);
    maybeCompleteJoinLocked(group);

    if (group.members.empty()) groups_.erase(foundGroup);

    return ErrorCode::None;
}

size_t GroupCoordinator::expire(int64_t nowMs) {
    lock_guard lock(mutex_);
    size_t     expired = 0;

    for (auto groupIt = groups_.begin(); groupIt != groups_.end();) {
        Group& group = groupIt->second;

        bool lost = false;
        for (auto memberIt = group.members.begin(); memberIt != group.members.end();) {
            const auto& member = memberIt->second;
            if (nowMs - member.lastHeartbeatMs >= member.sessionTimeoutMs) {
                memberIt = group.members.erase(memberIt);
                ++expired;
                lost = true;
            } else {
                ++memberIt;
            }
        }

        if (lost) {
            // The reason this runs on a thread at all: a group whose members have
            // ALL vanished never sends another request, so nothing else would
            // ever notice, and its partitions would stay assigned to nobody.
            beginRebalanceLocked(group);
            maybeCompleteJoinLocked(group);
        }

        if (group.members.empty()) groupIt = groups_.erase(groupIt);
        else ++groupIt;
    }

    return expired;
}

GroupState GroupCoordinator::stateOf(const string& groupId) const {
    lock_guard lock(mutex_);
    const auto found = groups_.find(groupId);
    return found == groups_.end() ? GroupState::Empty : found->second.state;
}

size_t GroupCoordinator::memberCount(const string& groupId) const {
    lock_guard lock(mutex_);
    const auto found = groups_.find(groupId);
    return found == groups_.end() ? 0 : found->second.members.size();
}

size_t GroupCoordinator::groupCount() const {
    lock_guard lock(mutex_);
    return groups_.size();
}

}  // namespace dariyakyu::group
