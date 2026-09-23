#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "cli/client.hpp"
#include "common/types.hpp"
#include "protocol/error_codes.hpp"

namespace dariyakyu::cli {

// A member of a consumer group, from the client's side.
//
// The whole protocol dance lives here rather than in the broker, because the
// broker deliberately does not know what an assignment means. This is the part
// that does.
class GroupConsumer {
public:
    GroupConsumer(Client& client, std::string groupId, std::vector<std::string> topics,
                  std::string strategy = "range", std::int32_t sessionTimeoutMs = 10'000);

    // Joins or rejoins, and returns what this member now owns.
    //
    // Retries through RebalanceInProgress rather than treating it as a failure:
    // it is the coordinator saying "others have not rejoined yet", and every
    // member sees it on the way into a stable generation.
    std::vector<TopicPartition> join();

    // The assignment this member currently holds, rejoining if the group moved on.
    //
    // THE call a consumer makes every time round its loop, and join() is really
    // just its first iteration. Heartbeating is not optional bookkeeping: an eager
    // rebalance cannot complete until every member has rejoined, so a member that
    // stops asking stalls everyone else in its group until its session expires.
    // A consumer that joined once and then only fetched would be that member.
    const std::vector<TopicPartition>& ensureJoined();

    // Returns false when the group is rebalancing and this member must rejoin.
    // That is the ordinary way a member finds out, not an error.
    bool heartbeat();

    // The NEXT offset to read. Handle 500 through 509, commit 510.
    void commit(const TopicPartition& tp, Offset nextOffset);

    // Where this group left off, or nothing if it never committed here — which a
    // caller must not read as zero.
    std::optional<Offset> committed(const TopicPartition& tp);

    void leave();

    const std::string& memberId() const { return memberId_; }
    std::int32_t       generation() const { return generation_; }
    bool               isLeader() const { return isLeader_; }

private:
    // Nothing means "the group moved on, rejoin" rather than a failure.
    //
    // A leader's assignment is only valid for the generation it was computed in.
    // If a member joins between our JoinGroup and our SyncGroup the coordinator
    // has already started a new rebalance, and the assignment we are holding
    // describes a group that no longer exists — so it is discarded and the whole
    // round starts again. Retrying the sync would just resubmit a stale answer.
    std::optional<std::vector<TopicPartition>> syncAsLeader(
        const std::vector<std::pair<std::string, std::vector<std::string>>>& members);

    // Applies a sync or join error to our own state: an expired member has to
    // drop its id, or the coordinator would keep rejecting an id it has forgotten.
    void forgetIfExpired(protocol::ErrorCode error);

    Client&                  client_;
    std::string              groupId_;
    std::vector<std::string> topics_;
    std::string              strategy_;
    std::int32_t             sessionTimeoutMs_;

    std::string  memberId_;
    std::int32_t generation_ = -1;
    bool         isLeader_   = false;

    // What the last successful sync handed us. Cached so ensureJoined() can
    // return it on the common path, where nothing has changed.
    std::vector<TopicPartition> assignment_;
};

}  // namespace dariyakyu::cli
