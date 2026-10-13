#pragma once

#include <cstddef>
#include <cstdint>

#include "common/types.hpp"
#include "protocol/client.hpp"
#include "protocol/error_codes.hpp"
#include "storage/log.hpp"

namespace dariyakyu::replication {

// What one round of copying from the leader achieved.
struct FetchRound {
    protocol::ErrorCode error   = protocol::ErrorCode::None;
    std::size_t         batches = 0;   // appended to the follower's log
};

// One round of following: fetch `log`'s partition from its leader, starting at
// this log's end, and append every complete batch that comes back (M7).
//
// The follower is a consumer that happens to be a broker (DESIGN.md chunk 6):
// the same Fetch request, with replicaId set to `self` so the leader reads past
// its high watermark and counts this fetch as the follower's progress.
//
// Bytes go in exactly as they came out of the leader's log — appendAsFollower
// never restamps — so the two logs hold the same records at the same offsets.
// Each batch's CRC is checked first: these crossed a network, and a corrupted
// batch written here would be served to consumers after a failover.
//
// A response may end mid-batch (the fetch contract). The partial batch is left
// for the next round, which starts at this log's new end and fetches it whole.
//
// The leader's high watermark is adopted, capped at this log's own end: a
// follower cannot have committed records it does not hold yet.
//
// Throws on a transport failure or corrupt bytes. The caller — a thread, from
// the next step — decides whether to back off and retry.
FetchRound fetchOnce(protocol::Client& leader, storage::Log& log, NodeId self,
                     std::int32_t maxBytes = 1 << 20);

}  // namespace dariyakyu::replication
