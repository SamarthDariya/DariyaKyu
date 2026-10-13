#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

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
// Throws on a transport failure or corrupt bytes. The caller — ReplicaFetcher,
// below — decides whether to back off and retry.
FetchRound fetchOnce(protocol::Client& leader, storage::Log& log, NodeId self,
                     std::int32_t maxBytes = 1 << 20);

// A thread that keeps a set of partitions copied from one leader (M7).
//
// Pull, never push (DESIGN.md chunk 6): the follower knows its own log end, so
// it asks from there, and the leader tracks nothing but the offsets it is told.
//
// The loop decides how long to wait from what the last pass did:
//   copied something   go again at once — it is behind, catching up
//   copied nothing     wait idleWaitMs, so a quiet partition is not polled hot
//   failed             wait backoffMs, and reconnect next pass
//
// "Failed" covers an unreachable or silent leader (the client's timeout), bytes
// that fail their checksum, and an error code in the reply. The client is thrown
// away on any exception — after a timeout its stream may be out of step.
//
// One Fetch per partition per pass. Fetch takes many partitions in one request,
// and batching them is an optimisation this does not need yet.
class ReplicaFetcher {
public:
    struct Options {
        NodeId       self = 0;
        std::string  leaderHost;
        std::int32_t leaderPort  = 0;
        int          timeoutMs   = 5'000;
        std::int64_t idleWaitMs  = 100;
        std::int64_t backoffMs   = 500;
    };

    // `partitions` must outlive the fetcher, as with ReplicaManager.
    ReplicaFetcher(Options options, std::vector<storage::Log*> partitions);
    ~ReplicaFetcher();

    ReplicaFetcher(const ReplicaFetcher&)            = delete;
    ReplicaFetcher& operator=(const ReplicaFetcher&) = delete;

    void start();

    // Returns once the thread has exited, even if it was mid-wait. A fetch in
    // flight finishes first, bounded by the client's timeout.
    void stop();

    // Passes that failed, for tests and, later, for an operator to look at.
    std::uint64_t failures() const { return failures_.load(std::memory_order_acquire); }

private:
    void run();

    // Sleeps up to `ms`, waking early for stop(). True if stopping.
    bool waitFor(std::int64_t ms);

    Options                    options_;
    std::vector<storage::Log*> partitions_;

    std::thread                thread_;
    std::mutex                 mutex_;
    std::condition_variable    wake_;
    bool                       stopping_ = false;
    std::atomic<std::uint64_t> failures_{0};
};

}  // namespace dariyakyu::replication
