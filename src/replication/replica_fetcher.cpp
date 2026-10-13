#include "replication/replica_fetcher.hpp"

#include <algorithm>
#include <chrono>
#include <optional>
#include <utility>

#include "common/buffer.hpp"
#include "common/errors.hpp"
#include "protocol/fetch.hpp"
#include "storage/record_batch.hpp"

using namespace std;

namespace dariyakyu::replication {

FetchRound fetchOnce(protocol::Client& leader, storage::Log& log, NodeId self, int32_t maxBytes) {
    const TopicPartition& tp = log.topicPartition();

    protocol::FetchRequest request;
    request.replicaId = self;
    request.topics.push_back({tp.topic, {{tp.partition, log.logEndOffset(), maxBytes}}});

    BufferWriter out;
    protocol::encodeFetchRequest(out, request);
    const auto reply = leader.call(protocol::ApiKey::Fetch, out.take());

    // The view borrows from `reply`, which outlives every use of it below.
    BufferReader in(reply);
    const auto   response  = protocol::decodeFetchResponse(in);
    const auto&  partition = response.topics.at(0).partitions.at(0);

    FetchRound round;
    round.error = partition.error;
    if (round.error != protocol::ErrorCode::None) return round;

    // Walk the complete batches. A batch whose length runs past the end, or a
    // tail too short to hold a length at all, is the partial batch the fetch
    // contract allows — stop there, and the next round starts from it.
    span<const uint8_t> records = partition.records;
    while (!records.empty()) {
        size_t total = 0;
        try {
            total = storage::RecordBatch::totalSizeOf(records);
        } catch (const CorruptData&) {
            break;
        }
        if (total > records.size()) break;

        const auto batch = records.subspan(0, total);
        if (!storage::RecordBatch::verifyCrc(batch))
            throw CorruptData("replica fetch: a batch from the leader for " + tp.toString() +
                              " fails its checksum");

        log.appendAsFollower(storage::RecordBatch::parseHeader(batch).baseOffset, batch);
        ++round.batches;
        records = records.subspan(total);
    }

    log.setHighWatermark(min(partition.highWatermark, log.logEndOffset()));
    return round;
}

ReplicaFetcher::ReplicaFetcher(Options options, vector<storage::Log*> partitions)
    : options_(std::move(options)), partitions_(std::move(partitions)) {}

ReplicaFetcher::~ReplicaFetcher() {
    stop();
}

void ReplicaFetcher::start() {
    thread_ = thread([this] { run(); });
}

void ReplicaFetcher::stop() {
    {
        lock_guard lock(mutex_);
        stopping_ = true;
    }
    wake_.notify_all();
    if (thread_.joinable()) thread_.join();
}

bool ReplicaFetcher::waitFor(int64_t ms) {
    unique_lock lock(mutex_);
    return wake_.wait_for(lock, chrono::milliseconds(ms), [this] { return stopping_; });
}

void ReplicaFetcher::run() {
    optional<protocol::Client> leader;
    const string               clientId = "dariyakyu-replica-" + to_string(options_.self);

    while (true) {
        bool copied = false;
        bool failed = false;
        try {
            if (!leader)
                leader.emplace(options_.leaderHost, options_.leaderPort, clientId, options_.timeoutMs);

            for (storage::Log* log : partitions_) {
                const FetchRound round = fetchOnce(*leader, *log, options_.self);
                if (round.error != protocol::ErrorCode::None) failed = true;
                if (round.batches > 0) copied = true;
            }
        } catch (const exception&) {
            // Unreachable, timed out, hung up, or sent bytes that fail their
            // checksum — or a reply too malformed to index. Every exception, not
            // just Error: one escaping this thread would end the whole broker.
            // Reconnect from scratch next pass.
            leader.reset();
            failed = true;
        }

        if (failed) failures_.fetch_add(1, memory_order_acq_rel);
        const int64_t wait = failed ? options_.backoffMs : copied ? 0 : options_.idleWaitMs;
        if (waitFor(wait)) return;
    }
}

}  // namespace dariyakyu::replication
