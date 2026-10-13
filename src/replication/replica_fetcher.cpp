#include "replication/replica_fetcher.hpp"

#include <algorithm>

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

}  // namespace dariyakyu::replication
