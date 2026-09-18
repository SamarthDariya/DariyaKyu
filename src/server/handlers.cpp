#include "server/handlers.hpp"

#include <utility>

#include "common/errors.hpp"
#include "protocol/create_topic.hpp"
#include "protocol/list_offsets.hpp"
#include "protocol/metadata.hpp"

using namespace std;
using namespace dariyakyu::protocol;

namespace dariyakyu::server {

void handleListOffsets(RequestContext& request, Response& out) {
    const auto          decoded = decodeListOffsetsRequest(request.body);
    ListOffsetsResponse response;

    for (const auto& topic : decoded.topics) {
        ListOffsetsResponse::Topic answer;
        answer.name = topic.name;

        for (const auto& asked : topic.partitions) {
            ListOffsetsResponse::Partition result;
            result.partition = asked.partition;

            storage::Log* log =
                request.broker.logs.get(TopicPartition{topic.name, asked.partition});

            if (log == nullptr) {
                // Routine, not exceptional: a client with stale metadata, or a
                // partition that moved. It re-fetches Metadata and retries.
                result.error = ErrorCode::NotLeaderForPartition;
            } else if (asked.timestamp == kEarliestTimestamp) {
                // Rises as retention deletes segments, which is precisely why a
                // client asks after an OffsetOutOfRange.
                result.offset = log->logStartOffset();
            } else if (asked.timestamp == kLatestTimestamp) {
                result.offset = log->logEndOffset();
            } else {
                // Searching by a real timestamp would mean scanning segments by
                // largestTimestamp. Banked, and Unknown is the least wrong of
                // the codes that exist rather than a good one.
                result.error = ErrorCode::Unknown;
            }

            answer.partitions.push_back(result);
        }

        response.topics.push_back(std::move(answer));
    }

    BufferWriter buffer;
    encodeListOffsetsResponse(buffer, response);
    out.append(buffer.take());
}

void handleMetadata(RequestContext& request, Response& out) {
    const auto       decoded = decodeMetadataRequest(request.body);
    MetadataResponse response;

    response.brokers.push_back({request.broker.nodeId, request.broker.advertisedHost,
                                request.broker.advertisedPort});
    response.controllerId = -1;   // no controller until M8

    // Grouped here rather than by LogManager, which stores partitions flat: a
    // topic is a naming layer over partitions and nothing below this line knows
    // it exists.
    map<string, vector<PartitionId>> byTopic;
    for (const auto& tp : request.broker.logs.hostedPartitions())
        byTopic[tp.topic].push_back(tp.partition);

    const auto describe = [&](const string& name) {
        MetadataResponse::Topic topic;
        topic.name = name;

        const auto found = byTopic.find(name);
        if (found == byTopic.end()) {
            // Asked about by name and not here. Named back with an error rather
            // than omitted, so a client can tell "you do not have it" from "I
            // forgot to ask".
            topic.error = ErrorCode::UnknownTopicOrPartition;
            return topic;
        }

        for (const PartitionId partition : found->second)
            topic.partitions.push_back({ErrorCode::None, partition, request.broker.nodeId});
        return topic;
    };

    if (decoded.allTopics) {
        for (const auto& [name, partitions] : byTopic) response.topics.push_back(describe(name));
    } else {
        for (const auto& name : decoded.topics) response.topics.push_back(describe(name));
    }

    BufferWriter buffer;
    encodeMetadataResponse(buffer, response);
    out.append(buffer.take());
}

void handleCreateTopic(RequestContext& request, Response& out) {
    const auto          decoded = decodeCreateTopicRequest(request.body);
    CreateTopicResponse response;

    for (const auto& asked : decoded.topics) {
        CreateTopicResponse::Topic result;
        result.name = asked.name;

        if (!storage::isUsableTopicName(asked.name) || asked.partitionCount < 1) {
            // Checked before anything is created, because this is the boundary
            // where a client's string becomes a filesystem path.
            result.error = ErrorCode::InvalidTopic;
            response.topics.push_back(std::move(result));
            continue;
        }

        // Checked for EVERY partition first. Creating three of four and then
        // failing would leave a topic that half exists, which no error code
        // describes and no client could act on.
        bool exists = false;
        for (PartitionId p = 0; p < asked.partitionCount; ++p)
            if (request.broker.logs.get(TopicPartition{asked.name, p}) != nullptr) exists = true;

        if (exists) {
            result.error = ErrorCode::TopicAlreadyExists;
            response.topics.push_back(std::move(result));
            continue;
        }

        storage::LogConfig config = request.broker.logs.defaults();
        if (asked.retentionMs) config.retention.retentionMs = *asked.retentionMs;
        if (asked.retentionBytes)
            config.retention.retentionBytes = static_cast<uint64_t>(*asked.retentionBytes);
        if (asked.maxSegmentBytes)
            config.roll.maxSegmentBytes = static_cast<uint64_t>(*asked.maxSegmentBytes);

        try {
            for (PartitionId p = 0; p < asked.partitionCount; ++p)
                request.broker.logs.createPartition(TopicPartition{asked.name, p}, config);
        } catch (const Error&) {
            // A disk that filled, a permission that changed. Not the client's
            // doing, and there is no honest code for it.
            result.error = ErrorCode::Unknown;
        }

        response.topics.push_back(std::move(result));
    }

    BufferWriter buffer;
    encodeCreateTopicResponse(buffer, response);
    out.append(buffer.take());
}

void registerAllHandlers(ApiRegistry& registry) {
    registry.registerHandler(ApiKey::ListOffsets, 0, handleListOffsets);
    registry.registerHandler(ApiKey::Metadata, 0, handleMetadata);
    registry.registerHandler(ApiKey::CreateTopic, 0, handleCreateTopic);
}

}  // namespace dariyakyu::server
