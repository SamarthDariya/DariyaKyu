#include "server/handlers.hpp"

#include <utility>

#include "common/errors.hpp"
#include "protocol/create_topic.hpp"
#include "group/offsets_topic.hpp"
#include "protocol/fetch.hpp"
#include "protocol/group_apis.hpp"
#include "protocol/list_offsets.hpp"
#include "protocol/metadata.hpp"
#include "protocol/produce.hpp"
#include "storage/record_batch.hpp"

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
        for (const auto& [name, partitions] : byTopic) {
            // Internal topics are excluded from "everything" but answered when
            // asked for by name — Kafka's rule, and the right one: a client
            // subscribing to every topic should not find itself consuming the
            // offsets of every other consumer.
            if (name.rfind("__", 0) == 0) continue;
            response.topics.push_back(describe(name));
        }
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
        if (asked.compact)
            config.cleanup = *asked.compact ? storage::CleanupPolicy::Compact
                                            : storage::CleanupPolicy::Delete;

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

void handleProduce(RequestContext& request, Response& out) {
    const auto      decoded = decodeProduceRequest(request.body);
    ProduceResponse response;

    for (const auto& topic : decoded.topics) {
        ProduceResponse::Topic answer;
        answer.name = topic.name;

        for (const auto& asked : topic.partitions) {
            ProduceResponse::Partition result;
            result.partition  = asked.partition;
            result.baseOffset = kUnknownOffset;

            storage::Log* log =
                request.broker.logs.get(TopicPartition{topic.name, asked.partition});
            if (log == nullptr) {
                result.error = ErrorCode::NotLeaderForPartition;
                answer.partitions.push_back(result);
                continue;
            }

            try {
                // A mutable view of bytes still sitting in the request frame.
                // Log::append stamps the assigned offset into them, in place —
                // the only write the broker ever makes to a producer's bytes,
                // and the reason nothing here copies a batch.
                auto batch = mutableBatch(asked, request.frame);

                // Checked before appending, because a batch that fails its own
                // checksum must not reach a segment: recovery would find it
                // later and truncate everything after it.
                if (!storage::RecordBatch::verifyCrc(batch)) {
                    result.error = ErrorCode::CorruptMessage;
                } else {
                    result.baseOffset = log->append(batch);
                }
            } catch (const CorruptData&) {
                // The CLIENT's bytes did not parse. Its fault, and it should be
                // told precisely — which is why there is no general
                // errorCodeFor(const Error&): the same exception from a Fetch
                // means something else entirely.
                result.error = ErrorCode::CorruptMessage;
            } catch (const Error&) {
                // A full disk, a broken invariant. Not the client's doing, and
                // no code describes it honestly.
                result.error = ErrorCode::Unknown;
            }

            answer.partitions.push_back(result);
        }

        response.topics.push_back(std::move(answer));
    }

    BufferWriter buffer;
    encodeProduceResponse(buffer, response);
    out.append(buffer.take());
}

void handleFetch(RequestContext& request, Response& out) {
    const auto    decoded = decodeFetchRequest(request.body);
    FetchResponse response;

    for (const auto& topic : decoded.topics) {
        FetchResponse::Topic answer;
        answer.name = topic.name;

        for (const auto& asked : topic.partitions) {
            FetchResponse::Partition result;
            result.partition = asked.partition;

            storage::Log* log =
                request.broker.logs.get(TopicPartition{topic.name, asked.partition});
            if (log == nullptr) {
                result.error = ErrorCode::NotLeaderForPartition;
                answer.partitions.push_back(result);
                continue;
            }

            // Sent whether or not the read succeeded. The wire has one
            // OffsetOutOfRange code, and these two numbers are how a client tells
            // "you were too slow" from "the log was truncated under you".
            result.highWatermark  = log->highWatermark();
            result.logStartOffset = log->logStartOffset();

            try {
                const storage::ReadResult read =
                    log->read(asked.fetchOffset, static_cast<size_t>(asked.maxBytes));

                result.error = errorCodeFor(read.error);

                // A LOCATION, not bytes. These never enter this process: the
                // response encoder appends them as a file segment and sendfile
                // serves them from the kernel's page cache.
                if (read.ok()) result.records = read.range;
            } catch (const Error&) {
                // Including CorruptData, and deliberately NOT CorruptMessage:
                // our files are damaged, and telling the client its message was
                // corrupt would send it chasing a bug it does not have.
                result.error   = ErrorCode::Unknown;
                result.records = FileRange{};
            }

            answer.partitions.push_back(result);
        }

        response.topics.push_back(std::move(answer));
    }

    // Straight into the Response, so record bytes stay where they are.
    encodeFetchResponse(out, response);
}

// --------------------------------------------------------------------------
// Consumer groups
// --------------------------------------------------------------------------

namespace {

// Appends `response` encoded by `encoder`.
template <typename Response, typename Encoder>
void reply(protocol::Response& out, const Response& response, Encoder encoder) {
    BufferWriter buffer;
    encoder(buffer, response);
    out.append(buffer.take());
}

}  // namespace

void handleFindCoordinator(RequestContext& request, Response& out) {
    const auto              decoded = decodeFindCoordinatorRequest(request.body);
    FindCoordinatorResponse response;

    if (decoded.groupId.empty()) {
        response.error = ErrorCode::InvalidGroupId;
        reply(out, response, encodeFindCoordinatorResponse);
        return;
    }

    // hash(group) %% N names a partition of __offsets, and whoever leads it is the
    // coordinator. No new election and no new failover path — partition
    // leadership already does both.
    const PartitionId partition =
        group::coordinatorPartition(decoded.groupId, request.broker.offsetsPartitions);

    if (request.broker.logs.get(TopicPartition{group::kOffsetsTopic, partition}) == nullptr) {
        // __offsets is not ready. A client retries rather than giving up, because
        // this is a startup race and not a permanent condition.
        response.error = ErrorCode::CoordinatorNotAvailable;
        reply(out, response, encodeFindCoordinatorResponse);
        return;
    }

    // One node, so it is always this one. A client reads it rather than assuming,
    // so M8 can move coordinators without the client changing.
    response.nodeId = request.broker.nodeId;
    response.host   = request.broker.advertisedHost;
    response.port   = request.broker.advertisedPort;
    reply(out, response, encodeFindCoordinatorResponse);
}

void handleJoinGroup(RequestContext& request, Response& out) {
    const auto decoded = decodeJoinGroupRequest(request.body);

    const auto joined = request.broker.groups.join(
        decoded.groupId, decoded.memberId, request.header.clientId, decoded.subscription,
        decoded.protocols, decoded.sessionTimeoutMs, storage::wallClockMillis());

    JoinGroupResponse response;
    response.error        = joined.error;
    response.generation   = joined.generation;
    response.protocolName = joined.protocolName;
    response.leaderId     = joined.leaderId;
    response.memberId     = joined.memberId;

    for (const auto& member : joined.members)
        response.members.push_back({member.memberId, member.subscription});

    reply(out, response, encodeJoinGroupResponse);
}

void handleSyncGroup(RequestContext& request, Response& out) {
    const auto decoded = decodeSyncGroupRequest(request.body);

    const auto synced =
        request.broker.groups.sync(decoded.groupId, decoded.memberId, decoded.generation,
                                   decoded.assignments, storage::wallClockMillis());

    SyncGroupResponse response;
    response.error      = synced.error;
    response.assignment = synced.assignment;
    reply(out, response, encodeSyncGroupResponse);
}

void handleHeartbeat(RequestContext& request, Response& out) {
    const auto decoded = decodeHeartbeatRequest(request.body);

    HeartbeatResponse response;
    response.error = request.broker.groups.heartbeat(decoded.groupId, decoded.memberId,
                                                     decoded.generation,
                                                     storage::wallClockMillis());
    reply(out, response, encodeHeartbeatResponse);
}

void handleLeaveGroup(RequestContext& request, Response& out) {
    const auto decoded = decodeLeaveGroupRequest(request.body);

    LeaveGroupResponse response;
    response.error = request.broker.groups.leave(decoded.groupId, decoded.memberId,
                                                 storage::wallClockMillis());
    reply(out, response, encodeLeaveGroupResponse);
}

void handleOffsetCommit(RequestContext& request, Response& out) {
    const auto           decoded = decodeOffsetCommitRequest(request.body);
    OffsetCommitResponse response;

    // A member of a group must prove it is current; a standalone consumer — no
    // member id, no generation — is committing on its own behalf and has nothing
    // to prove. Kafka allows both, and refusing the second would mean a consumer
    // could not track its position without joining a group it does not need.
    ErrorCode membership = ErrorCode::None;
    if (!decoded.memberId.empty() || decoded.generation >= 0) {
        membership = request.broker.groups.heartbeat(decoded.groupId, decoded.memberId,
                                                     decoded.generation,
                                                     storage::wallClockMillis());
    }

    for (const auto& topic : decoded.topics) {
        OffsetCommitResponse::Topic answer;
        answer.name = topic.name;

        for (const auto& asked : topic.partitions) {
            OffsetCommitResponse::Partition result;
            result.partition = asked.partition;

            if (membership != ErrorCode::None) {
                // A zombie from an earlier generation would otherwise move a
                // position that another member now owns — backwards, silently.
                result.error = membership;
                answer.partitions.push_back(result);
                continue;
            }

            try {
                group::CommitValue value;
                value.nextOffset        = asked.nextOffset;
                value.metadata          = asked.metadata;
                value.commitTimestampMs = storage::wallClockMillis();

                request.broker.offsets.commit(
                    {decoded.groupId, topic.name, asked.partition}, value);
            } catch (const Error&) {
                result.error = ErrorCode::Unknown;
            }

            answer.partitions.push_back(result);
        }

        response.topics.push_back(std::move(answer));
    }

    reply(out, response, encodeOffsetCommitResponse);
}

void handleOffsetFetch(RequestContext& request, Response& out) {
    const auto          decoded = decodeOffsetFetchRequest(request.body);
    OffsetFetchResponse response;

    // No membership check. A consumer asking where it left off has nothing to
    // prove — it is reading its own past, not claiming a partition.
    for (const auto& topic : decoded.topics) {
        OffsetFetchResponse::Topic answer;
        answer.name = topic.name;

        for (const PartitionId partition : topic.partitions) {
            OffsetFetchResponse::Partition result;
            result.partition = partition;

            const auto found =
                request.broker.offsets.fetch({decoded.groupId, topic.name, partition});

            if (found) {
                result.nextOffset = found->nextOffset;
                result.metadata   = found->metadata;
            } else {
                // -1, never 0. "Never committed" and "committed at the beginning"
                // differ by an entire topic's worth of records, and only the
                // client knows which its reset policy wants.
                result.nextOffset = kUnknownOffset;
            }

            answer.partitions.push_back(std::move(result));
        }

        response.topics.push_back(std::move(answer));
    }

    reply(out, response, encodeOffsetFetchResponse);
}

void registerAllHandlers(ApiRegistry& registry) {
    registry.registerHandler(ApiKey::ListOffsets, 0, handleListOffsets);
    registry.registerHandler(ApiKey::Metadata, 0, handleMetadata);
    registry.registerHandler(ApiKey::CreateTopic, 0, handleCreateTopic);
    registry.registerHandler(ApiKey::Produce, 0, handleProduce);
    registry.registerHandler(ApiKey::Fetch, 0, handleFetch);

    registry.registerHandler(ApiKey::FindCoordinator, 0, handleFindCoordinator);
    registry.registerHandler(ApiKey::JoinGroup, 0, handleJoinGroup);
    registry.registerHandler(ApiKey::SyncGroup, 0, handleSyncGroup);
    registry.registerHandler(ApiKey::Heartbeat, 0, handleHeartbeat);
    registry.registerHandler(ApiKey::LeaveGroup, 0, handleLeaveGroup);
    registry.registerHandler(ApiKey::OffsetCommit, 0, handleOffsetCommit);
    registry.registerHandler(ApiKey::OffsetFetch, 0, handleOffsetFetch);
}

}  // namespace dariyakyu::server
