#include "protocol/metadata.hpp"

#include <utility>

#include "protocol/wire.hpp"

using namespace std;

namespace dariyakyu::protocol {

void encodeMetadataRequest(BufferWriter& out, const MetadataRequest& request) {
    out.writeInt8(request.allTopics ? 1 : 0);
    out.writeInt32(static_cast<int32_t>(request.topics.size()));
    for (const auto& topic : request.topics) writeString(out, topic);
}

MetadataRequest decodeMetadataRequest(BufferReader& in) {
    MetadataRequest request;

    // Any non-zero byte is true. A client that writes 0xFF for a boolean is
    // unusual but not wrong, and refusing it would be a compatibility trap for
    // no gain.
    request.allTopics = in.readInt8() != 0;

    const int32_t count = readElementCount(in, "topic");
    request.topics.reserve(static_cast<size_t>(count));
    for (int32_t i = 0; i < count; ++i) request.topics.push_back(readString(in));

    return request;
}

void encodeMetadataResponse(BufferWriter& out, const MetadataResponse& response) {
    out.writeInt32(static_cast<int32_t>(response.brokers.size()));
    for (const auto& broker : response.brokers) {
        out.writeInt32(broker.nodeId);
        writeString(out, broker.host);
        out.writeInt32(broker.port);
    }

    out.writeInt32(response.controllerId);

    out.writeInt32(static_cast<int32_t>(response.topics.size()));
    for (const auto& topic : response.topics) {
        out.writeInt16(static_cast<int16_t>(topic.error));
        writeString(out, topic.name);

        out.writeInt32(static_cast<int32_t>(topic.partitions.size()));
        for (const auto& partition : topic.partitions) {
            out.writeInt16(static_cast<int16_t>(partition.error));
            out.writeInt32(partition.partition);
            out.writeInt32(partition.leader);
        }
    }
}

MetadataResponse decodeMetadataResponse(BufferReader& in) {
    MetadataResponse response;

    const int32_t brokerCount = readElementCount(in, "broker");
    response.brokers.reserve(static_cast<size_t>(brokerCount));
    for (int32_t i = 0; i < brokerCount; ++i) {
        MetadataResponse::Broker broker;
        broker.nodeId = in.readInt32();
        broker.host   = readString(in);
        broker.port   = in.readInt32();
        response.brokers.push_back(std::move(broker));
    }

    response.controllerId = in.readInt32();

    const int32_t topicCount = readElementCount(in, "topic");
    response.topics.reserve(static_cast<size_t>(topicCount));
    for (int32_t t = 0; t < topicCount; ++t) {
        MetadataResponse::Topic topic;
        topic.error = static_cast<ErrorCode>(in.readInt16());
        topic.name  = readString(in);

        const int32_t partitionCount = readElementCount(in, "partition");
        topic.partitions.reserve(static_cast<size_t>(partitionCount));
        for (int32_t p = 0; p < partitionCount; ++p) {
            MetadataResponse::Partition partition;
            partition.error     = static_cast<ErrorCode>(in.readInt16());
            partition.partition = in.readInt32();
            partition.leader    = in.readInt32();
            topic.partitions.push_back(partition);
        }

        response.topics.push_back(std::move(topic));
    }

    return response;
}

}  // namespace dariyakyu::protocol
