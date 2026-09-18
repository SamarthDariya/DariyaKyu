#include "protocol/list_offsets.hpp"

#include "protocol/wire.hpp"

using namespace std;

namespace dariyakyu::protocol {

void encodeListOffsetsRequest(BufferWriter& out, const ListOffsetsRequest& request) {
    out.writeInt32(static_cast<int32_t>(request.topics.size()));
    for (const auto& topic : request.topics) {
        writeString(out, topic.name);
        out.writeInt32(static_cast<int32_t>(topic.partitions.size()));
        for (const auto& partition : topic.partitions) {
            out.writeInt32(partition.partition);
            out.writeInt64(partition.timestamp);
        }
    }
}

ListOffsetsRequest decodeListOffsetsRequest(BufferReader& in) {
    ListOffsetsRequest request;

    const int32_t topicCount = readElementCount(in, "topic");
    request.topics.reserve(static_cast<size_t>(topicCount));

    for (int32_t t = 0; t < topicCount; ++t) {
        ListOffsetsRequest::Topic topic;
        topic.name = readString(in);

        const int32_t partitionCount = readElementCount(in, "partition");
        topic.partitions.reserve(static_cast<size_t>(partitionCount));

        for (int32_t p = 0; p < partitionCount; ++p) {
            ListOffsetsRequest::Partition partition;
            partition.partition = in.readInt32();
            partition.timestamp = in.readInt64();
            topic.partitions.push_back(partition);
        }

        request.topics.push_back(std::move(topic));
    }

    return request;
}

void encodeListOffsetsResponse(BufferWriter& out, const ListOffsetsResponse& response) {
    out.writeInt32(static_cast<int32_t>(response.topics.size()));
    for (const auto& topic : response.topics) {
        writeString(out, topic.name);
        out.writeInt32(static_cast<int32_t>(topic.partitions.size()));
        for (const auto& partition : topic.partitions) {
            out.writeInt32(partition.partition);
            // Written explicitly rather than left to an initialiser, because
            // zero means success and a forgotten field would read as one.
            out.writeInt16(static_cast<int16_t>(partition.error));
            out.writeInt64(partition.offset.value());
        }
    }
}

ListOffsetsResponse decodeListOffsetsResponse(BufferReader& in) {
    ListOffsetsResponse response;

    const int32_t topicCount = readElementCount(in, "topic");
    response.topics.reserve(static_cast<size_t>(topicCount));

    for (int32_t t = 0; t < topicCount; ++t) {
        ListOffsetsResponse::Topic topic;
        topic.name = readString(in);

        const int32_t partitionCount = readElementCount(in, "partition");
        topic.partitions.reserve(static_cast<size_t>(partitionCount));

        for (int32_t p = 0; p < partitionCount; ++p) {
            ListOffsetsResponse::Partition partition;
            partition.partition = in.readInt32();
            partition.error     = static_cast<ErrorCode>(in.readInt16());
            partition.offset    = Offset{in.readInt64()};
            topic.partitions.push_back(partition);
        }

        response.topics.push_back(std::move(topic));
    }

    return response;
}

}  // namespace dariyakyu::protocol
