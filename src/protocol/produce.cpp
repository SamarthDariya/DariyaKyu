#include "protocol/produce.hpp"

#include <string>
#include <utility>

#include "common/errors.hpp"
#include "protocol/wire.hpp"

using namespace std;

namespace dariyakyu::protocol {

void encodeProduceRequest(BufferWriter& out, const ProduceRequest& request) {
    out.writeInt16(request.acks);
    out.writeInt32(request.timeoutMs);

    out.writeInt32(static_cast<int32_t>(request.topics.size()));
    for (const auto& topic : request.topics) {
        writeString(out, topic.name);
        out.writeInt32(static_cast<int32_t>(topic.partitions.size()));
        for (const auto& partition : topic.partitions) {
            out.writeInt32(partition.partition);
            out.writeInt32(static_cast<int32_t>(partition.batch.size()));
            out.writeBytes(partition.batch);
        }
    }
}

ProduceRequest decodeProduceRequest(BufferReader& in) {
    ProduceRequest request;

    request.acks      = in.readInt16();
    request.timeoutMs = in.readInt32();

    const int32_t topicCount = readElementCount(in, "topic");
    request.topics.reserve(static_cast<size_t>(topicCount));

    for (int32_t t = 0; t < topicCount; ++t) {
        ProduceRequest::Topic topic;
        topic.name = readString(in);

        const int32_t partitionCount = readElementCount(in, "partition");
        topic.partitions.reserve(static_cast<size_t>(partitionCount));

        for (int32_t p = 0; p < partitionCount; ++p) {
            ProduceRequest::Partition partition;
            partition.partition = in.readInt32();

            const int32_t batchLength = in.readInt32();
            if (batchLength < 0)
                throw CorruptData("produce: batch length " + to_string(batchLength));

            // Recorded BEFORE the read, because readBytes advances the position.
            partition.batchOffsetInBody = in.position();

            // Borrowed. readBytes throws of its own accord if the length runs
            // past the end, so a lying length needs no separate check.
            partition.batch = in.readBytes(static_cast<size_t>(batchLength));

            topic.partitions.push_back(partition);
        }

        request.topics.push_back(std::move(topic));
    }

    return request;
}

span<uint8_t> mutableBatch(const ProduceRequest::Partition& partition, span<uint8_t> body) {
    const size_t end = partition.batchOffsetInBody + partition.batch.size();

    // The arithmetic cannot overflow into a valid-looking range, because both
    // operands came from a decode that already bounded them — but a caller
    // passing the wrong buffer is a different mistake, and this is what catches
    // it before a stamp is written through a wild pointer.
    if (end < partition.batchOffsetInBody || end > body.size())
        throw OffsetInvariantViolated("produce: batch at " +
                                      to_string(partition.batchOffsetInBody) + " + " +
                                      to_string(partition.batch.size()) +
                                      " does not lie inside a body of " + to_string(body.size()));

    return body.subspan(partition.batchOffsetInBody, partition.batch.size());
}

void encodeProduceResponse(BufferWriter& out, const ProduceResponse& response) {
    out.writeInt32(static_cast<int32_t>(response.topics.size()));
    for (const auto& topic : response.topics) {
        writeString(out, topic.name);
        out.writeInt32(static_cast<int32_t>(topic.partitions.size()));
        for (const auto& partition : topic.partitions) {
            out.writeInt32(partition.partition);
            out.writeInt16(static_cast<int16_t>(partition.error));
            out.writeInt64(partition.baseOffset.value());
        }
    }
}

ProduceResponse decodeProduceResponse(BufferReader& in) {
    ProduceResponse response;

    const int32_t topicCount = readElementCount(in, "topic");
    response.topics.reserve(static_cast<size_t>(topicCount));

    for (int32_t t = 0; t < topicCount; ++t) {
        ProduceResponse::Topic topic;
        topic.name = readString(in);

        const int32_t partitionCount = readElementCount(in, "partition");
        topic.partitions.reserve(static_cast<size_t>(partitionCount));

        for (int32_t p = 0; p < partitionCount; ++p) {
            ProduceResponse::Partition partition;
            partition.partition  = in.readInt32();
            partition.error      = static_cast<ErrorCode>(in.readInt16());
            partition.baseOffset = Offset{in.readInt64()};
            topic.partitions.push_back(partition);
        }

        response.topics.push_back(std::move(topic));
    }

    return response;
}

}  // namespace dariyakyu::protocol
