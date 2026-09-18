#include "protocol/fetch.hpp"

#include <string>
#include <utility>

#include "common/errors.hpp"
#include "protocol/wire.hpp"

using namespace std;

namespace dariyakyu::protocol {

void encodeFetchRequest(BufferWriter& out, const FetchRequest& request) {
    out.writeInt32(request.maxWaitMs);
    out.writeInt32(request.minBytes);

    out.writeInt32(static_cast<int32_t>(request.topics.size()));
    for (const auto& topic : request.topics) {
        writeString(out, topic.name);
        out.writeInt32(static_cast<int32_t>(topic.partitions.size()));
        for (const auto& partition : topic.partitions) {
            out.writeInt32(partition.partition);
            out.writeInt64(partition.fetchOffset.value());
            out.writeInt32(partition.maxBytes);
        }
    }
}

FetchRequest decodeFetchRequest(BufferReader& in) {
    FetchRequest request;

    request.maxWaitMs = in.readInt32();
    request.minBytes  = in.readInt32();

    const int32_t topicCount = readElementCount(in, "topic");
    request.topics.reserve(static_cast<size_t>(topicCount));

    for (int32_t t = 0; t < topicCount; ++t) {
        FetchRequest::Topic topic;
        topic.name = readString(in);

        const int32_t partitionCount = readElementCount(in, "partition");
        topic.partitions.reserve(static_cast<size_t>(partitionCount));

        for (int32_t p = 0; p < partitionCount; ++p) {
            FetchRequest::Partition partition;
            partition.partition   = in.readInt32();
            partition.fetchOffset = Offset{in.readInt64()};
            partition.maxBytes    = in.readInt32();

            // A negative fetch size is not a conservative client, it is a broken
            // one, and clamping silently would hide it.
            if (partition.maxBytes < 0)
                throw CorruptData("fetch: maxBytes " + to_string(partition.maxBytes));

            topic.partitions.push_back(partition);
        }

        request.topics.push_back(std::move(topic));
    }

    return request;
}

void encodeFetchResponse(Response& out, const FetchResponse& response) {
    // Metadata accumulates here until a file range interrupts it. Flushing only
    // at those points keeps the segment count down: a fetch over twelve
    // partitions with no data waiting produces ONE buffer segment, not twelve.
    BufferWriter buffer;

    buffer.writeInt32(static_cast<int32_t>(response.topics.size()));
    for (const auto& topic : response.topics) {
        writeString(buffer, topic.name);
        buffer.writeInt32(static_cast<int32_t>(topic.partitions.size()));

        for (const auto& partition : topic.partitions) {
            buffer.writeInt32(partition.partition);
            buffer.writeInt16(static_cast<int16_t>(partition.error));
            buffer.writeInt64(partition.highWatermark.value());
            buffer.writeInt64(partition.logStartOffset.value());
            buffer.writeInt32(static_cast<int32_t>(partition.records.length));

            if (partition.records.length == 0) continue;

            // The records go next, so what has been written so far has to reach
            // the wire first. take() ends this writer, hence a fresh one.
            out.append(buffer.take());
            buffer = BufferWriter{};

            // Never read here. The kernel will copy these bytes from its own page
            // cache straight to the socket.
            out.append(partition.records);
        }
    }

    out.append(buffer.take());
}

FetchResponseView decodeFetchResponse(BufferReader& in) {
    FetchResponseView response;

    const int32_t topicCount = readElementCount(in, "topic");
    response.topics.reserve(static_cast<size_t>(topicCount));

    for (int32_t t = 0; t < topicCount; ++t) {
        FetchResponseView::Topic topic;
        topic.name = readString(in);

        const int32_t partitionCount = readElementCount(in, "partition");
        topic.partitions.reserve(static_cast<size_t>(partitionCount));

        for (int32_t p = 0; p < partitionCount; ++p) {
            FetchResponseView::Partition partition;
            partition.partition      = in.readInt32();
            partition.error          = static_cast<ErrorCode>(in.readInt16());
            partition.highWatermark  = Offset{in.readInt64()};
            partition.logStartOffset = Offset{in.readInt64()};

            const int32_t recordsLength = in.readInt32();
            if (recordsLength < 0)
                throw CorruptData("fetch: records length " + to_string(recordsLength));

            // Borrowed, like everything else on the read path.
            partition.records = in.readBytes(static_cast<size_t>(recordsLength));

            topic.partitions.push_back(partition);
        }

        response.topics.push_back(std::move(topic));
    }

    return response;
}

}  // namespace dariyakyu::protocol
