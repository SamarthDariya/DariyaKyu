#include "protocol/create_replicas.hpp"

#include <utility>

#include "protocol/wire.hpp"

using namespace std;

namespace dariyakyu::protocol {

void encodeCreateReplicasRequest(BufferWriter& out, const CreateReplicasRequest& request) {
    out.writeInt32(static_cast<int32_t>(request.topics.size()));
    for (const auto& topic : request.topics) {
        writeTopicSettings(out, topic.settings);
        out.writeInt32(static_cast<int32_t>(topic.partitions.size()));
        for (const auto& partition : topic.partitions) {
            out.writeInt32(partition.partition);
            out.writeInt32(static_cast<int32_t>(partition.replicas.size()));
            for (const NodeId node : partition.replicas) out.writeInt32(node);
        }
    }
}

CreateReplicasRequest decodeCreateReplicasRequest(BufferReader& in) {
    CreateReplicasRequest request;

    // Every count goes through readElementCount, which refuses one larger than
    // the bytes left — so a corrupt count fails at once instead of looping two
    // billion times before the reads run out.
    const int32_t topics = readElementCount(in, "topic");
    for (int32_t t = 0; t < topics; ++t) {
        CreateReplicasRequest::Topic topic;
        topic.settings = readTopicSettings(in);

        const int32_t partitions = readElementCount(in, "partition");
        for (int32_t p = 0; p < partitions; ++p) {
            CreateReplicasRequest::Partition partition;
            partition.partition = in.readInt32();

            const int32_t replicas = readElementCount(in, "replica");
            for (int32_t r = 0; r < replicas; ++r) partition.replicas.push_back(in.readInt32());
            topic.partitions.push_back(std::move(partition));
        }
        request.topics.push_back(std::move(topic));
    }
    return request;
}

}  // namespace dariyakyu::protocol
