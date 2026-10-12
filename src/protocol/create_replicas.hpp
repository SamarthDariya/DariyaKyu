#pragma once

#include <vector>

#include "common/buffer.hpp"
#include "common/types.hpp"
#include "protocol/create_topic.hpp"

namespace dariyakyu::protocol {

// Broker to broker (M7): "create your copies of these partitions".
//
// Sent by the broker that received a CreateTopic to every other broker holding a
// replica. There is no controller until M8, so the broker that was asked does a
// controller's job for that one topic, once — and M8's LeaderAndIsr replaces
// this request rather than adding a second way of doing the same thing.
//
//   topics       [
//     settings     the topic's CreateTopic fields, byte for byte
//     partitions   [ partition int32, replicas [int32] ]
//   ]
//
// The settings travel so the follower's copy is created with the LEADER's
// retention and segment rules, not with its own defaults. The replicas travel
// explicitly, rather than each broker recomputing them, so the receiver creates
// exactly what it is told even if its own cluster list somehow disagrees.
//
// The response is a CreateTopicResponse: one error per topic.
struct CreateReplicasRequest {
    struct Partition {
        PartitionId         partition = 0;
        std::vector<NodeId> replicas;   // leader first
    };

    struct Topic {
        CreateTopicRequest::Topic settings;
        std::vector<Partition>    partitions;
    };

    std::vector<Topic> topics;
};

void                  encodeCreateReplicasRequest(BufferWriter& out, const CreateReplicasRequest& request);
CreateReplicasRequest decodeCreateReplicasRequest(BufferReader& in);

}  // namespace dariyakyu::protocol
