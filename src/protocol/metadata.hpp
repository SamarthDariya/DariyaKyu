#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "common/buffer.hpp"
#include "common/types.hpp"
#include "protocol/error_codes.hpp"

namespace dariyakyu::protocol {

// "What topics exist, and who leads each partition?"
//
// The first request any client makes, and the one it repeats whenever it is told
// NotLeaderForPartition — which is how a client discovers that a partition moved.
struct MetadataRequest {
    // True means every topic the broker hosts.
    //
    // An explicit flag rather than Kafka's convention of a null array meaning
    // "all" and an empty array meaning "none". Those two encodings differ by one
    // byte and mean opposite things, and every other array in this protocol
    // treats null and empty alike — so relying on the distinction here would make
    // one array in the protocol read differently from all the others, which is
    // how a client ends up subscribed to everything by accident. A compatibility
    // shim maps null to this flag.
    bool allTopics = false;

    // Ignored when allTopics is set.
    std::vector<std::string> topics;
};

struct MetadataResponse {
    struct Broker {
        NodeId       nodeId = 0;
        std::string  host;
        std::int32_t port = 0;
    };

    struct Partition {
        ErrorCode   error     = ErrorCode::None;
        PartitionId partition = 0;

        // Which broker to send this partition's produces and fetches to. One
        // node until M8, so always this one — but the field exists from M4 so a
        // client is written against a moving target from the start rather than
        // learning about leaders later.
        NodeId leader = 0;
    };

    struct Topic {
        ErrorCode              error = ErrorCode::None;
        std::string            name;
        std::vector<Partition> partitions;
    };

    std::vector<Broker> brokers;

    // -1 until M8 introduces one. A client that needs the controller — to create
    // a topic, say — reads this rather than assuming any particular broker.
    NodeId controllerId = -1;

    std::vector<Topic> topics;
};

void            encodeMetadataRequest(BufferWriter& out, const MetadataRequest& request);
MetadataRequest decodeMetadataRequest(BufferReader& in);

void             encodeMetadataResponse(BufferWriter& out, const MetadataResponse& response);
MetadataResponse decodeMetadataResponse(BufferReader& in);

}  // namespace dariyakyu::protocol
