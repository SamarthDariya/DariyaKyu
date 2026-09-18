#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "common/buffer.hpp"
#include "common/types.hpp"
#include "protocol/error_codes.hpp"

namespace dariyakyu::protocol {

// "Where does this partition start and end?"
//
// The first API to implement, deliberately: it touches no record data at all, so
// it exercises framing, the topic/partition array shape and error mapping with
// nothing else in the way.
//
// A consumer asks this before its first fetch — to start at the beginning, or at
// the end — and again after an OffsetOutOfRange, to find out where the log now
// begins. That second use is why retention and this API are a pair: retention
// makes an offset disappear, and this is how a client finds out what replaced it.
//
// Timestamps are Kafka's sentinels. Searching by a real timestamp would mean
// scanning segments by largestTimestamp, which is banked rather than built: the
// two sentinels are what a consumer's auto.offset.reset actually uses.
inline constexpr std::int64_t kLatestTimestamp   = -1;
inline constexpr std::int64_t kEarliestTimestamp = -2;

// Requests and responses are grouped by topic, not flat lists of partitions.
// A member owning twelve partitions of one topic sends its name once rather than
// twelve times — and decision 9 makes multi-partition requests the normal case,
// not an optimisation.
struct ListOffsetsRequest {
    struct Partition {
        PartitionId  partition = 0;
        std::int64_t timestamp = kLatestTimestamp;
    };
    struct Topic {
        std::string            name;
        std::vector<Partition> partitions;
    };

    std::vector<Topic> topics;
};

struct ListOffsetsResponse {
    struct Partition {
        PartitionId partition = 0;
        ErrorCode   error     = ErrorCode::None;
        Offset      offset{0};
    };
    struct Topic {
        std::string            name;
        std::vector<Partition> partitions;
    };

    std::vector<Topic> topics;
};

void               encodeListOffsetsRequest(BufferWriter& out, const ListOffsetsRequest& request);
ListOffsetsRequest decodeListOffsetsRequest(BufferReader& in);

void                encodeListOffsetsResponse(BufferWriter&               out,
                                              const ListOffsetsResponse& response);
ListOffsetsResponse decodeListOffsetsResponse(BufferReader& in);

}  // namespace dariyakyu::protocol
