#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "common/buffer.hpp"
#include "common/types.hpp"
#include "protocol/error_codes.hpp"

namespace dariyakyu::protocol {

// "Append these batches."
//
// The write path, and the one API whose payload the broker deliberately does not
// understand. It validates a checksum, reads a record count out of the header,
// and hands the bytes to Log::append — which stamps twelve of them. A compressed
// batch therefore costs the broker nothing, because nothing here decompresses.
struct ProduceRequest {
    struct Partition {
        PartitionId partition = 0;

        // BORROWED from the frame, never copied. A produce of a megabyte must
        // not become two megabytes because the decoder wanted to own its input.
        // Valid only while that frame lives.
        std::span<const std::uint8_t> batch;

        // Where those bytes begin within the decoded body.
        //
        // Set by the decoder; ignored when encoding. It exists because the
        // broker stamps the assigned base offset INTO these bytes, in place, and
        // needs a mutable view of them — while BufferReader, being a reading
        // interface, only ever hands out const ones. Recording the position lets
        // the handler recover a mutable span from the frame it owns, with no
        // const_cast anywhere.
        std::size_t batchOffsetInBody = 0;
    };

    struct Topic {
        std::string            name;
        std::vector<Partition> partitions;
    };

    // How many replicas must acknowledge. Carried from M4 and meaningless until
    // M7: with one node, everything written is immediately committed.
    std::int16_t acks = 1;

    // Accepted and ignored, like acks — there is nothing to wait for yet.
    std::int32_t timeoutMs = 0;

    std::vector<Topic> topics;
};

struct ProduceResponse {
    struct Partition {
        PartitionId partition = 0;
        ErrorCode   error     = ErrorCode::None;

        // The offset assigned to the batch's FIRST record. A producer needs it
        // to report where its records landed; kUnknownOffset when the append
        // failed.
        Offset baseOffset{-1};
    };

    struct Topic {
        std::string            name;
        std::vector<Partition> partitions;
    };

    std::vector<Topic> topics;
};

void encodeProduceRequest(BufferWriter& out, const ProduceRequest& request);

// The returned spans point into `in`'s buffer, so it must outlive the request.
ProduceRequest decodeProduceRequest(BufferReader& in);

// A mutable view of one partition's batch, within the body the request was
// decoded from.
//
// Checked rather than trusted: a partition whose recorded range does not lie
// inside `body` throws rather than handing back a span into nothing. The check
// is cheap and the failure it prevents is a write through a wild pointer.
std::span<std::uint8_t> mutableBatch(const ProduceRequest::Partition& partition,
                                     std::span<std::uint8_t>          body);

void            encodeProduceResponse(BufferWriter& out, const ProduceResponse& response);
ProduceResponse decodeProduceResponse(BufferReader& in);

}  // namespace dariyakyu::protocol
