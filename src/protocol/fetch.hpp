#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "common/buffer.hpp"
#include "common/types.hpp"
#include "protocol/error_codes.hpp"
#include "protocol/response.hpp"

namespace dariyakyu::protocol {

// "Send me records from these offsets."
//
// The read path, and the only API whose response cannot be one buffer.
struct FetchRequest {
    struct Partition {
        PartitionId  partition   = 0;
        Offset       fetchOffset{0};
        std::int32_t maxBytes    = 1 << 20;
    };

    struct Topic {
        std::string            name;
        std::vector<Partition> partitions;
    };

    // Both accepted and IGNORED at M4: a fetch returns whatever is available
    // immediately. Honouring them means parking a request until enough data
    // arrives, which needs it to outlive the thread that read it — M9's
    // architecture. The fields exist now so the protocol does not change then.
    std::int32_t maxWaitMs = 0;
    std::int32_t minBytes  = 0;

    std::vector<Topic> topics;
};

// What the broker sends.
//
// Records are a FileRange, not bytes — the whole point. Encoding this produces a
// Response whose segments alternate between metadata built here and record bytes
// that stay in the kernel.
struct FetchResponse {
    struct Partition {
        PartitionId partition = 0;
        ErrorCode   error     = ErrorCode::None;

        // A consumer needs both to interpret an OffsetOutOfRange: below the log
        // start means "you were too slow", above the log end means the log was
        // truncated under it. The wire has one error code, Kafka-shaped, and
        // these two numbers are how a client tells the cases apart.
        Offset highWatermark{0};
        Offset logStartOffset{0};

        FileRange records;
    };

    struct Topic {
        std::string            name;
        std::vector<Partition> partitions;
    };

    std::vector<Topic> topics;
};

// What a client got.
//
// The same fields, except records are borrowed bytes rather than a location — a
// client receives a flat frame off a socket and has no file to point at. The two
// types are genuinely different shapes, so they are two types rather than one
// with a field that is only half-used.
struct FetchResponseView {
    struct Partition {
        PartitionId                   partition = 0;
        ErrorCode                     error     = ErrorCode::None;
        Offset                        highWatermark{0};
        Offset                        logStartOffset{0};
        std::span<const std::uint8_t> records;   // valid while the frame lives
    };

    struct Topic {
        std::string            name;
        std::vector<Partition> partitions;
    };

    std::vector<Topic> topics;
};

void         encodeFetchRequest(BufferWriter& out, const FetchRequest& request);
FetchRequest decodeFetchRequest(BufferReader& in);

// Appends this response's segments to `out`.
//
// Takes a Response rather than a BufferWriter, and that is the difference
// between this API and every other one: metadata is written into a buffer, and
// each partition's records are appended as a file range that sendfile will serve
// without the bytes ever entering this process.
void encodeFetchResponse(Response& out, const FetchResponse& response);

FetchResponseView decodeFetchResponse(BufferReader& in);

}  // namespace dariyakyu::protocol
