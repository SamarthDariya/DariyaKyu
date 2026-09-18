#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "common/buffer.hpp"
#include "protocol/error_codes.hpp"

namespace dariyakyu::protocol {

// "Make this topic, with this many partitions."
//
// The only API that creates state rather than reading or appending to it, and
// the only one a CLI needs before it can produce anything.
struct CreateTopicRequest {
    struct Topic {
        std::string  name;
        std::int32_t partitionCount = 1;

        // Overrides for this topic's partitions, each absent to mean "use the
        // broker's default".
        //
        // Typed fields rather than Kafka's generic [name, value] string pairs.
        // Strings would mean parsing numbers out of client input at the point
        // they are written into partition.meta — a place where a typo becomes a
        // retention window that silently keeps data forever. There are three
        // knobs; when there are thirty, the generic form earns itself.
        //
        // On the wire an absent override is -1, the same sentinel partition.meta
        // uses for unlimited bytes. It is unambiguous here because none of these
        // is meaningful at zero or below.
        std::optional<std::int64_t> retentionMs;
        std::optional<std::int64_t> retentionBytes;
        std::optional<std::int64_t> maxSegmentBytes;
    };

    std::vector<Topic> topics;

    // Accepted and ignored at M4: creation is synchronous and local, so there is
    // nothing to wait for. It exists now because M8 makes this a request to the
    // controller, which can take time, and adding the field later would change
    // the protocol.
    std::int32_t timeoutMs = 0;
};

struct CreateTopicResponse {
    struct Topic {
        std::string name;
        ErrorCode   error = ErrorCode::None;
    };

    std::vector<Topic> topics;
};

void               encodeCreateTopicRequest(BufferWriter& out, const CreateTopicRequest& request);
CreateTopicRequest decodeCreateTopicRequest(BufferReader& in);

void                encodeCreateTopicResponse(BufferWriter&              out,
                                              const CreateTopicResponse& response);
CreateTopicResponse decodeCreateTopicResponse(BufferReader& in);

}  // namespace dariyakyu::protocol
