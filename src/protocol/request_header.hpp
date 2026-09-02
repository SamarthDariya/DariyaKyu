#pragma once

#include <array>
#include <cstdint>
#include <string>

#include "common/buffer.hpp"

namespace dariyakyu::protocol {

// Which request this is.
//
// The numbers are Kafka's own, not ours in sequence. That costs nothing and it
// is what makes decision 19's "a shim rather than a redesign" credible: a
// translation layer would otherwise have to remap every key as well as every
// field. Kafka's gaps (5..18) are left as gaps rather than reused.
enum class ApiKey : std::int16_t {
    Produce     = 0,
    Fetch       = 1,
    ListOffsets = 2,
    Metadata    = 3,
    CreateTopic = 19,
};

inline constexpr std::array<ApiKey, 5> kAllApiKeys{
    ApiKey::Produce, ApiKey::Fetch, ApiKey::ListOffsets, ApiKey::Metadata, ApiKey::CreateTopic,
};

// Whether this build has a handler for `key` at all.
//
// Note this is asked AFTER decoding, never during it — see below.
bool        isKnown(ApiKey key);
const char* describe(ApiKey key);

// The fixed prefix every request carries, inside the length-prefixed frame.
//
//   apiKey        int16
//   apiVersion    int16
//   correlationId int32
//   clientId      int16 length + bytes   (-1 = null)
struct RequestHeader {
    ApiKey       apiKey        = ApiKey::Metadata;
    std::int16_t apiVersion    = 0;
    std::int32_t correlationId = 0;
    std::string  clientId;
};

void encodeRequestHeader(BufferWriter& out, const RequestHeader& header);

// Decodes the header and leaves the reader positioned at the body, so a handler
// reads its own fields from the same reader.
//
// Deliberately does NOT reject an unknown apiKey or apiVersion, and that is the
// whole point of the function's shape.
//
// A broker that cannot answer a request still has to answer *something*, and
// every response is matched to its request by `correlationId` — which lives in
// the header. Throwing on an unrecognised key would mean the broker knew the
// request was unsupported and had no way to say so, because the id it needed to
// reply with was in the part it refused to read. The client would see a closed
// connection or a hang instead of `UnsupportedVersion`.
//
// So the header decodes permissively and the registry answers. What this DOES
// reject is a header that is structurally impossible: a clientId length that is
// negative but not the null marker, or one that runs past the end of the frame.
RequestHeader decodeRequestHeader(BufferReader& in);

// A response's prefix is just the correlation id — there is nothing else a client
// needs to route it, and anything more would be a field every API paid for.
void         encodeResponseHeader(BufferWriter& out, std::int32_t correlationId);
std::int32_t decodeResponseHeader(BufferReader& in);

}  // namespace dariyakyu::protocol
