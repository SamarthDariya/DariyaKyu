#include "protocol/request_header.hpp"

#include <algorithm>
#include <string>

#include "common/errors.hpp"

using namespace std;

namespace dariyakyu::protocol {

namespace {

// Kafka's marker for a null string, as distinct from a zero-length one.
constexpr int16_t kNullLength = -1;

}  // namespace

bool isKnown(ApiKey key) {
    return find(kAllApiKeys.begin(), kAllApiKeys.end(), key) != kAllApiKeys.end();
}

const char* describe(ApiKey key) {
    switch (key) {
        case ApiKey::Produce: return "Produce";
        case ApiKey::Fetch: return "Fetch";
        case ApiKey::ListOffsets: return "ListOffsets";
        case ApiKey::Metadata: return "Metadata";
        case ApiKey::CreateTopic: return "CreateTopic";
    }
    return "unrecognised api key";
}

void encodeRequestHeader(BufferWriter& out, const RequestHeader& header) {
    out.writeInt16(static_cast<int16_t>(header.apiKey));
    out.writeInt16(header.apiVersion);
    out.writeInt32(header.correlationId);

    // An empty clientId is written as length 0 rather than as the null marker.
    //
    // Unlike a record key, where null and empty are different facts and
    // collapsing them would delete data, a clientId carries no meaning to the
    // broker either way — it is for logging. So the two are treated as the same
    // thing, and the simpler of the two encodings is chosen.
    out.writeInt16(static_cast<int16_t>(header.clientId.size()));
    out.writeBytes({reinterpret_cast<const uint8_t*>(header.clientId.data()),
                    header.clientId.size()});
}

RequestHeader decodeRequestHeader(BufferReader& in) {
    RequestHeader header;

    // No validation of either of these. The registry decides whether it can serve
    // them, because by then it has the correlationId it needs to say no.
    header.apiKey        = static_cast<ApiKey>(in.readInt16());
    header.apiVersion    = in.readInt16();
    header.correlationId = in.readInt32();

    const int16_t clientIdLength = in.readInt16();
    if (clientIdLength < 0) {
        // Null is legal and means "no client id". Any other negative length is
        // not a value a correct encoder can produce.
        if (clientIdLength != kNullLength)
            throw CorruptData("request header: client id length " + to_string(clientIdLength));
    } else {
        // readBytes throws CorruptData of its own accord if the length runs past
        // the end of the frame, so a lying length needs no separate check.
        const auto bytes = in.readBytes(static_cast<size_t>(clientIdLength));
        header.clientId.assign(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    }

    return header;
}

void encodeResponseHeader(BufferWriter& out, int32_t correlationId) {
    out.writeInt32(correlationId);
}

int32_t decodeResponseHeader(BufferReader& in) {
    return in.readInt32();
}

}  // namespace dariyakyu::protocol
