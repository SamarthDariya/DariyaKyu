#include "cli/client.hpp"

#include <utility>

#include "common/buffer.hpp"
#include "common/errors.hpp"
#include "protocol/frame.hpp"

using namespace std;

namespace dariyakyu::cli {

Client::Client(const string& host, int32_t port, string clientId)
    : socket_(protocol::Socket::connectTo(host, port)), clientId_(std::move(clientId)) {}

vector<uint8_t> Client::call(protocol::ApiKey key, const vector<uint8_t>& body) {
    protocol::RequestHeader header;
    header.apiKey        = key;
    header.apiVersion    = 0;
    header.correlationId = nextCorrelationId_++;
    header.clientId      = clientId_;

    BufferWriter out;
    protocol::encodeRequestHeader(out, header);
    out.writeBytes(body);
    protocol::writeFrame(socket_.fd(), out.take());

    const auto frame = protocol::readFrame(socket_.fd(), protocol::kMaxFrameBytes);
    if (!frame)
        throw Error("the broker closed the connection without answering — it may not support "
                    "this request, or it may be shutting down");

    BufferReader in(*frame);
    const int32_t correlationId = protocol::decodeResponseHeader(in);

    // Checked, not assumed. A mismatch means the stream has desynchronised, and
    // finding that out here beats decoding the next three responses against the
    // wrong requests.
    if (correlationId != header.correlationId)
        throw CorruptData("response carried correlation id " + to_string(correlationId) +
                          ", expected " + to_string(header.correlationId));

    const auto rest = in.readBytes(in.remaining());
    return vector<uint8_t>(rest.begin(), rest.end());
}

}  // namespace dariyakyu::cli
