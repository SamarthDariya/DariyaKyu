#include "server/connection.hpp"

#include <optional>
#include <span>
#include <utility>
#include <vector>

#include "common/errors.hpp"
#include "protocol/error_codes.hpp"
#include "protocol/request_header.hpp"
#include "protocol/response.hpp"

using namespace std;

namespace dariyakyu::server {

namespace {

// A reply carrying nothing but an error, for a request that was framed correctly
// and understood no further.
protocol::Response errorResponse(int32_t correlationId, protocol::ErrorCode error) {
    protocol::Response response;

    BufferWriter out;
    protocol::encodeResponseHeader(out, correlationId);
    out.writeInt16(static_cast<int16_t>(error));
    response.append(out.take());

    return response;
}

}  // namespace

Connection::Connection(Socket socket, const ApiRegistry& registry, BrokerContext& broker,
                       size_t maxFrameBytes)
    : socket_(std::move(socket)),
      registry_(registry),
      broker_(broker),
      maxFrameBytes_(maxFrameBytes) {}

void Connection::serve() {
    while (true) {
        optional<vector<uint8_t>> frame;
        try {
            frame = protocol::readFrame(socket_.fd(), maxFrameBytes_);
        } catch (const Error&) {
            // A frame that was too large, or a stream that ended mid-frame. Where
            // the next frame begins is now unknown, so there is nothing to do but
            // hang up — answering would put bytes on a stream neither side can
            // parse any more.
            return;
        }

        // Closed between frames: how a connection normally ends.
        if (!frame) return;

        BufferReader             reader(*frame);
        protocol::RequestHeader  header;
        try {
            header = protocol::decodeRequestHeader(reader);
        } catch (const Error&) {
            // No correlation id, so no way to say anything a client could match
            // to what it sent.
            return;
        }

        protocol::Response response;
        try {
            RequestContext request{header, reader, span<uint8_t>(*frame), broker_};
            response = registry_.dispatch(request);
        } catch (const Error&) {
            // The body did not decode, or a handler failed outright. The
            // connection SURVIVES this, and that is a property of length-prefixed
            // framing: a whole frame was consumed, so the next one still begins
            // where it should. A delimiter-based protocol would have to hang up
            // here, having no idea where the damage ended.
            response = errorResponse(header.correlationId, protocol::ErrorCode::CorruptMessage);
        }

        try {
            protocol::writeResponse(socket_.fd(), response);
        } catch (const Error&) {
            // The client went away mid-reply. Nothing to report it to.
            return;
        }
    }
}

void Connection::stop() {
    // shutdown, not close: the descriptor stays valid, so a thread that wakes up
    // in serve() sees a clean end of stream rather than a number the kernel may
    // already have handed to something else.
    socket_.shutdown();
}

}  // namespace dariyakyu::server
