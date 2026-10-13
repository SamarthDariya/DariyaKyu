#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "protocol/request_header.hpp"
#include "protocol/socket.hpp"

namespace dariyakyu::protocol {

// A client connection to one broker.
//
// The CLI's connection, and since M7 a broker's connection to its peers too —
// which is why it lives in protocol rather than cli: the broker must not link
// the CLI to talk to another broker. One client for both also means a field the
// encoder writes and the decoder ignores shows up the first time any two of
// them talk, rather than only when a second implementation appears.
//
// Synchronous: one request, one response, in order. correlationId is checked
// rather than assumed, because the whole point of having one is that a client
// could stop assuming — and a mismatch means the stream has desynchronised, which
// is worth finding out about immediately rather than three responses later.
class Client {
public:
    // `timeoutMs` bounds connecting and each call; 0, the CLI's choice, waits
    // as long as it takes. See Socket::connectTo.
    //
    // After ANY exception from call(), discard the client. A timed-out request
    // may still be answered later, and that late reply would be read as the
    // answer to the next request — the correlation check would catch it, but
    // only after the connection was already useless.
    Client(const std::string& host, std::int32_t port,
           std::string clientId = "dariyakyu-cli", int timeoutMs = 0);

    // Sends `body` under `key` and returns the response body, positioned past the
    // correlation id.
    std::vector<std::uint8_t> call(protocol::ApiKey key, const std::vector<std::uint8_t>& body);

private:
    protocol::Socket socket_;
    std::string      clientId_;
    std::int32_t     nextCorrelationId_ = 1;
};

}  // namespace dariyakyu::protocol
