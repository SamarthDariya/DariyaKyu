#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "protocol/request_header.hpp"
#include "protocol/socket.hpp"

namespace dariyakyu::cli {

// A client connection to one broker.
//
// Links the same protocol library the broker does, deliberately. A field the
// encoder writes and the decoder ignores shows up the first time these two talk,
// rather than at M5 when a second implementation appears — which is the entire
// reason the CLI is a real client and not test scaffolding.
//
// Synchronous: one request, one response, in order. correlationId is checked
// rather than assumed, because the whole point of having one is that a client
// could stop assuming — and a mismatch means the stream has desynchronised, which
// is worth finding out about immediately rather than three responses later.
class Client {
public:
    Client(const std::string& host, std::int32_t port,
           std::string clientId = "dariyakyu-cli");

    // Sends `body` under `key` and returns the response body, positioned past the
    // correlation id.
    std::vector<std::uint8_t> call(protocol::ApiKey key, const std::vector<std::uint8_t>& body);

private:
    protocol::Socket socket_;
    std::string      clientId_;
    std::int32_t     nextCorrelationId_ = 1;
};

}  // namespace dariyakyu::cli
