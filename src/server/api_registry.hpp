#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <utility>

#include "protocol/request_header.hpp"
#include "protocol/response.hpp"
#include "server/broker_context.hpp"

namespace dariyakyu::server {

// A handler appends its response body to `out`.
//
// Appending rather than returning, for one reason: a Fetch response is a list of
// buffer and file segments, and returning one would mean merging two lists per
// request. The response header is already in `out` when a handler is called, so
// no handler writes a correlation id.
using Handler = std::function<void(RequestContext& request, protocol::Response& out)>;

// Which handler serves which request.
//
// A registry rather than a switch, and the reason is apiVersion. Versioned APIs
// mean two handlers for one key, and a switch on key containing a switch on
// version is the shape that rots — every new version edits a function that every
// other version also lives in.
class ApiRegistry {
public:
    // Replaces any handler already registered for this key and version, so a test
    // can substitute one without unregistering first.
    void registerHandler(protocol::ApiKey key, std::int16_t version, Handler handler);

    bool has(protocol::ApiKey key, std::int16_t version) const;

    // Writes the response header, then the handler's body.
    //
    // A request this build cannot serve gets an UnsupportedVersion body rather
    // than a closed connection. A client probing what a broker supports is
    // normal behaviour, not an error — and the correlation id is already in
    // hand, which is exactly why decodeRequestHeader refuses to validate the api
    // key it just read.
    protocol::Response dispatch(RequestContext& request) const;

private:
    std::map<std::pair<protocol::ApiKey, std::int16_t>, Handler> handlers_;
};

}  // namespace dariyakyu::server
