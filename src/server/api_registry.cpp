#include "server/api_registry.hpp"

#include <utility>

#include "protocol/error_codes.hpp"

using namespace std;

namespace dariyakyu::server {

void ApiRegistry::registerHandler(protocol::ApiKey key, int16_t version, Handler handler) {
    handlers_[{key, version}] = std::move(handler);
}

bool ApiRegistry::has(protocol::ApiKey key, int16_t version) const {
    return handlers_.find({key, version}) != handlers_.end();
}

protocol::Response ApiRegistry::dispatch(RequestContext& request) const {
    protocol::Response response;

    // First, and by this layer rather than by each handler: every response
    // carries it, and a handler that forgot would produce a reply no client
    // could match to anything.
    BufferWriter header;
    protocol::encodeResponseHeader(header, request.header.correlationId);
    response.append(header.take());

    const auto found = handlers_.find({request.header.apiKey, request.header.apiVersion});
    if (found == handlers_.end()) {
        // Answered, not disconnected. A client asking whether this broker speaks
        // a newer version is doing something reasonable, and the connection is
        // still good for the request it sends next.
        BufferWriter unsupported;
        unsupported.writeInt16(static_cast<int16_t>(protocol::ErrorCode::UnsupportedVersion));
        response.append(unsupported.take());
        return response;
    }

    found->second(request, response);
    return response;
}

}  // namespace dariyakyu::server
