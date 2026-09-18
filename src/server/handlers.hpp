#pragma once

#include "server/api_registry.hpp"

namespace dariyakyu::server {

// The handlers, one per API.
//
// Each decodes its request from RequestContext::body, asks LogManager, and
// appends an encoded response. None of them knows what a socket is, and none
// holds state between calls — everything they touch lives in LogManager.
//
// Errors are attached PER PARTITION. A request across twelve partitions where one
// has moved returns eleven answers and one error, rather than failing wholesale
// and stalling every consumer that batched them together.
void handleListOffsets(RequestContext& request, protocol::Response& out);
void handleMetadata(RequestContext& request, protocol::Response& out);
void handleCreateTopic(RequestContext& request, protocol::Response& out);

// Registers every handler this build serves, at version 0.
void registerAllHandlers(ApiRegistry& registry);

}  // namespace dariyakyu::server
