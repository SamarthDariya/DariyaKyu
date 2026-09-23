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
void handleProduce(RequestContext& request, protocol::Response& out);
void handleFetch(RequestContext& request, protocol::Response& out);

// Consumer groups (M5).
void handleFindCoordinator(RequestContext& request, protocol::Response& out);
void handleJoinGroup(RequestContext& request, protocol::Response& out);
void handleSyncGroup(RequestContext& request, protocol::Response& out);
void handleHeartbeat(RequestContext& request, protocol::Response& out);
void handleLeaveGroup(RequestContext& request, protocol::Response& out);
void handleOffsetCommit(RequestContext& request, protocol::Response& out);
void handleOffsetFetch(RequestContext& request, protocol::Response& out);

// Registers every handler this build serves, at version 0.
void registerAllHandlers(ApiRegistry& registry);

}  // namespace dariyakyu::server
