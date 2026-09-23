#pragma once

#include <cstdint>
#include <span>
#include <string>

#include "common/buffer.hpp"
#include "common/types.hpp"
#include "protocol/request_header.hpp"
#include "protocol/response.hpp"
#include "group/group_coordinator.hpp"
#include "group/offset_store.hpp"
#include "storage/log_manager.hpp"

namespace dariyakyu::server {

// Everything a handler is allowed to touch.
//
// References, not ownership. Handlers are stateless — everything they mutate
// lives in LogManager, which has its own locking — so there is no broker-wide
// lock in this design and nowhere for one to be added by accident.
//
// M5 adds the group coordinator here and M8 the controller's view. Growing this
// struct is how a new subsystem becomes reachable from a handler, which makes it
// a deliberate act rather than a global appearing.
struct BrokerContext {
    storage::LogManager& logs;

    // M5's additions, and the first state in this struct that is not a log.
    // Growing it is how a new subsystem becomes reachable from a handler, which
    // keeps that a deliberate act rather than a global appearing.
    group::GroupCoordinator& groups;
    group::OffsetStore&      offsets;

    // How many partitions __offsets has, which is what hash(group) %% N uses.
    // Immutable for the life of the data directory.
    std::int32_t offsetsPartitions = 0;

    // Reported in Metadata as the leader of every partition. One node, so it is
    // always this one — but a client reads it rather than assuming, so M8 moves
    // partitions without the client changing.
    NodeId nodeId = 0;

    // What Metadata tells clients to connect to. NOT necessarily what the socket
    // is bound to: a broker listening on 0.0.0.0 must not advertise that, because
    // a client cannot connect to it.
    std::string  advertisedHost = "127.0.0.1";
    std::int32_t advertisedPort = 0;
};

// One request, as a handler sees it.
struct RequestContext {
    const protocol::RequestHeader& header;

    // Positioned at the body — decodeRequestHeader left it exactly there, so a
    // handler reads its own fields with no offset arithmetic.
    BufferReader& body;

    // The whole frame, mutable.
    //
    // Only Produce needs it, and it needs it badly: Log::append stamps the
    // assigned offset into a produced batch in place, and the batch lives in
    // these bytes. Offsets recorded during decoding are relative to this span's
    // start, which is why it is the whole frame rather than just the body.
    std::span<std::uint8_t> frame;

    BrokerContext& broker;
};

}  // namespace dariyakyu::server
