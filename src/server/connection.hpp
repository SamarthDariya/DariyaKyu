#pragma once

#include <cstddef>

#include "protocol/frame.hpp"
#include "server/api_registry.hpp"
#include "protocol/socket.hpp"

namespace dariyakyu::server {

// One client connection, served by one thread.
//
// Reads a frame, dispatches it, writes the response, repeats — until the peer
// closes or the stream stops making sense. That is the whole of M4's threading
// model: every hard question about request queues, I/O pools and backpressure is
// decision 20's territory and M9's milestone.
class Connection {
public:
    Connection(protocol::Socket socket, const ApiRegistry& registry, BrokerContext& broker,
               std::size_t maxFrameBytes = protocol::kMaxFrameBytes);

    // Blocks until the connection ends. Never throws: a connection failing is
    // routine, and taking the broker down with it would let one client stop
    // every other.
    //
    // Returning does NOT close the socket — this object owns it for its whole
    // lifetime, so the peer sees no end of stream until this is destroyed or
    // stop() is called. Whatever owns the Connection has to do one of those, or
    // a client that sent something unanswerable waits forever for a hangup that
    // never comes.
    void serve();

    // Unblocks a thread sitting in serve(), from another thread.
    void stop();

    int fd() const { return socket_.fd(); }

private:
    protocol::Socket             socket_;
    const ApiRegistry& registry_;
    BrokerContext&     broker_;
    std::size_t        maxFrameBytes_;
};

}  // namespace dariyakyu::server
