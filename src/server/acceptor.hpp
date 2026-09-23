#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "server/connection.hpp"

namespace dariyakyu::server {

// Accepts connections and gives each one a thread.
//
// The simplest thing that works, chosen so M4 is about the protocol rather than
// about an event loop. Every hard question — a request queue, an I/O pool,
// sendfile off the network thread, backpressure — is decision 20's territory and
// M9's milestone, with benchmarks to say whether it was worth it. A thread per
// connection is fine for the tens of connections this will ever see, and it is
// comparable enough to measure M9 against.
class Acceptor {
public:
    Acceptor(Socket listening, const ApiRegistry& registry, BrokerContext& broker);
    ~Acceptor();

    Acceptor(const Acceptor&)            = delete;
    Acceptor& operator=(const Acceptor&) = delete;

    void start();

    // Stops accepting, ends every live connection, and joins every thread.
    //
    // A blocked thread notices no flag, and the two blocked here need different
    // answers:
    //
    //   1. the accept thread is woken by CONNECTING TO THE BROKER ITSELF. It
    //      then sees the stopping flag and returns. The obvious alternative —
    //      closing the listening socket — is a data race on the descriptor
    //      member, which TSan reports: one thread writes it while the other is
    //      reading it to pass to accept(). Closing only AFTER the thread has
    //      joined is race-free, and this is what makes that ordering possible.
    //   2. connection threads are woken by shutdown() on their sockets, which
    //      makes the read they are blocked in report end of stream
    //   3. then join, and destroy the Connections — which is what finally closes
    //      their sockets, since serve() returning does not
    //
    // Idempotent, and the destructor calls it.
    void stop();

    std::int32_t port() const { return listening_.localPort(); }

    // Connections currently being served. Finished ones are reaped as new ones
    // arrive, so this is live count rather than lifetime total.
    std::size_t connectionCount() const;

private:
    void acceptLoop();
    void reapFinished();

    // A connection and the thread serving it. `finished` is set by that thread on
    // its way out, so the accept loop can join and destroy it without blocking on
    // one that is still busy.
    struct Served {
        std::shared_ptr<Connection> connection;
        std::thread                 thread;
        std::shared_ptr<std::atomic<bool>> finished;
    };

    Socket             listening_;
    const ApiRegistry& registry_;
    BrokerContext&     broker_;

    std::thread       acceptThread_;
    std::atomic<bool> stopping_{false};

    mutable std::mutex  servedMutex_;
    std::vector<Served> served_;
};

}  // namespace dariyakyu::server
