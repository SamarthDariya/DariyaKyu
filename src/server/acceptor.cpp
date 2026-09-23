#include "server/acceptor.hpp"

#include <algorithm>

#include "common/errors.hpp"
#include <utility>

using namespace std;

namespace dariyakyu::server {

Acceptor::Acceptor(Socket listening, const ApiRegistry& registry, BrokerContext& broker)
    : listening_(std::move(listening)), registry_(registry), broker_(broker) {}

Acceptor::~Acceptor() {
    // Before any member is destroyed, so threads are joined while the things they
    // touch are still alive.
    stop();
}

void Acceptor::start() {
    acceptThread_ = thread([this] { acceptLoop(); });
}

void Acceptor::acceptLoop() {
    while (true) {
        Socket accepted = listening_.accept();

        // Checked AFTER accept returns, because that is where this thread spends
        // its life. stop() wakes it with a connection of its own, which arrives
        // here as an ordinary client and is dropped on the next line.
        if (stopping_.load(memory_order_acquire)) return;

        if (!accepted.isOpen()) return;

        // Done here rather than on a timer: a broker with many short connections
        // would otherwise accumulate one thread object per connection forever.
        reapFinished();

        auto finished   = make_shared<atomic<bool>>(false);
        auto connection = make_shared<Connection>(std::move(accepted), registry_, broker_);

        thread worker([connection, finished] {
            connection->serve();
            // Set last, so a reaper that sees this can join immediately.
            finished->store(true, memory_order_release);
        });

        lock_guard lock(servedMutex_);
        served_.push_back(Served{std::move(connection), std::move(worker), std::move(finished)});
    }
}

void Acceptor::reapFinished() {
    lock_guard lock(servedMutex_);

    auto done = remove_if(served_.begin(), served_.end(), [](Served& served) {
        if (!served.finished->load(memory_order_acquire)) return false;
        served.thread.join();
        // Destroying the Connection is what closes its socket: serve() returning
        // does not, because the Connection owns it for its whole lifetime.
        served.connection.reset();
        return true;
    });

    served_.erase(done, served_.end());
}

void Acceptor::stop() {
    if (stopping_.exchange(true, memory_order_acq_rel)) return;   // already stopping

    // 1. Wake the accept thread by being its next client.
    //
    //    Connecting to ourselves rather than closing the listening socket, which
    //    would write the descriptor member while that thread is reading it —
    //    a race TSan reports, and a real one: the number could be reused between
    //    the read and the accept() it is passed to.
    //
    //    127.0.0.1 reaches a socket bound to it or to 0.0.0.0. If the connect
    //    fails, the thread was not blocked in accept() and will see the flag on
    //    its own.
    if (acceptThread_.joinable()) {
        try {
            Socket waker = Socket::connectTo("127.0.0.1", listening_.localPort());
        } catch (const Error&) {
        }
        acceptThread_.join();
    }

    // Safe only now, with no other thread reading it.
    listening_.close();

    // 2. Unblocks every serve(), which is sitting in read().
    {
        lock_guard lock(servedMutex_);
        for (auto& served : served_)
            if (served.connection) served.connection->stop();
    }

    // 3. Join, then destroy — the destruction is what closes their sockets.
    lock_guard lock(servedMutex_);
    for (auto& served : served_) {
        if (served.thread.joinable()) served.thread.join();
        served.connection.reset();
    }
    served_.clear();
}

size_t Acceptor::connectionCount() const {
    lock_guard lock(servedMutex_);
    return served_.size();
}

}  // namespace dariyakyu::server
