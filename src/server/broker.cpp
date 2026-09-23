#include "server/broker.hpp"

#include <utility>

#include "server/handlers.hpp"

using namespace std;

namespace dariyakyu::server {

Broker::Broker(Options options)
    : options_(std::move(options)),
      logs_(options_.dataDir, options_.defaults),
      context_{logs_, options_.nodeId,
               options_.advertisedHost.empty() ? options_.host : options_.advertisedHost,
               options_.port} {
    registerAllHandlers(registry_);
}

Broker::~Broker() {
    stop();
}

void Broker::start() {
    // Every partition already on disk, each with its own stored configuration
    // rather than the defaults.
    logs_.loadAll();

    Socket listening = Socket::listenOn(options_.host, options_.port);

    // Told to clients, so it has to be the port actually bound rather than the
    // one asked for — those differ whenever the request was 0.
    context_.advertisedPort = listening.localPort();

    acceptor_ = make_unique<Acceptor>(std::move(listening), registry_, context_);
    acceptor_->start();

    // Started AFTER the acceptor, so a broker that fails to bind has not already
    // spawned a thread to clean up.
    logs_.startMaintenance(options_.maintenanceIntervalMs);
}

void Broker::stop() {
    // Accepting first: no new work while the rest is winding down.
    if (acceptor_) {
        acceptor_->stop();
        acceptor_.reset();
    }

    // Then the sweep, which is the only other thread touching partitions.
    logs_.stopMaintenance();
}

int32_t Broker::port() const {
    return context_.advertisedPort;
}

}  // namespace dariyakyu::server
