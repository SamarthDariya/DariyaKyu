#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "server/acceptor.hpp"
#include "server/api_registry.hpp"
#include "storage/log_manager.hpp"

namespace dariyakyu::server {

// A running broker: partitions, handlers, and a socket.
//
// Owns everything, and the DECLARATION ORDER of its members is the shutdown
// order reversed — logs_ is declared first and destroyed last, so no connection
// thread can still be serving a partition that has already been freed. That is
// the same reasoning as LogManager's destructor joining its sweeper before its
// partitions go.
class Broker {
public:
    struct Options {
        std::filesystem::path dataDir;
        storage::LogConfig    defaults;

        std::string  host = "127.0.0.1";
        std::int32_t port = 9092;

        // What Metadata tells clients. Defaults to `host`, and differs from it
        // whenever the broker binds to 0.0.0.0 — which a client cannot connect to.
        std::string advertisedHost;

        NodeId       nodeId              = 1;
        std::int64_t maintenanceIntervalMs = 30'000;
    };

    explicit Broker(Options options);
    ~Broker();

    Broker(const Broker&)            = delete;
    Broker& operator=(const Broker&) = delete;

    // Opens the data directory, starts the maintenance sweep, and begins
    // accepting. Returns once it is listening.
    void start();

    void stop();

    // The port actually bound, which is the only way to learn it after asking
    // for 0.
    std::int32_t port() const;

    storage::LogManager& logs() { return logs_; }

private:
    Options              options_;
    storage::LogManager  logs_;
    ApiRegistry          registry_;
    BrokerContext        context_;
    std::unique_ptr<Acceptor> acceptor_;
};

}  // namespace dariyakyu::server
