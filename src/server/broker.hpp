#pragma once

#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "group/group_coordinator.hpp"
#include "group/offsets_topic.hpp"
#include "group/offset_store.hpp"
#include "server/acceptor.hpp"
#include "server/api_registry.hpp"
#include "storage/log_cleaner.hpp"
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

        // Partitions of __offsets, and therefore the number of possible
        // coordinators. Immutable once the topic exists — see offsets_topic.hpp.
        std::int32_t offsetsPartitions = group::kDefaultOffsetsPartitions;

        // How often expired members are swept. Shorter than a session timeout,
        // or a member could be gone for two timeouts before anyone notices.
        std::int64_t groupSweepIntervalMs = 1'000;

        // Compaction. Its own settings rather than per-topic, because they are
        // memory and scheduling — which belong to the process, not the data.
        storage::CleanerConfig cleaner;
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
    storage::LogCleaner& cleaner() { return cleaner_; }

private:
    void sweepGroups();

    Options             options_;

    // Declaration order is destruction order reversed, so logs_ outlives
    // everything that reads it and the sweep thread is joined before any of it
    // goes. Same reasoning as LogManager joining its own sweeper.
    storage::LogManager     logs_;

    // After logs_, so it is destroyed before them — its thread reads partitions,
    // and a cleaner outliving the LogManager would be rewriting freed segments.
    storage::LogCleaner     cleaner_;

    group::OffsetStore      offsets_;
    group::GroupCoordinator groups_;

    ApiRegistry               registry_;
    BrokerContext             context_;
    std::unique_ptr<Acceptor> acceptor_;

    // Expiry needs a thread: a group whose members have ALL vanished never sends
    // another request, so nothing else would ever notice and its partitions would
    // stay assigned to nobody. Same shape as LogManager's maintenance thread.
    std::thread             sweepThread_;
    std::mutex              sweepMutex_;
    std::condition_variable sweepWake_;
    bool                    sweepStopping_ = false;
};

}  // namespace dariyakyu::server
