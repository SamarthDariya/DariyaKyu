#include "server/broker.hpp"

#include <chrono>
#include <utility>

#include "group/offsets_topic.hpp"
#include "server/handlers.hpp"

using namespace std;

namespace dariyakyu::server {

Broker::Broker(Options options)
    : options_(std::move(options)),
      logs_(options_.dataDir, options_.defaults),
      cleaner_(logs_, options_.cleaner),
      offsets_(logs_, options_.offsetsPartitions),
      replicas_(options_.nodeId, options_.replicaLagTimeMaxMs),
      fetchers_([this] {
          replication::ReplicaFetcher::Options fetching;
          fetching.self = options_.nodeId;
          return fetching;
      }()),
      context_{logs_,
               groups_,
               offsets_,
               options_.offsetsPartitions,
               options_.nodeId,
               options_.advertisedHost.empty() ? options_.host : options_.advertisedHost,
               options_.port,
               options_.cluster,
               replicas_,
               fetchers_} {
    registerAllHandlers(registry_);
}

Broker::~Broker() {
    stop();
}

void Broker::start() {
    // Every partition already on disk, each with its own stored configuration
    // rather than the defaults.
    logs_.loadAll();

    // Before anything can be served: FindCoordinator needs the topic to exist,
    // and a coordinator answering before its offsets are loaded would tell a
    // group it had never committed.
    context_.offsetsPartitions = group::ensureOffsetsTopic(logs_, options_.offsetsPartitions);
    offsets_.replay();

    // Every partition already on disk, before any request can reach one: those
    // led here with followers are tracked, so only their followers' fetches
    // commit them; those followed here start being copied from their leader.
    for (const auto& tp : logs_.hostedPartitions())
        if (storage::Log* log = logs_.get(tp)) adopt(context_, *log);

    protocol::Socket listening = protocol::Socket::listenOn(options_.host, options_.port);

    // Told to clients, so it has to be the port actually bound rather than the
    // one asked for — those differ whenever the request was 0.
    context_.advertisedPort = listening.localPort();

    acceptor_ = make_unique<Acceptor>(std::move(listening), registry_, context_);
    acceptor_->start();

    // Started AFTER the acceptor, so a broker that fails to bind has not already
    // spawned a thread to clean up.
    logs_.startMaintenance(options_.maintenanceIntervalMs);
    cleaner_.startCleaning();

    sweepThread_ = thread([this] { sweep(); });
}

void Broker::sweep() {
    unique_lock lock(sweepMutex_);
    while (true) {
        if (sweepWake_.wait_for(lock, chrono::milliseconds(options_.sweepIntervalMs),
                                [this] { return sweepStopping_; }))
            return;

        lock.unlock();
        const int64_t now = storage::wallClockMillis();
        try {
            groups_.expire(now);
        } catch (...) {
            // One bad group must not stop every other being swept, and a sweep
            // that died silently would leave partitions assigned to members that
            // no longer exist.
        }
        try {
            // A dead follower never fetches, so only a timer can notice it. Until
            // it is dropped, every consumer of its partitions is frozen at
            // whatever it last copied.
            replicas_.shrinkInSync(now);
        } catch (...) {
            // Separately caught: a failure here must not stop groups expiring,
            // and the reverse.
        }
        lock.lock();
    }
}

void Broker::stop() {
    // Accepting first: no new work while the rest is winding down.
    if (acceptor_) {
        acceptor_->stop();
        acceptor_.reset();
    }

    // Then the threads copying from other brokers: they append to logs, and the
    // logs must still be open while a fetch in flight finishes.
    fetchers_.stop();

    {
        lock_guard lock(sweepMutex_);
        sweepStopping_ = true;
    }
    sweepWake_.notify_all();
    if (sweepThread_.joinable()) sweepThread_.join();

    // Then the two threads that touch partitions. The cleaner first: it is the
    // one that rewrites files, and a pass in flight has to finish before the
    // sweeper can free anything it is standing on.
    cleaner_.stopCleaning();
    logs_.stopMaintenance();
}

int32_t Broker::port() const {
    return context_.advertisedPort;
}

}  // namespace dariyakyu::server
