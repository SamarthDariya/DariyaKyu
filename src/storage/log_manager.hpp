#pragma once

#include <cstddef>
#include <filesystem>
#include <memory>
#include <shared_mutex>
#include <unordered_map>

#include "common/types.hpp"
#include "storage/log.hpp"

namespace dariyakyu::storage {

// Every partition this broker hosts.
//
// One level above Log, and the last purely-storage object: from M4 the request
// handler asks this "do I host orders-3?" and then works with the Log it gets
// back. Nothing above this layer knows what a segment is.
//
// # Why unordered_map here, when Log uses map
//
// Log's question is "which segment holds offset N" — a range query, which needs
// ordering. This layer's question is "do I host exactly this partition" — an
// exact match, where hashing beats a tree. `types.hpp` already provides
// hash<TopicPartition>, with proper mixing, because a broker's keys are
// dominated by one topic with many partitions: without mixing, every partition of
// a topic would hash to adjacent buckets.
//
// # Why the lock is barely used
//
// logsMutex_ guards the REGISTRY — which partitions exist — and nothing else.
// Creating or deleting a topic takes it exclusively; that happens when an
// administrator says so, not on any data path. Producing and consuming take it
// briefly to look a partition up and then work entirely inside that Log, which
// has its own, equally thin, locking.
class LogManager {
public:
    // `defaults` applies to partitions created from here on, and to a partition
    // found on disk with no partition.meta of its own. A partition that has one
    // keeps what it was configured with — see Log::open.
    LogManager(std::filesystem::path dataDir, LogConfig defaults);

    // The Log for `tp`, or nullptr if this broker does not host it.
    //
    // A raw pointer, deliberately: a NON-OWNING observer. LogManager owns every
    // Log for the broker's lifetime and callers borrow. A shared_ptr would imply
    // a lifetime question that does not exist and put a refcount on every lookup.
    //
    // Valid until removePartition() is called for that partition. Nothing above
    // this layer may hold one across a topic deletion — which is why M4's
    // request handler will look up per request rather than caching.
    Log* get(const TopicPartition& tp) const;

    // Creates a partition, registers it, and returns it.
    //
    // A reference rather than a pointer: on success it always exists, and a
    // pointer would invite a null check that can never fire. Non-owning either
    // way — the manager keeps the unique_ptr.
    //
    // Throws if the partition is already hosted here. That is a caller bug rather
    // than a race: the controller decides where partitions live, so asking twice
    // means its view and the broker's have diverged, and quietly returning the
    // existing one would hide that. M4 maps it to TOPIC_ALREADY_EXISTS.
    Log& createPartition(const TopicPartition& tp, const LogConfig& config);

    // Same, with the manager's defaults.
    Log& createPartition(const TopicPartition& tp);

    std::size_t partitionCount() const;

    const std::filesystem::path& dataDir() const { return dataDir_; }
    const LogConfig&             defaults() const { return defaults_; }

    LogManager(const LogManager&)            = delete;
    LogManager& operator=(const LogManager&) = delete;

private:
    std::filesystem::path dataDir_;
    LogConfig             defaults_;

    std::unordered_map<TopicPartition, std::unique_ptr<Log>> logs_;

    // mutable so const lookups can take a shared lock. Guards which entries
    // exist — never the contents of a Log, which does its own.
    mutable std::shared_mutex logsMutex_;
};

}  // namespace dariyakyu::storage
