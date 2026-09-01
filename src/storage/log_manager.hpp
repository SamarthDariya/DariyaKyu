#pragma once

#include <cstddef>
#include <filesystem>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>
#include <unordered_map>

#include "common/types.hpp"
#include "storage/log.hpp"

namespace dariyakyu::storage {

// "orders-3" -> TopicPartition{"orders", 3}. The inverse of
// TopicPartition::toString(), and the reason that function's comment warns the
// encoding is ambiguous in reverse: a topic name may itself contain dashes, so
// "my-topic-5" could split three ways. It always splits at the LAST dash, which
// is what Kafka does and what makes the round trip well-defined.
//
// Strict, and throws CorruptData on anything else. Notably it requires the round
// trip to be exact, which rejects "orders-03": that parses as partition 3, but
// re-encoding gives "orders-3", so the same partition would have two directory
// names and whichever the scan met first would win.
TopicPartition topicPartitionFromDirName(const std::string& name);

// A partition directory being deleted is renamed to end in this, which takes it
// out of the startup scan's sight in one atomic step. See
// LogManager::removePartition.
inline constexpr const char* kDeletedSuffix = ".delete";

// Reserved for the controller's own state (data/meta/cluster.meta), so it is not
// a partition and the startup scan steps over it.
inline constexpr const char* kMetaDirName = "meta";

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

    // Opens every partition already on disk. The startup scan.
    //
    // Each partition's own configuration is read from its partition.meta, so this
    // does not impose the manager's defaults on partitions that were configured
    // differently — the defaults apply only to a partition that has no meta file
    // and no records, which is what an interrupted create leaves behind.
    //
    // Strict: a directory it cannot interpret as a partition aborts the scan
    // rather than being skipped. `data/` belongs to dariyakyu, and silently
    // ignoring a directory that should have been a partition is the worst
    // outcome — the data stays on disk, invisible, while consumers get "not
    // hosted here" forever with nothing to explain it.
    //
    // Startup only. Calling it twice would try to open partitions that are
    // already open, putting two writable handles on one active segment.
    void loadAll();

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

    // Stops hosting a partition and deletes its files.
    //
    // The directory is RENAMED first, to `<name>.<timestamp>.delete`, and only
    // then dropped. Two reasons, and both are about being interrupted:
    //
    //   - the rename is one atomic step, so the partition stops being visible to
    //     a startup scan the instant it begins disappearing. Unlinking files one
    //     by one and then crashing would leave a directory that still looks like
    //     a partition but has a hole in its offset sequence, which Log::open
    //     refuses — turning a topic deletion into a broker that will not start.
    //   - the timestamp makes the name unique, so a partition deleted, recreated,
    //     and deleted again does not collide with its own earlier corpse.
    //
    // Destruction is then deferred, for the same reason retention defers it: a
    // FileRange already handed out names a descriptor this Log owns, and closing
    // it early would fail the send or, worse, let the number be reused by another
    // file. sweepDeletedPartitions does the freeing.
    //
    // Any Log* previously returned by get() for this partition dangles from here
    // on. Nothing above this layer may cache one.
    void removePartition(const TopicPartition& tp, std::int64_t nowMs);

    // One pass of housekeeping over every partition on the broker.
    //
    // Four jobs, and the FIRST is the one that is easy to forget: a partition
    // receiving no writes never calls append, so nothing re-evaluates its age.
    // Its active segment would never seal, and retention only ever deletes sealed
    // segments — so an idle topic would keep its data forever. Age-based rolling
    // has to be driven by the sweeper as well as by the write path
    // (DESIGN.md decision 11).
    //
    // The other three are retention itself, and draining the two graveyards:
    // segments retention deleted, and partitions an administrator deleted.
    //
    // Approximate by design. A partition may sit over its byte limit, or a
    // segment outlive its window, until the next pass.
    void runMaintenance(std::int64_t nowMs);

    // Frees partitions whose deletion delay has elapsed, and removes their
    // renamed directories.
    void sweepDeletedPartitions(std::int64_t nowMs);

    std::size_t removedPartitionCount() const;

    std::size_t partitionCount() const;

    const std::filesystem::path& dataDir() const { return dataDir_; }
    const LogConfig&             defaults() const { return defaults_; }

    LogManager(const LogManager&)            = delete;
    LogManager& operator=(const LogManager&) = delete;

private:
    std::filesystem::path dataDir_;
    LogConfig             defaults_;

    std::unordered_map<TopicPartition, std::unique_ptr<Log>> logs_;

    // Partitions whose directories are renamed away but whose descriptors are
    // still open. The manager's counterpart to Log's segment graveyard.
    struct RemovedPartition {
        std::unique_ptr<Log>  log;
        std::filesystem::path directory;   // the renamed one
        std::int64_t          removedAtMs = 0;
    };
    std::vector<RemovedPartition> removed_;

    // mutable so const lookups can take a shared lock. Guards which entries
    // exist — never the contents of a Log, which does its own.
    mutable std::shared_mutex logsMutex_;
};

}  // namespace dariyakyu::storage
