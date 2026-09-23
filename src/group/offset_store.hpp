#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <shared_mutex>

#include "group/commit_record.hpp"
#include "storage/log_manager.hpp"

namespace dariyakyu::group {

// Where every group has got to, served from memory and durable in __offsets.
//
// Decision 21's shape exactly: committing is an append plus a map update, and
// reading never touches disk. The map is not a cache in front of the topic — it
// IS the answer, and the topic is how it survives a restart.
//
// That makes replay the only reason this ever reads its own topic, and it is
// dariyanache's AOF replay down to the shape of the loop.
class OffsetStore {
public:
    OffsetStore(storage::LogManager& logs, std::int32_t partitions);

    // Appends the commit, then updates the map.
    //
    // That order, not the reverse: a map updated before a failed append would
    // report a position that does not survive a restart, which is the one thing a
    // committed offset must never do.
    void commit(const CommitKey& key, const CommitValue& value);

    // Forgets a position, by writing a tombstone — a record with a NULL value.
    //
    // Null rather than empty, and that distinction has been load-bearing since
    // M1: an empty value is a commit of offset zero, a null one is "this group
    // no longer reads this partition". Compaction removes a tombstone and every
    // commit before it; collapsing the two would resurrect a position instead.
    void forget(const CommitKey& key);

    std::optional<CommitValue> fetch(const CommitKey& key) const;

    // Rebuilds the map by reading every __offsets partition from the beginning.
    //
    // Last write wins, in log order — which is exactly what compaction will later
    // collapse the topic down to, so a replayed map and a compacted topic agree
    // by construction rather than by agreement.
    void replay();

    std::size_t size() const;

private:
    PartitionId partitionFor(const std::string& group) const;
    void        replayPartition(storage::Log& log);

    storage::LogManager& logs_;
    std::int32_t         partitions_;

    mutable std::shared_mutex        mutex_;
    std::map<CommitKey, CommitValue> committed_;
};

}  // namespace dariyakyu::group
