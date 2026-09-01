#include "storage/log_manager.hpp"

#include <utility>

#include "common/errors.hpp"

using namespace std;

namespace dariyakyu::storage {

LogManager::LogManager(filesystem::path dataDir, LogConfig defaults)
    : dataDir_(std::move(dataDir)), defaults_(defaults) {
    // First boot creates the data directory rather than refusing to start. An
    // empty data directory and a missing one describe the same situation — a
    // broker with no partitions yet — and treating them differently would mean
    // every deployment needed a mkdir before it could run.
    //
    // A failure that is NOT "it wasn't there" does refuse: a data directory that
    // exists but cannot be written to is a problem no default fixes, and
    // discovering that at the first produce is far worse than at startup.
    error_code ec;
    filesystem::create_directories(dataDir_, ec);
    if (ec) throw IoError("create_directories", dataDir_, ec.value());
}

Log* LogManager::get(const TopicPartition& tp) const {
    shared_lock lock(logsMutex_);

    const auto it = logs_.find(tp);
    if (it == logs_.end()) return nullptr;

    // The unique_ptr stays in the map; the caller gets a borrowed view of the Log
    // it owns. Note this outlives the lock, which is safe because a Log's address
    // does not move — rehashing an unordered_map relocates nodes, not the objects
    // a unique_ptr points at. Only removePartition invalidates it.
    return it->second.get();
}

size_t LogManager::partitionCount() const {
    shared_lock lock(logsMutex_);
    return logs_.size();
}

}  // namespace dariyakyu::storage
