#include "storage/log_manager.hpp"

#include <cctype>
#include <string>
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

namespace {

// A topic name becomes a directory name, so this is the boundary where a string
// chosen by a client turns into a filesystem path.
//
// The restriction is Kafka's: letters, digits, dot, underscore, hyphen. It is not
// about aesthetics. A name containing '/' would place the partition outside the
// data directory entirely, and ".." would climb out of it — so an unchecked topic
// name is a path-traversal bug waiting for the first CreateTopic request M4
// serves.
void requireUsableName(const TopicPartition& tp) {
    if (tp.topic.empty()) throw Error("partition: topic name is empty");

    if (tp.topic == "." || tp.topic == "..")
        throw Error("partition: topic name '" + tp.topic + "' is a directory reference");

    for (const char c : tp.topic) {
        const bool allowed = isalnum(static_cast<unsigned char>(c)) != 0 || c == '.' ||
                             c == '_' || c == '-';
        if (!allowed)
            throw Error("partition: topic name '" + tp.topic +
                        "' contains a character that cannot appear in a directory name");
    }

    if (tp.partition < 0)
        throw Error("partition: negative partition number " + to_string(tp.partition));
}

}  // namespace

Log& LogManager::createPartition(const TopicPartition& tp, const LogConfig& config) {
    requireUsableName(tp);

    // Exclusive for the whole operation, including the filesystem work.
    //
    // That does block every lookup on the broker for the duration — a few
    // milliseconds of mkdir, two file creations and two fsyncs. Accepted, because
    // it makes "is it already there" and "create it" atomic, and because creating
    // a partition happens when an administrator says so, not on any data path.
    // Doing the I/O outside the lock would leave a window where two callers both
    // build the same partition and one has to clean up files it already created.
    unique_lock lock(logsMutex_);

    if (logs_.count(tp) != 0)
        throw Error("partition " + tp.toString() + " is already hosted on this broker");

    // The directory name IS the partition identity — "orders-3" — so this doubles
    // as the on-disk layout rather than being a debug convenience.
    auto log = Log::create(tp, dataDir_ / tp.toString(), config);

    Log& borrowed = *log;
    logs_.emplace(tp, std::move(log));
    return borrowed;
}

Log& LogManager::createPartition(const TopicPartition& tp) {
    return createPartition(tp, defaults_);
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
