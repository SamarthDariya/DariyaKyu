#include "storage/log_manager.hpp"

#include <algorithm>
#include <cctype>
#include <stdexcept>
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

TopicPartition topicPartitionFromDirName(const string& name) {
    // The LAST dash, because a topic name may contain dashes of its own.
    const size_t dash = name.rfind('-');
    if (dash == string::npos)
        throw CorruptData("data directory: '" + name + "' has no partition number");
    if (dash == 0)
        throw CorruptData("data directory: '" + name + "' has an empty topic name");
    if (dash + 1 == name.size())
        throw CorruptData("data directory: '" + name + "' ends with its separator");

    for (size_t i = dash + 1; i < name.size(); ++i)
        if (isdigit(static_cast<unsigned char>(name[i])) == 0)
            throw CorruptData("data directory: '" + name +
                              "' has a non-digit in its partition number");

    TopicPartition tp;
    tp.topic = name.substr(0, dash);
    try {
        tp.partition = stoi(name.substr(dash + 1));
    } catch (const out_of_range&) {
        throw CorruptData("data directory: '" + name +
                          "' has a partition number too large for int32");
    }

    // The round trip must be exact. "orders-03" parses as partition 3, but
    // re-encoding gives "orders-3" — so the same partition would have two valid
    // directory names, and whichever the scan met first would win while the other
    // sat there holding records nothing would ever read.
    if (tp.toString() != name)
        throw CorruptData("data directory: '" + name + "' is not the canonical name for " +
                          tp.toString());

    return tp;
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

void LogManager::loadAll() {
    unique_lock lock(logsMutex_);

    if (!logs_.empty())
        throw Error("LogManager::loadAll is a startup operation, but " +
                    to_string(logs_.size()) + " partition(s) are already open");

    for (const auto& entry : filesystem::directory_iterator(dataDir_)) {
        // Stray files at the top level are not partitions. Only directories are
        // considered, so a leftover archive or an editor's temp file is harmless.
        if (!entry.is_directory()) continue;

        const string name = entry.path().filename().string();
        if (name == kMetaDirName) continue;   // the controller's own state

        // A deletion that was interrupted. Finish it now rather than skipping it
        // forever: a restart means there are no in-flight reads to protect, so
        // the delay that removePartition observes serves no purpose here.
        if (name.ends_with(kDeletedSuffix)) {
            error_code removeEc;
            filesystem::remove_all(entry.path(), removeEc);
            continue;
        }

        const TopicPartition tp = topicPartitionFromDirName(name);

        // Log::open reads this partition's own partition.meta, so `defaults_` is a
        // fallback and not an imposition.
        logs_.emplace(tp, Log::open(tp, entry.path(), defaults_));
    }
}

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

void LogManager::removePartition(const TopicPartition& tp, int64_t nowMs) {
    unique_lock lock(logsMutex_);

    const auto it = logs_.find(tp);
    if (it == logs_.end())
        throw Error("partition " + tp.toString() + " is not hosted on this broker");

    const filesystem::path from = dataDir_ / tp.toString();
    const filesystem::path to =
        dataDir_ / (tp.toString() + "." + to_string(nowMs) + kDeletedSuffix);

    // Rename BEFORE unregistering, so a failure here leaves the partition intact
    // and still served rather than orphaned: registered nowhere, files still on
    // disk, invisible until the next restart.
    error_code ec;
    filesystem::rename(from, to, ec);
    if (ec) throw IoError("rename", from, ec.value());

    removed_.push_back({std::move(it->second), to, nowMs});
    logs_.erase(it);
}

void LogManager::runMaintenance(int64_t nowMs) {
    {
        // Shared, held across the whole loop. It blocks topic creation and
        // deletion for the duration but not lookups, so producing and consuming
        // carry on. Holding it is what keeps a Log* from being retired while this
        // is standing on it.
        //
        // Lock order is manager then partition, and nothing anywhere takes them
        // the other way round.
        shared_lock lock(logsMutex_);

        for (const auto& [tp, log] : logs_) {
            // Age-based rolling first: retention below can only delete SEALED
            // segments, so on an idle partition this is what produces something
            // for it to delete.
            log->maybeRoll(nowMs);
            log->applyRetention(nowMs);
            log->sweepGraveyard(nowMs);
        }
    }

    // Outside the loop and outside that lock, because it takes the exclusive one.
    sweepDeletedPartitions(nowMs);
}

void LogManager::sweepDeletedPartitions(int64_t nowMs) {
    unique_lock lock(logsMutex_);

    const int64_t delay = defaults_.retention.segmentDeleteDelayMs;

    erase_if(removed_, [&](RemovedPartition& removed) {
        if (nowMs - removed.removedAtMs < delay) return false;

        // The Log goes first, closing every descriptor it owns. remove_all would
        // work on open files — POSIX keeps the inode alive — but doing it in this
        // order means the space is reclaimed by the time this returns rather than
        // whenever the last handle happens to close.
        removed.log.reset();

        error_code ec;
        filesystem::remove_all(removed.directory, ec);
        // A failure leaves the renamed directory behind. It is out of the startup
        // scan's sight and the next scan finishes the job, so this does not throw
        // and stall the sweep for every other partition.
        return true;
    });
}

size_t LogManager::removedPartitionCount() const {
    shared_lock lock(logsMutex_);
    return removed_.size();
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
