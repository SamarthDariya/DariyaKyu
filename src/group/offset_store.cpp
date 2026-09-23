#include "group/offset_store.hpp"

#include <unistd.h>

#include <cerrno>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "common/errors.hpp"
#include "group/offsets_topic.hpp"
#include "storage/record_batch.hpp"

using namespace std;

namespace dariyakyu::group {

namespace {

// One commit as record bytes, ready to append.
vector<uint8_t> batchFor(const CommitKey& key, const optional<CommitValue>& value,
                         int64_t timestampMs) {
    const auto keyBytes = encodeCommitKey(key);

    storage::RecordBatchBuilder builder;
    if (value) {
        const auto valueBytes = encodeCommitValue(*value);
        builder.append(timestampMs, span<const uint8_t>(keyBytes), span<const uint8_t>(valueBytes));
    } else {
        // A tombstone. nullopt, not an empty vector — an empty value is a commit
        // of offset zero.
        builder.append(timestampMs, span<const uint8_t>(keyBytes), nullopt);
    }

    return builder.build();
}

}  // namespace

OffsetStore::OffsetStore(storage::LogManager& logs, int32_t partitions)
    : logs_(logs), partitions_(partitions) {}

PartitionId OffsetStore::partitionFor(const string& group) const {
    return coordinatorPartition(group, partitions_);
}

void OffsetStore::commit(const CommitKey& key, const CommitValue& value) {
    storage::Log* log = logs_.get(TopicPartition{kOffsetsTopic, partitionFor(key.group)});
    if (log == nullptr)
        throw Error("__offsets partition for group '" + key.group + "' is not hosted here");

    auto batch = batchFor(key, value, value.commitTimestampMs);

    // Appended FIRST. A map updated before a failed append would report a
    // position that does not survive a restart, which is the one thing a
    // committed offset must never do.
    log->append(batch);

    unique_lock lock(mutex_);
    committed_[key] = value;
}

void OffsetStore::forget(const CommitKey& key) {
    storage::Log* log = logs_.get(TopicPartition{kOffsetsTopic, partitionFor(key.group)});
    if (log == nullptr)
        throw Error("__offsets partition for group '" + key.group + "' is not hosted here");

    auto batch = batchFor(key, nullopt, storage::wallClockMillis());
    log->append(batch);

    unique_lock lock(mutex_);
    committed_.erase(key);
}

optional<CommitValue> OffsetStore::fetch(const CommitKey& key) const {
    shared_lock lock(mutex_);

    const auto found = committed_.find(key);
    if (found == committed_.end()) return nullopt;
    return found->second;
}

size_t OffsetStore::size() const {
    shared_lock lock(mutex_);
    return committed_.size();
}

void OffsetStore::replayPartition(storage::Log& log) {
    Offset offset = log.logStartOffset();

    while (offset < log.logEndOffset()) {
        const storage::ReadResult result = log.read(offset, 1u << 20);

        // A partition whose start moved under us, or one with nothing left.
        // Neither is an error here: replay is reconstructing a map, not serving
        // a consumer.
        if (!result.ok() || result.range.empty()) break;

        vector<uint8_t> bytes(result.range.length);
        const ssize_t   got = ::pread(result.range.fd, bytes.data(), bytes.size(),
                                      static_cast<off_t>(result.range.position));
        if (got < 0) throw IoError("pread", "__offsets", errno);
        bytes.resize(static_cast<size_t>(got));

        size_t position = 0;
        bool   advanced = false;

        while (position < bytes.size()) {
            const auto remaining = span<const uint8_t>(bytes).subspan(position);

            size_t total = 0;
            try {
                total = storage::RecordBatch::totalSizeOf(remaining);
            } catch (const CorruptData&) {
                break;
            }
            // A response may end mid-batch; the tail is read again next time
            // round with a fresh range.
            if (total > remaining.size()) break;

            const auto batch  = remaining.subspan(0, total);
            const auto header = storage::RecordBatch::parseHeader(batch);

            for (const auto& record : storage::RecordBatch::decodeRecords(batch)) {
                // A record with no key is not a commit. Nothing writes one today,
                // but this topic is ordinary and anything could have.
                if (!record.key) continue;

                const CommitKey key = decodeCommitKey(*record.key);

                // Last write wins, in log order — which is exactly what
                // compaction will collapse the topic down to, so a replayed map
                // and a compacted topic agree by construction.
                unique_lock lock(mutex_);
                if (record.value)
                    committed_[key] = decodeCommitValue(*record.value);
                else
                    committed_.erase(key);   // a tombstone
            }

            offset   = header.lastOffset() + 1;
            position += total;
            advanced = true;
        }

        // Nothing decoded from a non-empty range would loop forever otherwise.
        if (!advanced) break;
    }
}

void OffsetStore::replay() {
    {
        unique_lock lock(mutex_);
        committed_.clear();
    }

    for (PartitionId p = 0; p < partitions_; ++p) {
        storage::Log* log = logs_.get(TopicPartition{kOffsetsTopic, p});
        if (log == nullptr) continue;
        replayPartition(*log);
    }
}

}  // namespace dariyakyu::group
