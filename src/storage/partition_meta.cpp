#include "storage/partition_meta.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <optional>
#include <string>

#include "common/buffer.hpp"
#include "common/errors.hpp"
#include "common/file_handle.hpp"

using namespace std;

namespace dariyakyu::storage {

namespace {

constexpr int64_t kUnlimitedBytes = -1;

// fsync a directory, which is what makes a rename inside it durable. Renaming
// changes a directory entry, and that change lives in the directory's own data
// until it is flushed — so without this, a crash can undo the rename and leave
// the old partition.meta in place.
//
// Opened read-only: a directory cannot be opened for writing, and fsync does not
// require it.
void syncDirectory(const filesystem::path& dir) {
    const int fd = ::open(dir.c_str(), O_RDONLY);
    if (fd < 0) throw IoError("open", dir, errno);
    const int rc    = ::fsync(fd);
    const int saved = errno;
    ::close(fd);
    if (rc < 0) throw IoError("fsync", dir, saved);
}

}  // namespace

vector<uint8_t> encodePartitionMeta(const PartitionMeta& meta) {
    BufferWriter out;

    out.writeUint32(PartitionMeta::kMagic);
    out.writeInt16(PartitionMeta::kVersion);

    // Length-prefixed rather than NUL-terminated: topic names come from clients,
    // and a length says exactly how many bytes to read without trusting the
    // content to contain a terminator.
    const string& topic = meta.tp.topic;
    out.writeInt16(static_cast<int16_t>(topic.size()));
    out.writeBytes({reinterpret_cast<const uint8_t*>(topic.data()), topic.size()});
    out.writeInt32(meta.tp.partition);

    out.writeInt64(static_cast<int64_t>(meta.config.roll.maxSegmentBytes));
    out.writeInt64(meta.config.roll.maxSegmentAgeMs);
    out.writeInt64(static_cast<int64_t>(meta.config.roll.indexIntervalBytes));
    out.writeInt64(static_cast<int64_t>(meta.config.roll.maxIndexBytes));

    out.writeInt64(meta.config.retention.retentionMs);
    // The one place the optional becomes a sentinel.
    out.writeInt64(meta.config.retention.retentionBytes
                       ? static_cast<int64_t>(*meta.config.retention.retentionBytes)
                       : kUnlimitedBytes);
    out.writeInt64(meta.config.retention.segmentDeleteDelayMs);

    return out.take();
}

PartitionMeta decodePartitionMeta(span<const uint8_t> bytes) {
    // BufferReader throws CorruptData of its own accord the moment a read runs
    // past the end, so a truncated file needs no length checks here.
    BufferReader in(bytes);

    const uint32_t magic = in.readUint32();
    if (magic != PartitionMeta::kMagic)
        throw CorruptData("partition.meta: magic " + to_string(magic) + ", expected " +
                          to_string(PartitionMeta::kMagic) + " — this is not a partition.meta");

    const int16_t version = in.readInt16();
    if (version != PartitionMeta::kVersion)
        throw CorruptData("partition.meta: version " + to_string(version) +
                          ", this build understands only " + to_string(PartitionMeta::kVersion));

    PartitionMeta meta;

    const int16_t topicLength = in.readInt16();
    if (topicLength <= 0)
        throw CorruptData("partition.meta: topic name length " + to_string(topicLength));
    const auto topicBytes = in.readBytes(static_cast<size_t>(topicLength));
    meta.tp.topic.assign(reinterpret_cast<const char*>(topicBytes.data()), topicBytes.size());

    meta.tp.partition = in.readInt32();
    if (meta.tp.partition < 0)
        throw CorruptData("partition.meta: partition " + to_string(meta.tp.partition));

    meta.config.roll.maxSegmentBytes    = static_cast<uint64_t>(in.readInt64());
    meta.config.roll.maxSegmentAgeMs    = in.readInt64();
    meta.config.roll.indexIntervalBytes = static_cast<uint64_t>(in.readInt64());
    meta.config.roll.maxIndexBytes      = static_cast<size_t>(in.readInt64());

    meta.config.retention.retentionMs = in.readInt64();

    // The sentinel comes back to an optional here, the mirror of encode.
    const int64_t retentionBytes = in.readInt64();
    meta.config.retention.retentionBytes =
        (retentionBytes == kUnlimitedBytes) ? nullopt
                                            : optional<uint64_t>(static_cast<uint64_t>(retentionBytes));

    meta.config.retention.segmentDeleteDelayMs = in.readInt64();

    // Bytes left over mean this is not the file it claims to be — most likely
    // one that was appended to rather than replaced. The version matched, so
    // there is no forward-compatibility story that explains a longer file.
    if (!in.empty())
        throw CorruptData("partition.meta: " + to_string(in.remaining()) +
                          " unexpected trailing byte(s)");

    return meta;
}

PartitionMeta readPartitionMeta(const filesystem::path& dir, const TopicPartition& expected) {
    const filesystem::path path = dir / PartitionMeta::kFileName;

    FileHandle      handle(path, FileHandle::Mode::ReadOnly);
    vector<uint8_t> bytes(handle.size());
    if (handle.readAt(0, bytes) < bytes.size())
        throw CorruptData("partition.meta: " + path.string() + " ended early");

    PartitionMeta meta = decodePartitionMeta(bytes);

    if (meta.tp != expected)
        throw CorruptData("partition.meta: " + path.string() + " describes " +
                          meta.tp.toString() + " but was found in " + expected.toString() +
                          " — the directory was copied or renamed");

    return meta;
}

void writePartitionMeta(const filesystem::path& dir, const PartitionMeta& meta) {
    const filesystem::path target = dir / PartitionMeta::kFileName;
    const filesystem::path temp   = dir / (string(PartitionMeta::kFileName) + ".tmp");

    const vector<uint8_t> bytes = encodePartitionMeta(meta);

    {
        FileHandle handle(temp, FileHandle::Mode::ReadWrite);

        // A leftover temp file from an interrupted write would otherwise be
        // appended to, producing a file with two records in it.
        handle.truncate(0);
        handle.append(bytes);

        // The rename below makes the NAME change atomically; it says nothing
        // about the bytes having reached the disk. Without this fsync, a crash
        // can leave a correctly named file full of zeroes.
        handle.sync();
    }

    error_code ec;
    filesystem::rename(temp, target, ec);
    if (ec) throw IoError("rename", temp, ec.value());

    // And the rename itself is a directory change, which needs flushing too.
    syncDirectory(dir);
}

}  // namespace dariyakyu::storage
