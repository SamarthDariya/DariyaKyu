#include "storage/partition_meta.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
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
