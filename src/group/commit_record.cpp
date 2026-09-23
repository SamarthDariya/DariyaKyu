#include "group/commit_record.hpp"

#include "common/buffer.hpp"
#include "common/errors.hpp"
#include "protocol/wire.hpp"

using namespace std;

namespace dariyakyu::group {

namespace {

// Versioned like partition.meta, and for the same reason: these bytes outlive
// the build that wrote them. A version this build does not know is refused rather
// than half-read, because a misread offset silently moves a consumer.
constexpr int16_t kVersion = 1;

}  // namespace

vector<uint8_t> encodeCommitKey(const CommitKey& key) {
    BufferWriter out;
    out.writeInt16(kVersion);
    protocol::writeString(out, key.group);
    protocol::writeString(out, key.topic);
    out.writeInt32(key.partition);
    return out.take();
}

CommitKey decodeCommitKey(span<const uint8_t> bytes) {
    BufferReader in(bytes);

    const int16_t version = in.readInt16();
    if (version != kVersion)
        throw CorruptData("commit key: version " + to_string(version) + ", this build knows " +
                          to_string(kVersion));

    CommitKey key;
    key.group     = protocol::readString(in);
    key.topic     = protocol::readString(in);
    key.partition = in.readInt32();

    if (!in.empty())
        throw CorruptData("commit key: " + to_string(in.remaining()) + " trailing byte(s)");

    return key;
}

vector<uint8_t> encodeCommitValue(const CommitValue& value) {
    BufferWriter out;
    out.writeInt16(kVersion);
    out.writeInt64(value.nextOffset.value());
    protocol::writeString(out, value.metadata);
    out.writeInt64(value.commitTimestampMs);
    return out.take();
}

CommitValue decodeCommitValue(span<const uint8_t> bytes) {
    BufferReader in(bytes);

    const int16_t version = in.readInt16();
    if (version != kVersion)
        throw CorruptData("commit value: version " + to_string(version) +
                          ", this build knows " + to_string(kVersion));

    CommitValue value;
    value.nextOffset = Offset{in.readInt64()};

    // A negative offset is not a position a consumer can have reached. Accepting
    // one would make the next fetch ask for something before the log began, and
    // the group would silently restart from the earliest record.
    if (value.nextOffset < Offset(0))
        throw CorruptData("commit value: negative offset " + value.nextOffset.toString());

    value.metadata          = protocol::readString(in);
    value.commitTimestampMs = in.readInt64();

    if (!in.empty())
        throw CorruptData("commit value: " + to_string(in.remaining()) + " trailing byte(s)");

    return value;
}

}  // namespace dariyakyu::group
