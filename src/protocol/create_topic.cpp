#include "protocol/create_topic.hpp"

#include <utility>

#include "protocol/wire.hpp"

using namespace std;

namespace dariyakyu::protocol {

namespace {

constexpr int64_t kUseDefault = -1;

void writeOverride(BufferWriter& out, const optional<int64_t>& value) {
    out.writeInt64(value.value_or(kUseDefault));
}

// Anything at or below the sentinel means "no override". A client sending 0 or a
// negative retention is not expressing a policy — it is failing to express one —
// and adopting it would produce a topic that deletes everything immediately.
optional<int64_t> readOverride(BufferReader& in) {
    const int64_t value = in.readInt64();
    if (value <= 0) return nullopt;
    return value;
}

}  // namespace

void encodeCreateTopicRequest(BufferWriter& out, const CreateTopicRequest& request) {
    out.writeInt32(static_cast<int32_t>(request.topics.size()));
    for (const auto& topic : request.topics) {
        writeString(out, topic.name);
        out.writeInt32(topic.partitionCount);
        writeOverride(out, topic.retentionMs);
        writeOverride(out, topic.retentionBytes);
        writeOverride(out, topic.maxSegmentBytes);
    }
    out.writeInt32(request.timeoutMs);
}

CreateTopicRequest decodeCreateTopicRequest(BufferReader& in) {
    CreateTopicRequest request;

    const int32_t count = readElementCount(in, "topic");
    request.topics.reserve(static_cast<size_t>(count));

    for (int32_t i = 0; i < count; ++i) {
        CreateTopicRequest::Topic topic;
        topic.name           = readString(in);
        topic.partitionCount = in.readInt32();
        topic.retentionMs    = readOverride(in);
        topic.retentionBytes = readOverride(in);
        topic.maxSegmentBytes = readOverride(in);
        request.topics.push_back(std::move(topic));
    }

    request.timeoutMs = in.readInt32();
    return request;
}

void encodeCreateTopicResponse(BufferWriter& out, const CreateTopicResponse& response) {
    out.writeInt32(static_cast<int32_t>(response.topics.size()));
    for (const auto& topic : response.topics) {
        writeString(out, topic.name);
        out.writeInt16(static_cast<int16_t>(topic.error));
    }
}

CreateTopicResponse decodeCreateTopicResponse(BufferReader& in) {
    CreateTopicResponse response;

    const int32_t count = readElementCount(in, "topic");
    response.topics.reserve(static_cast<size_t>(count));

    for (int32_t i = 0; i < count; ++i) {
        CreateTopicResponse::Topic topic;
        topic.name  = readString(in);
        topic.error = static_cast<ErrorCode>(in.readInt16());
        response.topics.push_back(std::move(topic));
    }

    return response;
}

}  // namespace dariyakyu::protocol
