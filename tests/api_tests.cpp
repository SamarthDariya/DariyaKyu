#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include <string>
#include <vector>

#include "protocol/list_offsets.hpp"
#include "protocol/create_topic.hpp"
#include "protocol/metadata.hpp"
#include "protocol/produce.hpp"
#include "protocol/wire.hpp"
#include "test_support.hpp"

using namespace std;
using namespace dariyakyu;
using namespace dariyakyu::protocol;
using namespace dariyakyu::test;

// ===========================================================================
// Wire primitives
// ===========================================================================

namespace {

// Encode with `write`, decode with `read`, and hand back what came out. Every
// codec test below goes through this, so a field written but never read shows up
// as a leftover-bytes failure rather than as a silent mismatch later.
template <typename Encode, typename Decode>
auto roundTrip(Encode encode, Decode decode) {
    BufferWriter out;
    encode(out);
    const auto   bytes = out.take();
    BufferReader in(bytes);
    auto         decoded = decode(in);
    CHECK(in.empty());   // nothing written was left unread
    return decoded;
}

}  // namespace

TEST_CASE("A string round-trips") {
    const auto value = roundTrip([](BufferWriter& out) { writeString(out, "orders"); },
                                 [](BufferReader& in) { return readString(in); });
    CHECK(value == "orders");
}

TEST_CASE("An empty string costs two bytes") {
    BufferWriter out;
    writeString(out, "");
    const auto bytes = out.take();
    CHECK(bytes.size() == 2);

    BufferReader in(bytes);
    CHECK(readString(in).empty());
}

TEST_CASE("A null string decodes as empty") {
    // Unlike a record key, no string in this protocol means anything by being
    // absent rather than empty, so the two are treated alike.
    const auto bytes = vector<uint8_t>{0xFF, 0xFF};
    BufferReader in(bytes);
    CHECK(readString(in).empty());
}

TEST_CASE("A string length that no encoder could produce is refused") {
    const auto bytes = vector<uint8_t>{0xFF, 0xFE};   // -2
    BufferReader in(bytes);
    CHECK_THROWS_AS(readString(in), CorruptData);
}

TEST_CASE("A string running past the buffer is refused") {
    const auto bytes = vector<uint8_t>{0x7F, 0xFF, 'a'};   // claims 32767 bytes
    BufferReader in(bytes);
    CHECK_THROWS_AS(readString(in), CorruptData);
}

TEST_CASE("An element count larger than the bytes remaining is refused") {
    // A count is not a length, so readArrayLength cannot bound it against the
    // buffer. Without this, a four-byte frame can claim two billion elements and
    // the decoder reserves memory for all of them.
    BufferWriter out;
    out.writeInt32(2'000'000'000);
    const auto   bytes = out.take();
    BufferReader in(bytes);
    CHECK_THROWS_AS(readElementCount(in, "topic"), CorruptData);
}

TEST_CASE("A null element count reads as none") {
    BufferWriter out;
    out.writeInt32(-1);
    const auto   bytes = out.take();
    BufferReader in(bytes);
    CHECK(readElementCount(in, "topic") == 0);
}

// ===========================================================================
// ListOffsets
// ===========================================================================

namespace {

ListOffsetsRequest sampleListOffsetsRequest() {
    ListOffsetsRequest request;
    request.topics.push_back({"orders", {{0, kEarliestTimestamp}, {1, kLatestTimestamp}}});
    request.topics.push_back({"payments", {{7, kLatestTimestamp}}});
    return request;
}

}  // namespace

TEST_CASE("A ListOffsets request round-trips") {
    const auto decoded = roundTrip(
        [](BufferWriter& out) { encodeListOffsetsRequest(out, sampleListOffsetsRequest()); },
        [](BufferReader& in) { return decodeListOffsetsRequest(in); });

    REQUIRE(decoded.topics.size() == 2);
    CHECK(decoded.topics[0].name == "orders");
    REQUIRE(decoded.topics[0].partitions.size() == 2);
    CHECK(decoded.topics[0].partitions[0].partition == 0);
    CHECK(decoded.topics[0].partitions[0].timestamp == kEarliestTimestamp);
    CHECK(decoded.topics[0].partitions[1].partition == 1);
    CHECK(decoded.topics[0].partitions[1].timestamp == kLatestTimestamp);

    CHECK(decoded.topics[1].name == "payments");
    REQUIRE(decoded.topics[1].partitions.size() == 1);
    CHECK(decoded.topics[1].partitions[0].partition == 7);
}

TEST_CASE("A topic name is sent once however many partitions it has") {
    ListOffsetsRequest grouped;
    grouped.topics.push_back({"a-long-topic-name", {{0, -1}, {1, -1}, {2, -1}, {3, -1}}});

    ListOffsetsRequest single;
    single.topics.push_back({"a-long-topic-name", {{0, -1}}});

    BufferWriter groupedOut;
    encodeListOffsetsRequest(groupedOut, grouped);
    BufferWriter singleOut;
    encodeListOffsetsRequest(singleOut, single);

    // Three extra partitions cost 12 bytes each and not one byte of the name.
    // That is why requests are grouped by topic rather than flat — decision 9
    // makes multi-partition requests the normal case.
    CHECK(groupedOut.position() - singleOut.position() == 3 * (4 + 8));
}

TEST_CASE("A ListOffsets response round-trips, errors and all") {
    ListOffsetsResponse response;
    response.topics.push_back({"orders",
                               {{0, ErrorCode::None, Offset(1000)},
                                {1, ErrorCode::NotLeaderForPartition, Offset(0)},
                                {2, ErrorCode::UnknownTopicOrPartition, Offset(0)}}});

    const auto decoded = roundTrip(
        [&](BufferWriter& out) { encodeListOffsetsResponse(out, response); },
        [](BufferReader& in) { return decodeListOffsetsResponse(in); });

    REQUIRE(decoded.topics.size() == 1);
    REQUIRE(decoded.topics[0].partitions.size() == 3);

    // Per partition, not per request: one partition that has moved must not stop
    // the other two being answered.
    CHECK(decoded.topics[0].partitions[0].error == ErrorCode::None);
    CHECK(decoded.topics[0].partitions[0].offset == Offset(1000));
    CHECK(decoded.topics[0].partitions[1].error == ErrorCode::NotLeaderForPartition);
    CHECK(decoded.topics[0].partitions[2].error == ErrorCode::UnknownTopicOrPartition);
}

TEST_CASE("An empty ListOffsets request and response round-trip") {
    const auto request = roundTrip(
        [](BufferWriter& out) { encodeListOffsetsRequest(out, ListOffsetsRequest{}); },
        [](BufferReader& in) { return decodeListOffsetsRequest(in); });
    CHECK(request.topics.empty());

    const auto response = roundTrip(
        [](BufferWriter& out) { encodeListOffsetsResponse(out, ListOffsetsResponse{}); },
        [](BufferReader& in) { return decodeListOffsetsResponse(in); });
    CHECK(response.topics.empty());
}

TEST_CASE("A topic with no partitions round-trips") {
    ListOffsetsRequest request;
    request.topics.push_back({"orders", {}});

    const auto decoded = roundTrip(
        [&](BufferWriter& out) { encodeListOffsetsRequest(out, request); },
        [](BufferReader& in) { return decodeListOffsetsRequest(in); });

    REQUIRE(decoded.topics.size() == 1);
    CHECK(decoded.topics[0].name == "orders");
    CHECK(decoded.topics[0].partitions.empty());
}

TEST_CASE("Large offsets survive the round trip") {
    ListOffsetsResponse response;
    response.topics.push_back(
        {"orders", {{0, ErrorCode::None, Offset(9'223'372'036'854'775'807LL)}}});

    const auto decoded = roundTrip(
        [&](BufferWriter& out) { encodeListOffsetsResponse(out, response); },
        [](BufferReader& in) { return decodeListOffsetsResponse(in); });

    // int64 all the way through. A 32-bit offset would wrap after four billion
    // records, which a busy partition reaches in days.
    CHECK(decoded.topics[0].partitions[0].offset == Offset(9'223'372'036'854'775'807LL));
}

TEST_CASE("A truncated ListOffsets request is refused at every length") {
    BufferWriter out;
    encodeListOffsetsRequest(out, sampleListOffsetsRequest());
    const auto full = out.take();

    for (size_t length = 0; length < full.size(); ++length) {
        const vector<uint8_t> partial(full.begin(), full.begin() + static_cast<long>(length));
        BufferReader          in(partial);
        CHECK_THROWS_AS(decodeListOffsetsRequest(in), CorruptData);
    }
}

// ===========================================================================
// Metadata
// ===========================================================================

TEST_CASE("A Metadata request naming topics round-trips") {
    MetadataRequest request;
    request.topics = {"orders", "payments"};

    const auto decoded =
        roundTrip([&](BufferWriter& out) { encodeMetadataRequest(out, request); },
                  [](BufferReader& in) { return decodeMetadataRequest(in); });

    CHECK_FALSE(decoded.allTopics);
    REQUIRE(decoded.topics.size() == 2);
    CHECK(decoded.topics[0] == "orders");
    CHECK(decoded.topics[1] == "payments");
}

TEST_CASE("Asking for everything and asking for nothing are different requests") {
    MetadataRequest everything;
    everything.allTopics = true;

    const MetadataRequest nothing;   // no flag, no topics

    const auto decodedEverything =
        roundTrip([&](BufferWriter& out) { encodeMetadataRequest(out, everything); },
                  [](BufferReader& in) { return decodeMetadataRequest(in); });
    const auto decodedNothing =
        roundTrip([&](BufferWriter& out) { encodeMetadataRequest(out, nothing); },
                  [](BufferReader& in) { return decodeMetadataRequest(in); });

    // An explicit flag rather than Kafka's null-array-means-all convention. Those
    // two encodings differ by one byte and mean opposite things, and every other
    // array in this protocol treats null and empty alike — so relying on the
    // distinction here is how a client ends up subscribed to everything by
    // accident.
    CHECK(decodedEverything.allTopics);
    CHECK(decodedEverything.topics.empty());
    CHECK_FALSE(decodedNothing.allTopics);
    CHECK(decodedNothing.topics.empty());
}

TEST_CASE("Any non-zero byte is a true flag") {
    BufferWriter out;
    out.writeInt8(static_cast<int8_t>(0xFF));
    out.writeInt32(0);
    const auto   bytes = out.take();
    BufferReader in(bytes);

    // A client writing 0xFF for a boolean is unusual but not wrong, and refusing
    // it would be a compatibility trap for no gain.
    CHECK(decodeMetadataRequest(in).allTopics);
}

TEST_CASE("A Metadata response round-trips") {
    MetadataResponse response;
    response.brokers.push_back({1, "127.0.0.1", 9092});
    response.controllerId = 1;
    response.topics.push_back({ErrorCode::None,
                               "orders",
                               {{ErrorCode::None, 0, 1}, {ErrorCode::None, 1, 1}}});
    response.topics.push_back({ErrorCode::UnknownTopicOrPartition, "missing", {}});

    const auto decoded =
        roundTrip([&](BufferWriter& out) { encodeMetadataResponse(out, response); },
                  [](BufferReader& in) { return decodeMetadataResponse(in); });

    REQUIRE(decoded.brokers.size() == 1);
    CHECK(decoded.brokers[0].nodeId == 1);
    CHECK(decoded.brokers[0].host == "127.0.0.1");
    CHECK(decoded.brokers[0].port == 9092);
    CHECK(decoded.controllerId == 1);

    REQUIRE(decoded.topics.size() == 2);
    CHECK(decoded.topics[0].name == "orders");
    CHECK(decoded.topics[0].error == ErrorCode::None);
    REQUIRE(decoded.topics[0].partitions.size() == 2);
    CHECK(decoded.topics[0].partitions[1].partition == 1);
    CHECK(decoded.topics[0].partitions[1].leader == 1);

    // A topic error sits beside the topic, so one unknown topic does not spoil
    // the answer for the others.
    CHECK(decoded.topics[1].error == ErrorCode::UnknownTopicOrPartition);
    CHECK(decoded.topics[1].partitions.empty());
}

TEST_CASE("An unset controller round-trips as -1") {
    MetadataResponse response;   // controllerId defaults to -1

    const auto decoded =
        roundTrip([&](BufferWriter& out) { encodeMetadataResponse(out, response); },
                  [](BufferReader& in) { return decodeMetadataResponse(in); });

    // There is no controller until M8. A client reads this rather than assuming
    // any particular broker is one.
    CHECK(decoded.controllerId == -1);
    CHECK(decoded.brokers.empty());
    CHECK(decoded.topics.empty());
}

TEST_CASE("A truncated Metadata response is refused at every length") {
    MetadataResponse response;
    response.brokers.push_back({1, "127.0.0.1", 9092});
    response.topics.push_back({ErrorCode::None, "orders", {{ErrorCode::None, 0, 1}}});

    BufferWriter out;
    encodeMetadataResponse(out, response);
    const auto full = out.take();

    for (size_t length = 0; length < full.size(); ++length) {
        const vector<uint8_t> partial(full.begin(), full.begin() + static_cast<long>(length));
        BufferReader          in(partial);
        CHECK_THROWS_AS(decodeMetadataResponse(in), CorruptData);
    }
}

// ===========================================================================
// CreateTopic
// ===========================================================================

TEST_CASE("A CreateTopic request round-trips with and without overrides") {
    CreateTopicRequest request;
    request.timeoutMs = 5000;

    CreateTopicRequest::Topic plain;
    plain.name           = "orders";
    plain.partitionCount = 4;

    CreateTopicRequest::Topic configured;
    configured.name            = "audit";
    configured.partitionCount  = 1;
    configured.retentionMs     = 3'600'000;
    configured.maxSegmentBytes = 1u << 20;

    request.topics = {plain, configured};

    const auto decoded =
        roundTrip([&](BufferWriter& out) { encodeCreateTopicRequest(out, request); },
                  [](BufferReader& in) { return decodeCreateTopicRequest(in); });

    REQUIRE(decoded.topics.size() == 2);
    CHECK(decoded.timeoutMs == 5000);

    CHECK(decoded.topics[0].name == "orders");
    CHECK(decoded.topics[0].partitionCount == 4);
    CHECK_FALSE(decoded.topics[0].retentionMs.has_value());
    CHECK_FALSE(decoded.topics[0].retentionBytes.has_value());
    CHECK_FALSE(decoded.topics[0].maxSegmentBytes.has_value());

    CHECK(decoded.topics[1].name == "audit");
    CHECK(decoded.topics[1].retentionMs == 3'600'000);
    CHECK(decoded.topics[1].maxSegmentBytes == (1u << 20));
    CHECK_FALSE(decoded.topics[1].retentionBytes.has_value());
}

TEST_CASE("A nonsensical override is read as no override at all") {
    CreateTopicRequest request;
    request.topics.push_back({"orders", 1, {}, {}, {}});

    BufferWriter out;
    encodeCreateTopicRequest(out, request);
    auto bytes = out.take();

    // Overwrite the retentionMs field with zero. A client sending 0 or a
    // negative retention is not expressing a policy, it is failing to express
    // one — and adopting it would make a topic that deletes everything
    // immediately.
    const size_t retentionAt = 4 + 2 + string("orders").size() + 4;
    for (size_t i = 0; i < 8; ++i) bytes[retentionAt + i] = 0;

    BufferReader in(bytes);
    const auto   decoded = decodeCreateTopicRequest(in);
    CHECK_FALSE(decoded.topics[0].retentionMs.has_value());
}

TEST_CASE("A CreateTopic response reports per topic") {
    CreateTopicResponse response;
    response.topics = {{"orders", ErrorCode::None},
                       {"orders", ErrorCode::TopicAlreadyExists},
                       {"../escape", ErrorCode::InvalidTopic}};

    const auto decoded =
        roundTrip([&](BufferWriter& out) { encodeCreateTopicResponse(out, response); },
                  [](BufferReader& in) { return decodeCreateTopicResponse(in); });

    REQUIRE(decoded.topics.size() == 3);
    CHECK(decoded.topics[0].error == ErrorCode::None);
    CHECK(decoded.topics[1].error == ErrorCode::TopicAlreadyExists);

    // A topic name that cannot become a directory name is refused by the broker,
    // not by the codec — the codec carries whatever was sent so the error can
    // name it back.
    CHECK(decoded.topics[2].name == "../escape");
    CHECK(decoded.topics[2].error == ErrorCode::InvalidTopic);
}

TEST_CASE("An empty CreateTopic request round-trips") {
    const auto decoded =
        roundTrip([](BufferWriter& out) { encodeCreateTopicRequest(out, CreateTopicRequest{}); },
                  [](BufferReader& in) { return decodeCreateTopicRequest(in); });
    CHECK(decoded.topics.empty());
    CHECK(decoded.timeoutMs == 0);
}

TEST_CASE("A truncated CreateTopic request is refused at every length") {
    CreateTopicRequest request;
    request.topics.push_back({"orders", 2, 1000, 2000, 3000});

    BufferWriter out;
    encodeCreateTopicRequest(out, request);
    const auto full = out.take();

    for (size_t length = 0; length < full.size(); ++length) {
        const vector<uint8_t> partial(full.begin(), full.begin() + static_cast<long>(length));
        BufferReader          in(partial);
        CHECK_THROWS_AS(decodeCreateTopicRequest(in), CorruptData);
    }
}

// ===========================================================================
// Produce
// ===========================================================================

TEST_CASE("A Produce request carries batches without copying them") {
    const auto first  = makeUnstampedBatch(1000, 48);
    const auto second = makeUnstampedBatch(2000, 48);

    ProduceRequest request;
    request.acks      = 1;
    request.timeoutMs = 3000;
    request.topics.push_back({"orders",
                              {{0, span<const uint8_t>(first), 0},
                               {1, span<const uint8_t>(second), 0}}});

    BufferWriter out;
    encodeProduceRequest(out, request);
    const auto body = out.take();

    BufferReader in(body);
    const auto   decoded = decodeProduceRequest(in);
    CHECK(in.empty());

    CHECK(decoded.acks == 1);
    CHECK(decoded.timeoutMs == 3000);
    REQUIRE(decoded.topics.size() == 1);
    REQUIRE(decoded.topics[0].partitions.size() == 2);

    // Borrowed, not copied: the span points into the frame that was decoded. A
    // produce of a megabyte must not become two because the decoder wanted to
    // own its input.
    const auto& partition = decoded.topics[0].partitions[0];
    CHECK(partition.batch.size() == first.size());
    CHECK(partition.batch.data() >= body.data());
    CHECK(partition.batch.data() < body.data() + body.size());
    CHECK(vector<uint8_t>(partition.batch.begin(), partition.batch.end()) == first);
}

TEST_CASE("A decoded batch can be stamped in place") {
    const auto original = makeUnstampedBatch(1000, 48);

    ProduceRequest request;
    request.topics.push_back({"orders", {{0, span<const uint8_t>(original), 0}}});

    BufferWriter out;
    encodeProduceRequest(out, request);
    auto body = out.take();   // mutable, and owned here

    BufferReader in(body);
    const auto   decoded = decodeProduceRequest(in);

    // The broker's actual write path: recover a mutable view of bytes the
    // decoder handed out as const, and stamp the assigned offset into them.
    auto writable = mutableBatch(decoded.topics[0].partitions[0], body);
    CHECK(writable.size() == original.size());
    storage::RecordBatch::stampBaseOffset(writable, Offset(4242));

    // The stamp landed in the frame itself, and the checksum still holds —
    // because baseOffset sits before the crc field in the v2 layout.
    const auto stamped = decoded.topics[0].partitions[0].batch;
    CHECK(storage::RecordBatch::parseHeader(stamped).baseOffset == Offset(4242));
    CHECK(storage::RecordBatch::verifyCrc(stamped));
    CHECK(storage::RecordBatch::parseHeader(original).baseOffset == Offset(0));
}

TEST_CASE("A batch recorded against the wrong buffer is refused") {
    const auto batch = makeUnstampedBatch(1000, 48);

    ProduceRequest request;
    request.topics.push_back({"orders", {{0, span<const uint8_t>(batch), 0}}});

    BufferWriter out;
    encodeProduceRequest(out, request);
    auto body = out.take();

    BufferReader in(body);
    const auto   decoded = decodeProduceRequest(in);

    // Passing a buffer the batch does not lie inside is a caller mistake, and
    // the alternative to catching it is a stamp written through a wild pointer.
    vector<uint8_t> tooSmall(4);
    CHECK_THROWS_AS(mutableBatch(decoded.topics[0].partitions[0], tooSmall),
                    OffsetInvariantViolated);
}

TEST_CASE("The broker never decompresses what it is given") {
    // Arbitrary bytes standing in for a compressed body. The codec carries them
    // through untouched, because nothing in the produce path looks inside a
    // batch beyond its fixed header.
    vector<uint8_t> opaque(200);
    for (size_t i = 0; i < opaque.size(); ++i) opaque[i] = static_cast<uint8_t>(i * 7);

    ProduceRequest request;
    request.topics.push_back({"orders", {{0, span<const uint8_t>(opaque), 0}}});

    BufferWriter out;
    encodeProduceRequest(out, request);
    const auto body = out.take();

    BufferReader in(body);
    const auto   decoded = decodeProduceRequest(in);
    const auto&  carried = decoded.topics[0].partitions[0].batch;
    CHECK(vector<uint8_t>(carried.begin(), carried.end()) == opaque);
}

TEST_CASE("A negative batch length is refused") {
    BufferWriter out;
    out.writeInt16(1);        // acks
    out.writeInt32(0);        // timeoutMs
    out.writeInt32(1);        // one topic
    writeString(out, "orders");
    out.writeInt32(1);        // one partition
    out.writeInt32(0);        // partition id
    out.writeInt32(-5);       // batch length
    const auto   body = out.take();
    BufferReader in(body);

    CHECK_THROWS_AS(decodeProduceRequest(in), CorruptData);
}

TEST_CASE("A Produce response reports an offset per partition") {
    ProduceResponse response;
    response.topics.push_back({"orders",
                               {{0, ErrorCode::None, Offset(500)},
                                {1, ErrorCode::NotLeaderForPartition, kUnknownOffset},
                                {2, ErrorCode::CorruptMessage, kUnknownOffset}}});

    const auto decoded =
        roundTrip([&](BufferWriter& out) { encodeProduceResponse(out, response); },
                  [](BufferReader& in) { return decodeProduceResponse(in); });

    REQUIRE(decoded.topics[0].partitions.size() == 3);
    CHECK(decoded.topics[0].partitions[0].baseOffset == Offset(500));
    CHECK(decoded.topics[0].partitions[0].error == ErrorCode::None);

    // A partition that failed carries no offset, and says why. One moved
    // partition must not stop the other two being answered.
    CHECK(decoded.topics[0].partitions[1].error == ErrorCode::NotLeaderForPartition);
    CHECK(decoded.topics[0].partitions[1].baseOffset == kUnknownOffset);
    CHECK(decoded.topics[0].partitions[2].error == ErrorCode::CorruptMessage);
}

TEST_CASE("A truncated Produce request is refused at every length") {
    const auto batch = makeUnstampedBatch(1000, 32);

    ProduceRequest request;
    request.topics.push_back({"orders", {{0, span<const uint8_t>(batch), 0}}});

    BufferWriter out;
    encodeProduceRequest(out, request);
    const auto full = out.take();

    for (size_t length = 0; length < full.size(); ++length) {
        const vector<uint8_t> partial(full.begin(), full.begin() + static_cast<long>(length));
        BufferReader          in(partial);
        CHECK_THROWS_AS(decodeProduceRequest(in), CorruptData);
    }
}
