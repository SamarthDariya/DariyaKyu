#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include <string>
#include <vector>

#include "protocol/fetch.hpp"
#include "protocol/group_apis.hpp"
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

// ===========================================================================
// Fetch
// ===========================================================================

namespace {

// A partition with real batches, so a fetch response can carry ranges that point
// at bytes something actually wrote.
struct FetchFixture {
    TempDir                  dir;
    unique_ptr<storage::Log> log;
    explicit FetchFixture(const string& name, int records = 20) : dir(name) {
        log = storage::Log::create(TopicPartition{"orders", 0}, dir.file("orders-0"),
                                   testConfig());
        for (int i = 0; i < records; ++i) {
            auto bytes = makeUnstampedBatch(1000 + i, 48);
            log->append(bytes);
        }
    }
};

}  // namespace

TEST_CASE("A Fetch request round-trips") {
    FetchRequest request;
    request.maxWaitMs = 500;
    request.minBytes  = 1;
    request.topics.push_back({"orders", {{0, Offset(100), 1 << 20}, {1, Offset(0), 4096}}});

    const auto decoded =
        roundTrip([&](BufferWriter& out) { encodeFetchRequest(out, request); },
                  [](BufferReader& in) { return decodeFetchRequest(in); });

    CHECK(decoded.maxWaitMs == 500);
    CHECK(decoded.minBytes == 1);
    REQUIRE(decoded.topics[0].partitions.size() == 2);
    CHECK(decoded.topics[0].partitions[0].fetchOffset == Offset(100));
    CHECK(decoded.topics[0].partitions[0].maxBytes == (1 << 20));
    CHECK(decoded.topics[0].partitions[1].fetchOffset == Offset(0));
}

TEST_CASE("A negative fetch size is refused rather than clamped") {
    BufferWriter out;
    out.writeInt32(0);
    out.writeInt32(0);
    out.writeInt32(1);
    writeString(out, "orders");
    out.writeInt32(1);
    out.writeInt32(0);
    out.writeInt64(0);
    out.writeInt32(-1);   // maxBytes
    const auto   body = out.take();
    BufferReader in(body);

    // Not a conservative client, a broken one. Clamping would hide it.
    CHECK_THROWS_AS(decodeFetchRequest(in), CorruptData);
}

TEST_CASE("A fetch response puts records in file segments, not buffers") {
    FetchFixture fixture("fetch-segments");
    const auto   result = fixture.log->read(Offset(0), kBigFetch);
    REQUIRE(result.ok());
    REQUIRE(result.range.length > 0);

    FetchResponse response;
    response.topics.push_back(
        {"orders",
         {{0, ErrorCode::None, fixture.log->logEndOffset(), Offset(0), result.range}}});

    Response wire;
    encodeFetchResponse(wire, response);

    // Metadata in a buffer, records as a range. The records were never read.
    REQUIRE(wire.segments().size() == 2);
    CHECK_FALSE(wire.segments()[0].isFile());
    CHECK(wire.segments()[1].isFile());
    CHECK(wire.segments()[1].range.length == result.range.length);
    CHECK(wire.segments()[1].buffer.empty());
}

TEST_CASE("A fetch response round-trips through the wire") {
    FetchFixture fixture("fetch-roundtrip");
    const auto   result = fixture.log->read(Offset(5), kBigFetch);
    REQUIRE(result.ok());

    FetchResponse response;
    response.topics.push_back({"orders",
                               {{0, ErrorCode::None, fixture.log->logEndOffset(),
                                 fixture.log->logStartOffset(), result.range}}});

    Response wire;
    encodeFetchResponse(wire, response);

    // What a client receives: the segments, flattened by the kernel on the way
    // out and by the socket on the way in.
    const auto   frame = wire.materialise();
    BufferReader in(frame);
    const auto   view = decodeFetchResponse(in);
    CHECK(in.empty());

    REQUIRE(view.topics.size() == 1);
    REQUIRE(view.topics[0].partitions.size() == 1);
    const auto& partition = view.topics[0].partitions[0];

    CHECK(partition.error == ErrorCode::None);
    CHECK(partition.highWatermark == fixture.log->logEndOffset());
    CHECK(partition.records.size() == result.range.length);

    // And the records are a real batch containing the offset asked for.
    CHECK(storage::RecordBatch::verifyCrc(partition.records));
    const auto header = storage::RecordBatch::parseHeader(partition.records);
    CHECK(header.baseOffset <= Offset(5));
    CHECK(header.lastOffset() >= Offset(5));
}

TEST_CASE("Several partitions interleave metadata and records") {
    FetchFixture fixture("fetch-multi", 40);
    const auto   first  = fixture.log->read(Offset(0), 300);
    const auto   second = fixture.log->read(Offset(20), 300);
    REQUIRE(first.ok());
    REQUIRE(second.ok());

    FetchResponse response;
    response.topics.push_back({"orders",
                               {{0, ErrorCode::None, Offset(40), Offset(0), first.range},
                                {1, ErrorCode::None, Offset(40), Offset(0), second.range}}});

    Response wire;
    encodeFetchResponse(wire, response);

    // buffer, records, buffer, records — the alternation the whole design exists
    // to make possible.
    REQUIRE(wire.segments().size() == 4);
    CHECK_FALSE(wire.segments()[0].isFile());
    CHECK(wire.segments()[1].isFile());
    CHECK_FALSE(wire.segments()[2].isFile());
    CHECK(wire.segments()[3].isFile());

    const auto   frame = wire.materialise();
    BufferReader in(frame);
    const auto   view = decodeFetchResponse(in);
    CHECK(view.topics[0].partitions[0].records.size() == first.range.length);
    CHECK(view.topics[0].partitions[1].records.size() == second.range.length);
}

TEST_CASE("A fetch with nothing waiting is one buffer segment and no ranges") {
    FetchFixture fixture("fetch-caught-up", 5);

    FetchResponse response;
    FetchResponse::Topic topic;
    topic.name = "orders";
    for (PartitionId p = 0; p < 12; ++p)
        topic.partitions.push_back({p, ErrorCode::None, Offset(5), Offset(0), FileRange{}});
    response.topics.push_back(topic);

    Response wire;
    encodeFetchResponse(wire, response);

    // The most common fetch in the system, and it costs one segment rather than
    // twelve — which is why metadata accumulates and only flushes when a range
    // interrupts it.
    CHECK(wire.segments().size() == 1);
    CHECK_FALSE(wire.segments().front().isFile());

    const auto   frame = wire.materialise();
    BufferReader in(frame);
    const auto   view = decodeFetchResponse(in);
    REQUIRE(view.topics[0].partitions.size() == 12);
    for (const auto& partition : view.topics[0].partitions) CHECK(partition.records.empty());
}

TEST_CASE("An errored partition carries the offsets a client needs to react") {
    FetchResponse response;
    response.topics.push_back({"orders",
                               {{0, ErrorCode::OffsetOutOfRange, Offset(900), Offset(500),
                                 FileRange{}}}});

    Response wire;
    encodeFetchResponse(wire, response);
    const auto   frame = wire.materialise();
    BufferReader in(frame);
    const auto   view = decodeFetchResponse(in);

    // The wire has one OffsetOutOfRange code, Kafka-shaped. These two numbers are
    // how a client tells "you were too slow" from "the log was truncated under
    // you" — the distinction storage keeps and the code collapses.
    const auto& partition = view.topics[0].partitions[0];
    CHECK(partition.error == ErrorCode::OffsetOutOfRange);
    CHECK(partition.logStartOffset == Offset(500));
    CHECK(partition.highWatermark == Offset(900));
    CHECK(partition.records.empty());
}

TEST_CASE("A truncated fetch response is refused at every length") {
    FetchFixture fixture("fetch-truncated", 8);
    const auto   result = fixture.log->read(Offset(0), 200);
    REQUIRE(result.ok());

    FetchResponse response;
    response.topics.push_back(
        {"orders", {{0, ErrorCode::None, Offset(8), Offset(0), result.range}}});

    Response wire;
    encodeFetchResponse(wire, response);
    const auto full = wire.materialise();

    for (size_t length = 0; length < full.size(); ++length) {
        const vector<uint8_t> partial(full.begin(), full.begin() + static_cast<long>(length));
        BufferReader          in(partial);
        CHECK_THROWS_AS(decodeFetchResponse(in), CorruptData);
    }
}

// ===========================================================================
// The consumer-group APIs
// ===========================================================================

TEST_CASE("FindCoordinator round-trips") {
    FindCoordinatorRequest request{"payments-consumer"};
    const auto decoded = roundTrip(
        [&](BufferWriter& out) { encodeFindCoordinatorRequest(out, request); },
        [](BufferReader& in) { return decodeFindCoordinatorRequest(in); });
    CHECK(decoded.groupId == "payments-consumer");

    FindCoordinatorResponse response{ErrorCode::None, 1, "127.0.0.1", 9092};
    const auto decodedResponse = roundTrip(
        [&](BufferWriter& out) { encodeFindCoordinatorResponse(out, response); },
        [](BufferReader& in) { return decodeFindCoordinatorResponse(in); });
    CHECK(decodedResponse.nodeId == 1);
    CHECK(decodedResponse.host == "127.0.0.1");
    CHECK(decodedResponse.port == 9092);
}

TEST_CASE("A coordinator that is not available says so") {
    FindCoordinatorResponse response;
    response.error = ErrorCode::CoordinatorNotAvailable;

    const auto decoded = roundTrip(
        [&](BufferWriter& out) { encodeFindCoordinatorResponse(out, response); },
        [](BufferReader& in) { return decodeFindCoordinatorResponse(in); });

    // __offsets not ready yet. A client retries rather than giving up.
    CHECK(decoded.error == ErrorCode::CoordinatorNotAvailable);
    CHECK(decoded.nodeId == -1);
}

TEST_CASE("JoinGroup round-trips, with and without a member id") {
    JoinGroupRequest first;
    first.groupId      = "g";
    first.subscription = {"orders", "payments"};
    first.protocols    = {"range", "roundrobin"};

    const auto decodedFirst = roundTrip(
        [&](BufferWriter& out) { encodeJoinGroupRequest(out, first); },
        [](BufferReader& in) { return decodeJoinGroupRequest(in); });

    // Empty on a first join — the coordinator issues one, because a client that
    // chose its own could collide with another and become indistinguishable.
    CHECK(decodedFirst.memberId.empty());
    CHECK(decodedFirst.subscription.size() == 2);
    CHECK(decodedFirst.protocols == vector<string>{"range", "roundrobin"});

    JoinGroupRequest rejoin = first;
    rejoin.memberId         = "consumer-a-1";
    const auto decodedRejoin = roundTrip(
        [&](BufferWriter& out) { encodeJoinGroupRequest(out, rejoin); },
        [](BufferReader& in) { return decodeJoinGroupRequest(in); });
    CHECK(decodedRejoin.memberId == "consumer-a-1");
}

TEST_CASE("Only a leader's JoinGroup response carries the member list") {
    JoinGroupResponse leader;
    leader.generation   = 5;
    leader.protocolName = "range";
    leader.leaderId     = "a";
    leader.memberId     = "a";
    leader.members      = {{"a", {"orders"}}, {"b", {"orders"}}};

    const auto decodedLeader = roundTrip(
        [&](BufferWriter& out) { encodeJoinGroupResponse(out, leader); },
        [](BufferReader& in) { return decodeJoinGroupResponse(in); });
    CHECK(decodedLeader.members.size() == 2);
    CHECK(decodedLeader.members[1].memberId == "b");
    CHECK(decodedLeader.members[0].subscription == vector<string>{"orders"});

    JoinGroupResponse follower = leader;
    follower.memberId          = "b";
    follower.members.clear();

    const auto decodedFollower = roundTrip(
        [&](BufferWriter& out) { encodeJoinGroupResponse(out, follower); },
        [](BufferReader& in) { return decodeJoinGroupResponse(in); });
    CHECK(decodedFollower.members.empty());
    CHECK(decodedFollower.leaderId == "a");
    CHECK(decodedFollower.generation == 5);
}

TEST_CASE("SyncGroup carries opaque assignment bytes") {
    SyncGroupRequest request;
    request.groupId     = "g";
    request.generation  = 3;
    request.memberId    = "a";
    request.assignments = {{"a", {0x01, 0x02}}, {"b", {0xFF}}};

    const auto decoded = roundTrip(
        [&](BufferWriter& out) { encodeSyncGroupRequest(out, request); },
        [](BufferReader& in) { return decodeSyncGroupRequest(in); });

    // The broker stores these and hands each member its own. It never parses
    // them, which is what lets a new strategy ship without the broker knowing it
    // exists.
    REQUIRE(decoded.assignments.size() == 2);
    CHECK(decoded.assignments.at("a") == vector<uint8_t>{0x01, 0x02});
    CHECK(decoded.assignments.at("b") == vector<uint8_t>{0xFF});

    SyncGroupRequest follower;
    follower.groupId    = "g";
    follower.generation = 3;
    follower.memberId   = "b";
    const auto decodedFollower = roundTrip(
        [&](BufferWriter& out) { encodeSyncGroupRequest(out, follower); },
        [](BufferReader& in) { return decodeSyncGroupRequest(in); });
    CHECK(decodedFollower.assignments.empty());
}

TEST_CASE("An assignment of arbitrary bytes survives untouched") {
    vector<uint8_t> opaque(256);
    for (size_t i = 0; i < opaque.size(); ++i) opaque[i] = static_cast<uint8_t>(i);

    SyncGroupResponse response;
    response.assignment = opaque;

    const auto decoded = roundTrip(
        [&](BufferWriter& out) { encodeSyncGroupResponse(out, response); },
        [](BufferReader& in) { return decodeSyncGroupResponse(in); });
    CHECK(decoded.assignment == opaque);
}

TEST_CASE("Heartbeat and LeaveGroup round-trip") {
    HeartbeatRequest heartbeat{"g", 7, "a"};
    const auto decodedHeartbeat = roundTrip(
        [&](BufferWriter& out) { encodeHeartbeatRequest(out, heartbeat); },
        [](BufferReader& in) { return decodeHeartbeatRequest(in); });
    CHECK(decodedHeartbeat.generation == 7);

    HeartbeatResponse rebalancing{ErrorCode::RebalanceInProgress};
    const auto decodedResponse = roundTrip(
        [&](BufferWriter& out) { encodeHeartbeatResponse(out, rebalancing); },
        [](BufferReader& in) { return decodeHeartbeatResponse(in); });

    // Not a failure — the signal, on the heartbeat the member was making anyway.
    CHECK(decodedResponse.error == ErrorCode::RebalanceInProgress);

    LeaveGroupRequest leave{"g", "a"};
    const auto decodedLeave = roundTrip(
        [&](BufferWriter& out) { encodeLeaveGroupRequest(out, leave); },
        [](BufferReader& in) { return decodeLeaveGroupRequest(in); });
    CHECK(decodedLeave.memberId == "a");
}

TEST_CASE("OffsetCommit round-trips the next offset to read") {
    OffsetCommitRequest request;
    request.groupId    = "g";
    request.generation = 4;
    request.memberId   = "a";
    request.topics.push_back({"orders", {{0, Offset(510), "deploy-7"}, {1, Offset(0), ""}}});

    const auto decoded = roundTrip(
        [&](BufferWriter& out) { encodeOffsetCommitRequest(out, request); },
        [](BufferReader& in) { return decodeOffsetCommitRequest(in); });

    // 510, not 509. Handle 500 through 509, commit 510 — off by one here
    // reprocesses or skips exactly one message per restart.
    CHECK(decoded.topics[0].partitions[0].nextOffset == Offset(510));
    CHECK(decoded.topics[0].partitions[0].metadata == "deploy-7");
    CHECK(decoded.topics[0].partitions[1].nextOffset == Offset(0));
    CHECK(decoded.generation == 4);
}

TEST_CASE("A negative committed offset is refused") {
    BufferWriter out;
    writeString(out, "g");
    out.writeInt32(1);
    writeString(out, "a");
    out.writeInt32(1);
    writeString(out, "orders");
    out.writeInt32(1);
    out.writeInt32(0);
    out.writeInt64(-1);
    const auto   bytes = out.take();
    BufferReader in(bytes);

    // Not a position a consumer reached. Accepting it would make the next fetch
    // ask for something before the log began, and the group would silently
    // restart from the earliest record it could find.
    CHECK_THROWS_AS(decodeOffsetCommitRequest(in), CorruptData);
}

TEST_CASE("OffsetFetch distinguishes never-committed from committed-at-zero") {
    OffsetFetchResponse response;
    response.topics.push_back({"orders",
                               {{0, Offset(0), "", ErrorCode::None},
                                {1, Offset(-1), "", ErrorCode::None}}});

    const auto decoded = roundTrip(
        [&](BufferWriter& out) { encodeOffsetFetchResponse(out, response); },
        [](BufferReader& in) { return decodeOffsetFetchResponse(in); });

    // Zero means "committed, at the beginning". -1 means "never committed", which
    // a consumer reads as "start wherever your reset policy says". Those differ
    // by an entire topic's worth of records.
    CHECK(decoded.topics[0].partitions[0].nextOffset == Offset(0));
    CHECK(decoded.topics[0].partitions[1].nextOffset == Offset(-1));
}

TEST_CASE("OffsetFetch requests round-trip") {
    OffsetFetchRequest request;
    request.groupId = "g";
    request.topics.push_back({"orders", {0, 1, 2}});
    request.topics.push_back({"payments", {}});

    const auto decoded = roundTrip(
        [&](BufferWriter& out) { encodeOffsetFetchRequest(out, request); },
        [](BufferReader& in) { return decodeOffsetFetchRequest(in); });

    CHECK(decoded.topics[0].partitions == vector<PartitionId>{0, 1, 2});
    CHECK(decoded.topics[1].partitions.empty());
}

TEST_CASE("Every group request is refused at every truncation") {
    JoinGroupRequest join;
    join.groupId      = "g";
    join.memberId     = "a";
    join.subscription = {"orders"};
    join.protocols    = {"range"};

    BufferWriter out;
    encodeJoinGroupRequest(out, join);
    const auto full = out.take();

    for (size_t length = 0; length < full.size(); ++length) {
        const vector<uint8_t> partial(full.begin(), full.begin() + static_cast<long>(length));
        BufferReader          in(partial);
        CHECK_THROWS_AS(decodeJoinGroupRequest(in), CorruptData);
    }
}
