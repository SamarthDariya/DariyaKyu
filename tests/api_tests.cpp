#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include <string>
#include <vector>

#include "protocol/list_offsets.hpp"
#include "protocol/metadata.hpp"
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
