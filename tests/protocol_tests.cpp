#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include <set>
#include <span>
#include <vector>
#include <string>
#include <type_traits>

#include "protocol/error_codes.hpp"
#include "protocol/request_header.hpp"
#include "test_support.hpp"

using namespace std;
using namespace dariyakyu;
using namespace dariyakyu::protocol;
using namespace dariyakyu::test;

// ===========================================================================
// Error codes
// ===========================================================================

TEST_CASE("Success is zero") {
    // Deliberate and worth pinning: it means a zero-filled buffer reads as
    // "fine", which is the wrong default — so every encoder writes this field
    // explicitly rather than trusting an initialiser.
    CHECK(static_cast<int16_t>(ErrorCode::None) == 0);
    CHECK(static_cast<int16_t>(ErrorCode::Unknown) != 0);
}

TEST_CASE("Every code has a distinct number") {
    // The numbers are the contract. Two enumerators sharing one would make a
    // client's branch silently wrong, and nothing else would notice.
    set<int16_t> seen;
    for (const ErrorCode code : kAllErrorCodes) seen.insert(static_cast<int16_t>(code));
    CHECK(seen.size() == kAllErrorCodes.size());
}

TEST_CASE("Every code has a description") {
    // kAllErrorCodes exists so this cannot silently skip a newly added code.
    for (const ErrorCode code : kAllErrorCodes) {
        const string text = describe(code);
        CHECK_FALSE(text.empty());
        CHECK(text != "unrecognised error code");
    }
}

TEST_CASE("A code from a newer broker still describes itself") {
    // A client of a newer broker can genuinely receive a code this build has
    // never heard of. Better a placeholder than a crash or an empty string.
    const auto fromTheFuture = static_cast<ErrorCode>(9999);
    CHECK(string(describe(fromTheFuture)) == "unrecognised error code");
}

TEST_CASE("A read's outcome translates to a wire code") {
    using storage::ReadError;

    CHECK(errorCodeFor(ReadError::None) == ErrorCode::None);
    CHECK(errorCodeFor(ReadError::BelowLogStart) == ErrorCode::OffsetOutOfRange);
    CHECK(errorCodeFor(ReadError::AboveLogEnd) == ErrorCode::OffsetOutOfRange);
}

TEST_CASE("The two out-of-range cases collapse into one code") {
    using storage::ReadError;

    // Storage keeps them apart because a client's reset policy differs — "you
    // were too slow" versus "the log was truncated under you". The wire has one
    // code, Kafka-shaped, and a client tells them apart by comparing against the
    // log start and end offsets the response also carries.
    CHECK(errorCodeFor(ReadError::BelowLogStart) == errorCodeFor(ReadError::AboveLogEnd));

    // Which is exactly why the storage-level distinction must not be lost.
    CHECK(ReadError::BelowLogStart != ReadError::AboveLogEnd);
}

TEST_CASE("A real read's outcome maps end to end") {
    TempDir dir("proto-error-mapping");
    auto    log = storage::Log::create(TopicPartition{"orders", 0}, dir.file("orders-0"),
                                       testConfig());
    auto bytes = makeUnstampedBatch(1000, 48);
    log->append(bytes);

    // Not a hand-built enum value — the outcomes a Log actually produces.
    CHECK(errorCodeFor(log->read(Offset(0), kBigFetch).error) == ErrorCode::None);
    CHECK(errorCodeFor(log->read(Offset(1), kBigFetch).error) == ErrorCode::None);  // caught up
    CHECK(errorCodeFor(log->read(Offset(2), kBigFetch).error) == ErrorCode::OffsetOutOfRange);
}

// ===========================================================================
// Api keys
// ===========================================================================

TEST_CASE("Api keys use Kafka's own numbers") {
    // Not ours in sequence. It costs nothing and it is what makes "a shim rather
    // than a redesign" credible — a translation layer would otherwise have to
    // remap every key as well as every field.
    CHECK(static_cast<int16_t>(ApiKey::Produce) == 0);
    CHECK(static_cast<int16_t>(ApiKey::Fetch) == 1);
    CHECK(static_cast<int16_t>(ApiKey::ListOffsets) == 2);
    CHECK(static_cast<int16_t>(ApiKey::Metadata) == 3);
    CHECK(static_cast<int16_t>(ApiKey::CreateTopic) == 19);
}

TEST_CASE("Every api key is known and described") {
    for (const ApiKey key : kAllApiKeys) {
        CHECK(isKnown(key));
        CHECK_FALSE(string(describe(key)).empty());
        CHECK(string(describe(key)) != "unrecognised api key");
    }
}

TEST_CASE("A key from a newer broker is neither known nor nameless") {
    // Kafka's gaps are left as gaps rather than reused, so 5 stays unassigned.
    const auto unassigned = static_cast<ApiKey>(5);
    CHECK_FALSE(isKnown(unassigned));
    CHECK(string(describe(unassigned)) == "unrecognised api key");
    CHECK_FALSE(isKnown(static_cast<ApiKey>(9999)));
}

// ===========================================================================
// Request header
// ===========================================================================

namespace {

vector<uint8_t> encodedHeader(const RequestHeader& header) {
    BufferWriter out;
    encodeRequestHeader(out, header);
    return out.take();
}

RequestHeader sampleHeader() {
    RequestHeader header;
    header.apiKey        = ApiKey::Fetch;
    header.apiVersion    = 0;
    header.correlationId = 4242;
    header.clientId      = "dariyakyu-cli";
    return header;
}

}  // namespace

TEST_CASE("A request header round-trips") {
    const auto original = sampleHeader();
    const auto bytes    = encodedHeader(original);

    BufferReader in(bytes);
    const auto   decoded = decodeRequestHeader(in);

    CHECK(decoded.apiKey == ApiKey::Fetch);
    CHECK(decoded.apiVersion == 0);
    CHECK(decoded.correlationId == 4242);
    CHECK(decoded.clientId == "dariyakyu-cli");

    // Fully consumed, so nothing was left unread or over-read.
    CHECK(in.empty());
}

TEST_CASE("The header has the documented byte layout") {
    RequestHeader header;
    header.apiKey        = ApiKey::CreateTopic;   // 19
    header.apiVersion    = 2;
    header.correlationId = 0x01020304;
    header.clientId      = "ab";
    const auto bytes = encodedHeader(header);

    REQUIRE(bytes.size() == 2 + 2 + 4 + 2 + 2);
    CHECK(bytes[0] == 0x00);                  // apiKey, big-endian
    CHECK(bytes[1] == 19);
    CHECK(bytes[2] == 0x00);                  // apiVersion
    CHECK(bytes[3] == 2);
    CHECK(bytes[4] == 0x01);                  // correlationId
    CHECK(bytes[5] == 0x02);
    CHECK(bytes[6] == 0x03);
    CHECK(bytes[7] == 0x04);
    CHECK(bytes[8] == 0x00);                  // clientId length
    CHECK(bytes[9] == 2);
    CHECK(bytes[10] == 'a');
    CHECK(bytes[11] == 'b');
}

TEST_CASE("Decoding leaves the reader on the body") {
    // A handler reads its own fields from the same reader, so the position after
    // the header has to be exactly where the body starts.
    BufferWriter out;
    encodeRequestHeader(out, sampleHeader());
    out.writeInt32(0xBEEF);   // stand-in for a body field
    const auto bytes = out.take();

    BufferReader in(bytes);
    const auto   header = decodeRequestHeader(in);
    CHECK(header.correlationId == 4242);
    CHECK(in.readInt32() == static_cast<int32_t>(0xBEEF));
    CHECK(in.empty());
}

TEST_CASE("An unknown api key decodes rather than throwing") {
    // The requirement that shapes this function. A broker that cannot serve a
    // request still has to answer, and every response is matched to its request
    // by correlationId — which lives in the header. Throwing here would mean
    // knowing the request was unsupported and having no way to say so, because
    // the id needed to reply was in the part that was refused.
    RequestHeader header = sampleHeader();
    auto          bytes  = encodedHeader(header);
    bytes[0] = 0x7F;
    bytes[1] = 0x7F;   // an api key from far in the future

    BufferReader in(bytes);
    RequestHeader decoded;
    CHECK_NOTHROW(decoded = decodeRequestHeader(in));
    CHECK_FALSE(isKnown(decoded.apiKey));
    CHECK(decoded.correlationId == 4242);   // still answerable
}

TEST_CASE("An unknown api version decodes rather than throwing") {
    RequestHeader header = sampleHeader();
    header.apiVersion    = 99;

    const auto   bytes   = encodedHeader(header);
    BufferReader in(bytes);
    const auto   decoded = decodeRequestHeader(in);
    CHECK(decoded.apiVersion == 99);
    CHECK(decoded.correlationId == 4242);
}

TEST_CASE("A null client id decodes as empty") {
    RequestHeader header = sampleHeader();
    header.clientId      = "";
    auto bytes           = encodedHeader(header);

    // An empty clientId encodes as length 0; a client may also send -1 for null.
    // Unlike a record key, where null and empty are different facts and
    // collapsing them would delete data, a clientId means nothing to the broker
    // either way — so both arrive as an empty string.
    REQUIRE(bytes[8] == 0x00);
    REQUIRE(bytes[9] == 0x00);
    BufferReader emptyId(bytes);
    CHECK(decodeRequestHeader(emptyId).clientId.empty());

    bytes[8] = 0xFF;
    bytes[9] = 0xFF;   // -1, null
    BufferReader nullId(bytes);
    CHECK(decodeRequestHeader(nullId).clientId.empty());
    CHECK(nullId.empty());
}

TEST_CASE("A client id length that no encoder could produce is refused") {
    auto bytes = encodedHeader(sampleHeader());
    bytes[8]   = 0xFF;
    bytes[9]   = 0xFE;   // -2: negative, but not the null marker

    BufferReader in(bytes);
    CHECK_THROWS_AS(decodeRequestHeader(in), CorruptData);
}

TEST_CASE("A client id length that runs past the frame is refused") {
    auto bytes = encodedHeader(sampleHeader());
    bytes[8]   = 0x7F;
    bytes[9]   = 0xFF;   // claims 32767 bytes of name

    BufferReader in(bytes);
    CHECK_THROWS_AS(decodeRequestHeader(in), CorruptData);
}

TEST_CASE("A truncated header is refused at every length") {
    const auto full = encodedHeader(sampleHeader());
    for (size_t length = 0; length < full.size(); ++length) {
        const vector<uint8_t> partial(full.begin(), full.begin() + static_cast<long>(length));
        BufferReader          in(partial);
        CHECK_THROWS_AS(decodeRequestHeader(in), CorruptData);
    }
}

TEST_CASE("A response header carries only the correlation id") {
    BufferWriter out;
    encodeResponseHeader(out, 777);
    const auto bytes = out.take();

    // Nothing else a client needs to route it, and anything more would be a
    // field every API paid for.
    CHECK(bytes.size() == 4);
    BufferReader in(bytes);
    CHECK(decodeResponseHeader(in) == 777);
    CHECK(in.empty());
}

TEST_CASE("Constructing a reader from a temporary does not compile") {
    // The guard added while writing this suite. A BufferReader borrows, so
    // BufferReader in(encodedHeader(header)) leaves it pointing at a vector that
    // died at the end of the expression — and the span conversion made it silent.
    //
    // Asserted at compile time, because the whole point is that the bad form is
    // not expressible.
    static_assert(!is_constructible_v<BufferReader, vector<uint8_t>&&>,
                  "a reader must not be constructible from a temporary buffer");
    static_assert(is_constructible_v<BufferReader, span<const uint8_t>>,
                  "but a borrowed span is exactly what it is for");

    // And an lvalue vector still works, since that is the normal case.
    const vector<uint8_t> owned{1, 2, 3, 4};
    BufferReader          in(owned);
    CHECK(in.remaining() == 4);
}
