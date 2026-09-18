#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include <initializer_list>
#include <set>
#include <thread>
#include <span>
#include <vector>
#include <string>
#include <type_traits>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include "protocol/error_codes.hpp"
#include "protocol/frame.hpp"
#include "protocol/request_header.hpp"
#include "protocol/response.hpp"
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

// ===========================================================================
// Frames
// ===========================================================================

namespace {

// A pipe stands in for a socket wherever only read() and write() are involved.
// sendfile needs a real socket; that arrives with the segment writer.
class Pipe {
public:
    Pipe() {
        int ends[2];
        REQUIRE(::pipe(ends) == 0);
        readEnd_  = ends[0];
        writeEnd_ = ends[1];
    }
    ~Pipe() {
        if (readEnd_ >= 0) ::close(readEnd_);
        if (writeEnd_ >= 0) ::close(writeEnd_);
    }
    Pipe(const Pipe&)            = delete;
    Pipe& operator=(const Pipe&) = delete;

    int  reader() const { return readEnd_; }
    int  writer() const { return writeEnd_; }
    void closeWriter() {
        if (writeEnd_ >= 0) ::close(writeEnd_);
        writeEnd_ = -1;
    }

private:
    int readEnd_  = -1;
    int writeEnd_ = -1;
};

vector<uint8_t> bytesOf(initializer_list<int> values) {
    vector<uint8_t> out;
    for (const int value : values) out.push_back(static_cast<uint8_t>(value));
    return out;
}

}  // namespace

TEST_CASE("A frame round-trips") {
    Pipe pipe;
    const auto payload = bytesOf({1, 2, 3, 4, 5});

    writeFrame(pipe.writer(), payload);
    const auto received = readFrame(pipe.reader(), kMaxFrameBytes);

    REQUIRE(received.has_value());
    CHECK(*received == payload);
}

TEST_CASE("The length prefix is four big-endian bytes") {
    Pipe pipe;
    writeFrame(pipe.writer(), bytesOf({0xAA, 0xBB}));

    uint8_t prefix[4];
    REQUIRE(readExactly(pipe.reader(), prefix));
    CHECK(prefix[0] == 0x00);
    CHECK(prefix[1] == 0x00);
    CHECK(prefix[2] == 0x00);
    CHECK(prefix[3] == 0x02);
}

TEST_CASE("Frames come back in order, one per read") {
    Pipe pipe;
    for (int i = 1; i <= 5; ++i) writeFrame(pipe.writer(), vector<uint8_t>(i, uint8_t(i)));

    // The prefix is what makes this possible: without it there would be no way
    // to tell where one payload ends and the next begins, because a payload may
    // contain any byte at all.
    for (int i = 1; i <= 5; ++i) {
        const auto frame = readFrame(pipe.reader(), kMaxFrameBytes);
        REQUIRE(frame.has_value());
        CHECK(frame->size() == static_cast<size_t>(i));
        CHECK(frame->front() == static_cast<uint8_t>(i));
    }
}

TEST_CASE("A payload containing the delimiter that does not exist is fine") {
    Pipe pipe;

    // Record bytes are arbitrary — newlines, nulls, anything that would have to
    // be escaped under a delimiter scheme. A length prefix does not care.
    vector<uint8_t> payload;
    for (int value = 0; value < 256; ++value) payload.push_back(static_cast<uint8_t>(value));

    writeFrame(pipe.writer(), payload);
    const auto received = readFrame(pipe.reader(), kMaxFrameBytes);
    REQUIRE(received.has_value());
    CHECK(*received == payload);
}

TEST_CASE("A closed connection between frames is a clean end, not an error") {
    Pipe pipe;
    writeFrame(pipe.writer(), bytesOf({7}));
    pipe.closeWriter();

    CHECK(readFrame(pipe.reader(), kMaxFrameBytes).has_value());

    // How a connection normally ends. The server loop stops; nothing is wrong.
    CHECK_FALSE(readFrame(pipe.reader(), kMaxFrameBytes).has_value());
}

TEST_CASE("A connection closed mid-frame is corruption") {
    Pipe pipe;

    // A prefix promising ten bytes, followed by three and a hangup.
    writeAll(pipe.writer(), bytesOf({0, 0, 0, 10}));
    writeAll(pipe.writer(), bytesOf({1, 2, 3}));
    pipe.closeWriter();

    CHECK_THROWS_AS(readFrame(pipe.reader(), kMaxFrameBytes), CorruptData);
}

TEST_CASE("An oversized frame is refused on the strength of its prefix alone") {
    Pipe pipe;

    // Two gigabytes claimed, nothing sent. The entire point is that this costs
    // four bytes to reject rather than an allocation to discover — otherwise one
    // packet is a denial of service.
    writeAll(pipe.writer(), bytesOf({0x7F, 0xFF, 0xFF, 0xFF}));

    CHECK_THROWS_AS(readFrame(pipe.reader(), kMaxFrameBytes), CorruptData);
}

TEST_CASE("A frame at exactly the limit is allowed and one byte over is not") {
    Pipe pipe;
    constexpr size_t kLimit = 64;

    writeFrame(pipe.writer(), vector<uint8_t>(kLimit, 0xAB));
    const auto atLimit = readFrame(pipe.reader(), kLimit);
    REQUIRE(atLimit.has_value());
    CHECK(atLimit->size() == kLimit);

    writeFrame(pipe.writer(), vector<uint8_t>(kLimit + 1, 0xAB));
    CHECK_THROWS_AS(readFrame(pipe.reader(), kLimit), CorruptData);
}

TEST_CASE("A negative length is refused") {
    Pipe pipe;
    writeAll(pipe.writer(), bytesOf({0xFF, 0xFF, 0xFF, 0xFF}));   // -1
    CHECK_THROWS_AS(readFrame(pipe.reader(), kMaxFrameBytes), CorruptData);
}

TEST_CASE("An empty frame reads back empty rather than as a hangup") {
    Pipe pipe;
    writeFrame(pipe.writer(), {});

    // Legal in shape, meaningless in content — every request carries at least a
    // header. The caller's decoder rejects it; this layer does not guess.
    const auto frame = readFrame(pipe.reader(), kMaxFrameBytes);
    REQUIRE(frame.has_value());
    CHECK(frame->empty());
}

TEST_CASE("Reads and writes larger than a pipe buffer still complete") {
    Pipe pipe;

    // A pipe holds about 64 KB, so this cannot complete in one write() — which
    // is the point. A single read() or write() returns what fits, and code that
    // assumes otherwise is correct only by luck on small payloads.
    vector<uint8_t> payload(512u * 1024);
    for (size_t i = 0; i < payload.size(); ++i) payload[i] = static_cast<uint8_t>(i);

    thread writer([&] { writeFrame(pipe.writer(), payload); });
    const auto received = readFrame(pipe.reader(), kMaxFrameBytes);
    writer.join();

    REQUIRE(received.has_value());
    CHECK(received->size() == payload.size());
    CHECK(*received == payload);
}

TEST_CASE("A request header survives a frame round trip") {
    Pipe pipe;

    RequestHeader header;
    header.apiKey        = ApiKey::ListOffsets;
    header.correlationId = 31337;
    header.clientId      = "cli";

    BufferWriter out;
    encodeRequestHeader(out, header);
    const auto payload = out.take();
    writeFrame(pipe.writer(), payload);

    const auto frame = readFrame(pipe.reader(), kMaxFrameBytes);
    REQUIRE(frame.has_value());

    BufferReader in(*frame);
    const auto   decoded = decodeRequestHeader(in);
    CHECK(decoded.apiKey == ApiKey::ListOffsets);
    CHECK(decoded.correlationId == 31337);
    CHECK(in.empty());
}

// ===========================================================================
// Response segments
// ===========================================================================

namespace {

// A log with real batches in it, so a Response can carry ranges that point at
// bytes something actually wrote.
struct LoadedPartition {
    TempDir                  dir;
    unique_ptr<storage::Log> log;
    LoadedPartition(const string& name, int records, LogConfig config = testConfig())
        : dir(name) {
        log = storage::Log::create(TopicPartition{"orders", 0}, dir.file("orders-0"), config);
        for (int i = 0; i < records; ++i) {
            auto bytes = makeUnstampedBatch(1000 + i, 48);
            log->append(bytes);
        }
    }
};

}  // namespace

TEST_CASE("An empty response has nothing to send") {
    const Response response;
    CHECK(response.empty());
    CHECK(response.totalBytes() == 0);
    CHECK(response.segments().empty());
    CHECK(response.materialise().empty());
}

TEST_CASE("Buffer segments accumulate in order") {
    Response response;
    response.append(bytesOf({1, 2, 3}));
    response.append(bytesOf({4, 5}));

    // Coalesced: adjacent buffers are adjacent bytes on the wire, so keeping
    // them apart would cost a write() each for nothing.
    CHECK(response.segments().size() == 1);
    CHECK(response.totalBytes() == 5);
    CHECK(response.materialise() == bytesOf({1, 2, 3, 4, 5}));

    // A metadata or produce response is exactly this: buffers, no ranges. The
    // abstraction costs them nothing.
    for (const auto& segment : response.segments()) CHECK_FALSE(segment.isFile());
}

TEST_CASE("Empty pieces are dropped rather than stored") {
    Response response;
    response.append(vector<uint8_t>{});
    response.append(FileRange{});
    response.append(FileRange{3, 100, 0});   // a caught-up read

    // Not a tidiness rule. A caught-up fetch produces a zero-length range and is
    // the common case, so callers append unconditionally — and a response full
    // of nothings would be a write loop that sends nothing per iteration.
    CHECK(response.empty());
    CHECK(response.totalBytes() == 0);
}

TEST_CASE("Every stored segment has something in it") {
    Response response;
    response.append(bytesOf({1}));
    response.append(FileRange{});
    response.append(bytesOf({2, 3}));

    for (const auto& segment : response.segments()) CHECK(segment.size() > 0);
    CHECK(response.segments().size() == 1);   // the empty range separated nothing
    CHECK(response.totalBytes() == 3);
}

TEST_CASE("The total is known without reading a single file byte") {
    LoadedPartition partition("resp-total", 10);
    const auto      range = partition.log->read(Offset(0), kBigFetch);
    REQUIRE(range.ok());
    REQUIRE(range.range.length > 0);

    Response response;
    response.append(bytesOf({0, 0, 0, 1}));   // a header
    response.append(range.range);
    response.append(bytesOf({9}));            // a trailer

    // This is what makes the frame length affordable. Log::read resolved a
    // LOCATION, so the length of the payload is known before any of it is sent —
    // which is the only reason a length-prefixed frame and sendfile can coexist.
    CHECK(response.totalBytes() == 4 + range.range.length + 1);
}

TEST_CASE("A file segment carries a location, never the bytes") {
    LoadedPartition partition("resp-location", 6);
    const auto      result = partition.log->read(Offset(2), kBigFetch);
    REQUIRE(result.ok());

    Response response;
    response.append(result.range);

    REQUIRE(response.segments().size() == 1);
    const auto& segment = response.segments().front();
    CHECK(segment.isFile());
    CHECK(segment.buffer.empty());          // nothing was copied
    CHECK(segment.range.fd == result.range.fd);
    CHECK(segment.range.position == result.range.position);
    CHECK(segment.size() == result.range.length);
}

TEST_CASE("Buffers and file ranges interleave in the order appended") {
    LoadedPartition partition("resp-interleave", 12);
    const auto      first  = partition.log->read(Offset(0), 200);
    const auto      second = partition.log->read(Offset(6), 200);
    REQUIRE(first.ok());
    REQUIRE(second.ok());

    // The shape of a multi-partition fetch response: metadata, records,
    // metadata, records.
    Response response;
    response.append(bytesOf({0xAA}));
    response.append(first.range);
    response.append(bytesOf({0xBB}));
    response.append(second.range);

    REQUIRE(response.segments().size() == 4);
    CHECK_FALSE(response.segments()[0].isFile());
    CHECK(response.segments()[1].isFile());
    CHECK_FALSE(response.segments()[2].isFile());
    CHECK(response.segments()[3].isFile());

    const auto flat = response.materialise();
    CHECK(flat.size() == response.totalBytes());
    CHECK(flat.front() == 0xAA);
    CHECK(flat[1 + first.range.length] == 0xBB);
}

TEST_CASE("Materialising a file segment yields the batch that is really there") {
    LoadedPartition partition("resp-materialise", 8);
    const auto      result = partition.log->read(Offset(3), kBigFetch);
    REQUIRE(result.ok());

    Response response;
    response.append(result.range);
    const auto flat = response.materialise();

    // The bytes a consumer would receive — a real batch, checksum intact,
    // containing the offset that was asked for.
    REQUIRE(flat.size() == result.range.length);
    CHECK(storage::RecordBatch::verifyCrc(flat));
    const auto header = storage::RecordBatch::parseHeader(flat);
    CHECK(header.baseOffset <= Offset(3));
    CHECK(header.lastOffset() >= Offset(3));
}

TEST_CASE("A response survives its segment's partition being deleted") {
    TempDir    dir("resp-retention");
    LogConfig  config = testConfig();
    config.roll.maxSegmentBytes           = 400;
    config.retention.retentionMs          = 1;
    config.retention.segmentDeleteDelayMs = 60'000;

    auto log = storage::Log::create(TopicPartition{"orders", 0}, dir.file("orders-0"), config);
    for (int i = 0; i < 30; ++i) {
        auto bytes = makeUnstampedBatch(100'000 + i, 48);
        log->append(bytes);
    }

    // A response built, then not yet written — which is the real sequence: a
    // handler resolves it on one thread and the connection sends it later.
    const auto result = log->read(Offset(0), kBigFetch);
    REQUIRE(result.ok());
    Response response;
    response.append(result.range);

    log->applyRetention(1'000'000);
    REQUIRE(log->logStartOffset() > Offset(0));
    REQUIRE(log->graveyardSize() > 0);

    // M3's deferred deletion, seen from the layer that needed it. Without the
    // graveyard the descriptor would be closed by now — or its number reused by
    // another file, and a consumer would receive someone else's bytes.
    const auto flat = response.materialise();
    CHECK(flat.size() == response.totalBytes());
    CHECK(storage::RecordBatch::verifyCrc(flat));
}

// ===========================================================================
// Writing a response to a socket
// ===========================================================================

namespace {

// A connected pair of loopback TCP sockets.
//
// Not a pipe and not socketpair(): BSD sendfile needs a real stream socket as
// its destination, and will not accept a pipe. Bound to port 0 so the OS picks a
// free one — a fixed port would make the suite fail on a machine already running
// something, and unsafe to run twice at once.
class TcpPair {
public:
    TcpPair() {
        const int listener = ::socket(AF_INET, SOCK_STREAM, 0);
        REQUIRE(listener >= 0);

        sockaddr_in address{};
        address.sin_family      = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port        = 0;
        REQUIRE(::bind(listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0);
        REQUIRE(::listen(listener, 1) == 0);

        socklen_t length = sizeof(address);
        REQUIRE(::getsockname(listener, reinterpret_cast<sockaddr*>(&address), &length) == 0);

        client_ = ::socket(AF_INET, SOCK_STREAM, 0);
        REQUIRE(client_ >= 0);
        REQUIRE(::connect(client_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0);

        server_ = ::accept(listener, nullptr, nullptr);
        REQUIRE(server_ >= 0);
        ::close(listener);
    }
    ~TcpPair() {
        if (server_ >= 0) ::close(server_);
        if (client_ >= 0) ::close(client_);
    }
    TcpPair(const TcpPair&)            = delete;
    TcpPair& operator=(const TcpPair&) = delete;

    int server() const { return server_; }   // the broker's end
    int client() const { return client_; }   // the consumer's end

private:
    int server_ = -1;
    int client_ = -1;
};

}  // namespace

TEST_CASE("A buffer-only response arrives as one frame") {
    TcpPair  sockets;
    Response response;
    response.append(bytesOf({1, 2, 3}));
    response.append(bytesOf({4, 5}));

    writeResponse(sockets.server(), response);

    const auto frame = readFrame(sockets.client(), kMaxFrameBytes);
    REQUIRE(frame.has_value());
    CHECK(*frame == bytesOf({1, 2, 3, 4, 5}));
    CHECK(frame->size() == response.totalBytes());
}

TEST_CASE("A file range reaches the socket without passing through this process") {
    LoadedPartition partition("write-sendfile", 10);
    const auto      result = partition.log->read(Offset(0), kBigFetch);
    REQUIRE(result.ok());
    REQUIRE(result.range.length > 0);

    TcpPair  sockets;
    Response response;
    response.append(result.range);

    // The whole point, exercised: these bytes go from the page cache to the
    // socket inside the kernel. The only copy in this test is the one the
    // consumer makes on the other end.
    writeResponse(sockets.server(), response);

    const auto frame = readFrame(sockets.client(), kMaxFrameBytes);
    REQUIRE(frame.has_value());
    CHECK(frame->size() == result.range.length);
    CHECK(*frame == response.materialise());
    CHECK(storage::RecordBatch::verifyCrc(*frame));
}

TEST_CASE("Buffers and file ranges arrive interleaved in order") {
    LoadedPartition partition("write-interleave", 12);
    const auto      first  = partition.log->read(Offset(0), 200);
    const auto      second = partition.log->read(Offset(6), 200);
    REQUIRE(first.ok());
    REQUIRE(second.ok());

    TcpPair  sockets;
    Response response;
    response.append(bytesOf({0xAA}));
    response.append(first.range);
    response.append(bytesOf({0xBB}));
    response.append(second.range);

    // The shape of a real multi-partition fetch: metadata, records, metadata,
    // records — alternating write() and sendfile() on one socket, arriving as
    // one continuous byte stream that the consumer cannot tell apart.
    writeResponse(sockets.server(), response);

    const auto frame = readFrame(sockets.client(), kMaxFrameBytes);
    REQUIRE(frame.has_value());
    CHECK(*frame == response.materialise());
    CHECK(frame->front() == 0xAA);
    CHECK((*frame)[1 + first.range.length] == 0xBB);
}

TEST_CASE("A response larger than the socket buffer still completes") {
    // Enough records that the payload exceeds any socket buffer, so neither the
    // write nor the sendfile can finish in one call. Code that ignores a short
    // send is correct only by luck on small responses and silently truncates
    // large ones — which is exactly the case a consumer with a big fetch hits.
    // One big segment, because a FileRange never spans two files — the default
    // test config rolls at 64 KB, so a read there is capped by the segment rather
    // than by the fetch size, and the payload would never be large enough to
    // exercise a short send.
    LogConfig roomy = testConfig();
    roomy.roll.maxSegmentBytes    = 32u << 20;
    roomy.roll.indexIntervalBytes = 4096;
    roomy.roll.maxIndexBytes      = 1u << 20;

    LoadedPartition partition("write-large", 4000, roomy);
    const auto      result = partition.log->read(Offset(0), 16u << 20);
    REQUIRE(result.ok());
    REQUIRE(result.range.length > 256u * 1024);

    TcpPair  sockets;
    Response response;
    response.append(bytesOf({0xEE}));
    response.append(result.range);

    // A reader is needed concurrently: with nobody draining it, the socket
    // buffer fills and the write blocks forever.
    optional<vector<uint8_t>> received;
    thread reader([&] { received = readFrame(sockets.client(), kMaxFrameBytes); });

    writeResponse(sockets.server(), response);
    reader.join();

    REQUIRE(received.has_value());
    CHECK(received->size() == response.totalBytes());
    CHECK(*received == response.materialise());
}

TEST_CASE("An empty response is still a well-formed frame") {
    TcpPair        sockets;
    const Response response;

    // A client is owed an answer even when there is nothing in it, and a
    // zero-length frame is how that is said.
    writeResponse(sockets.server(), response);

    const auto frame = readFrame(sockets.client(), kMaxFrameBytes);
    REQUIRE(frame.has_value());
    CHECK(frame->empty());
}

TEST_CASE("A caught-up fetch sends its metadata and no records") {
    LoadedPartition partition("write-caught-up", 4);
    const auto      result = partition.log->read(partition.log->logEndOffset(), kBigFetch);
    REQUIRE(result.ok());
    REQUIRE(result.range.empty());

    TcpPair  sockets;
    Response response;
    response.append(bytesOf({0, 0, 0, 0}));   // stand-in for partition metadata
    response.append(result.range);            // dropped, being empty

    CHECK(response.segments().size() == 1);
    writeResponse(sockets.server(), response);

    // The most common fetch in the system: an answer, four bytes long, with no
    // sendfile at all.
    const auto frame = readFrame(sockets.client(), kMaxFrameBytes);
    REQUIRE(frame.has_value());
    CHECK(frame->size() == 4);
}
