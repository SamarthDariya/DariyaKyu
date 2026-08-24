#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest/doctest.h>

#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "common/buffer.hpp"
#include "common/crc32c.hpp"
#include "common/errors.hpp"
#include "common/varint.hpp"
#include "storage/record_batch.hpp"

using namespace std;
using namespace dariyakyu;
using namespace dariyakyu::storage;

namespace {

span<const uint8_t> bytesOf(const string& s) {
    return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
}

string stringOf(span<const uint8_t> bytes) {
    return string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
}

// Round-trips a value and asserts the encoded width, since the whole point of
// varints is that small numbers are small.
void checkVarint(int32_t value, size_t expectedBytes) {
    uint8_t      buffer[kMaxVarintBytes];
    const size_t written = encodeVarint(value, buffer);
    CHECK(written == expectedBytes);
    CHECK(varintSize(value) == expectedBytes);

    int32_t decoded = 0;
    CHECK(decodeVarint(span<const uint8_t>(buffer, written), decoded) == written);
    CHECK(decoded == value);
}

void checkVarlong(int64_t value, size_t expectedBytes) {
    uint8_t      buffer[kMaxVarlongBytes];
    const size_t written = encodeVarlong(value, buffer);
    CHECK(written == expectedBytes);
    CHECK(varlongSize(value) == expectedBytes);

    int64_t decoded = 0;
    CHECK(decodeVarlong(span<const uint8_t>(buffer, written), decoded) == written);
    CHECK(decoded == value);
}

}  // namespace

// ===========================================================================
// varint
// ===========================================================================

TEST_CASE("Zigzag keeps small magnitudes small regardless of sign") {
    // Without zigzag, -1 is all ones in two's complement and would encode in the
    // maximum width. A null length is -1, and it must not cost five bytes.
    checkVarint(0, 1);
    checkVarint(-1, 1);
    checkVarint(1, 1);
    checkVarint(63, 1);
    checkVarint(-64, 1);
    checkVarint(64, 2);
    checkVarint(-65, 2);
    checkVarint(8191, 2);
    checkVarint(8192, 3);
    checkVarint(INT32_MAX, 5);
    checkVarint(INT32_MIN, 5);
}

TEST_CASE("Varlong covers the full 64-bit range") {
    checkVarlong(0, 1);
    checkVarlong(-1, 1);
    checkVarlong(1, 1);
    checkVarlong(INT64_MAX, 10);
    checkVarlong(INT64_MIN, 10);
    checkVarlong(1'700'000'000'000, 6);   // a plausible millisecond timestamp
}

TEST_CASE("Varint matches the canonical base-128 encoding") {
    // 300 zigzags to 600, which is 0b100_1011000 -> 0xD8 0x04.
    uint8_t      buffer[kMaxVarintBytes];
    const size_t written = encodeVarint(300, buffer);
    REQUIRE(written == 2);
    CHECK(buffer[0] == 0xD8);
    CHECK(buffer[1] == 0x04);
}

TEST_CASE("A varint that runs off the end of the buffer is corruption") {
    // 0x80 says "another byte follows" and there is none.
    const uint8_t truncated[] = {0x80};
    int32_t       value       = 0;
    CHECK_THROWS_AS(decodeVarint(truncated, value), CorruptData);
}

TEST_CASE("A varint too long for its type is rejected, not truncated") {
    // Six continuation bytes cannot describe an int32. Silently taking the low
    // bits would resynchronise the parser onto garbage.
    const uint8_t overlong[] = {0x80, 0x80, 0x80, 0x80, 0x80, 0x01};
    int32_t       value      = 0;
    CHECK_THROWS_AS(decodeVarint(overlong, value), CorruptData);
}

TEST_CASE("A final varint byte may not set bits the target type lacks") {
    // The fifth byte of an int32 varint contributes bits 28-34, of which only
    // 28-31 exist. Accepting the rest yields a wrong-but-plausible number from
    // input that was never a valid varint — and the recovery scan, walking a
    // partially written segment, is exactly where believing that is expensive.
    int32_t value = 0;

    const uint8_t overflowing[] = {0x80, 0x80, 0x80, 0x80, 0x7F};
    CHECK_THROWS_AS(decodeVarint(overflowing, value), CorruptData);

    // Four bits in the final byte is the boundary, and must still decode.
    const uint8_t atTheLimit[] = {0x80, 0x80, 0x80, 0x80, 0x0F};
    CHECK(decodeVarint(atTheLimit, value) == 5);

    // The same rule for varlong, where the tenth byte may carry a single bit.
    int64_t       longValue = 0;
    const uint8_t longOverflowing[] = {0x80, 0x80, 0x80, 0x80, 0x80,
                                       0x80, 0x80, 0x80, 0x80, 0x02};
    CHECK_THROWS_AS(decodeVarlong(longOverflowing, longValue), CorruptData);

    const uint8_t longAtTheLimit[] = {0x80, 0x80, 0x80, 0x80, 0x80,
                                      0x80, 0x80, 0x80, 0x80, 0x01};
    CHECK(decodeVarlong(longAtTheLimit, longValue) == 10);

    // And the extremes, which sit right at those boundaries, still round-trip.
    checkVarint(INT32_MAX, 5);
    checkVarint(INT32_MIN, 5);
    checkVarlong(INT64_MAX, 10);
    checkVarlong(INT64_MIN, 10);
}

// ===========================================================================
// CRC32C
// ===========================================================================

TEST_CASE("CRC32C matches the canonical iSCSI vectors") {
    // These are the RFC 3720 test vectors. Getting a different answer here means
    // we implemented zlib's CRC32 (a different polynomial) by mistake.
    const string check = "123456789";
    CHECK(crc32c(bytesOf(check)) == 0xE3069283u);

    const vector<uint8_t> zeros(32, 0x00);
    CHECK(crc32c(zeros) == 0x8A9136AAu);

    const vector<uint8_t> ones(32, 0xFF);
    CHECK(crc32c(ones) == 0x62A8AB43u);
}

TEST_CASE("Incremental CRC32C equals the one-shot form") {
    const vector<uint8_t> first{1, 2, 3, 4, 5};
    const vector<uint8_t> second{6, 7, 8};
    const vector<uint8_t> whole{1, 2, 3, 4, 5, 6, 7, 8};

    uint32_t crc = crc32cInit();
    crc          = crc32cUpdate(crc, first);
    crc          = crc32cUpdate(crc, second);
    CHECK(crc32cFinish(crc) == crc32c(whole));
}

TEST_CASE("CRC32C detects a single flipped bit") {
    vector<uint8_t> data(128, 0x5A);
    const uint32_t  before = crc32c(data);
    data[64] ^= 0x01;
    CHECK(crc32c(data) != before);
}

// ===========================================================================
// Buffer
// ===========================================================================

TEST_CASE("Fixed-width integers round-trip big-endian") {
    BufferWriter out;
    out.writeInt8(-1);
    out.writeInt16(-2);
    out.writeInt32(-3);
    out.writeInt64(-4);
    out.writeUint32(0xDEADBEEFu);

    BufferReader in(out.view());
    CHECK(in.readInt8() == -1);
    CHECK(in.readInt16() == -2);
    CHECK(in.readInt32() == -3);
    CHECK(in.readInt64() == -4);
    CHECK(in.readUint32() == 0xDEADBEEFu);
    CHECK(in.empty());
}

TEST_CASE("Integers are written most significant byte first") {
    // Network byte order, independent of the host — a file written on one
    // machine must be readable on another.
    BufferWriter out;
    out.writeInt32(0x01020304);

    const auto bytes = out.view();
    REQUIRE(bytes.size() == 4);
    CHECK(bytes[0] == 0x01);
    CHECK(bytes[1] == 0x02);
    CHECK(bytes[2] == 0x03);
    CHECK(bytes[3] == 0x04);
}

TEST_CASE("readBytes borrows rather than copying") {
    const vector<uint8_t> source{10, 20, 30, 40, 50};
    BufferReader          in(source);
    in.skip(1);

    const auto view = in.readBytes(3);
    // The span must point INTO the source buffer: a 1 MB batch has to cost
    // nothing to describe.
    CHECK(view.data() == source.data() + 1);
    CHECK(view.size() == 3);
    CHECK(in.remaining() == 1);
}

TEST_CASE("Reading past the end of a buffer is corruption") {
    const vector<uint8_t> source{1, 2, 3};
    BufferReader          in(source);
    CHECK_THROWS_AS(in.readInt64(), CorruptData);

    BufferReader other(source);
    CHECK_THROWS_AS(other.readBytes(4), CorruptData);
}

TEST_CASE("Patching overwrites a field written earlier") {
    // A batch cannot know its own length or checksum until its body exists.
    BufferWriter out;
    const size_t lengthAt = out.position();
    out.writeInt32(0);
    out.writeBytes(bytesOf("payload"));

    out.patchInt32(lengthAt, static_cast<int32_t>(out.position() - 4));

    BufferReader in(out.view());
    CHECK(in.readInt32() == 7);
    CHECK(stringOf(in.readBytes(7)) == "payload");
}

TEST_CASE("A patch past the end of the buffer is a programming error") {
    BufferWriter out;
    out.writeInt32(0);
    CHECK_THROWS_AS(out.patchInt32(2, 1), Error);
}

TEST_CASE("readArrayLength accepts a count or the null marker, nothing below") {
    BufferWriter out;
    out.writeInt32(3);
    out.writeInt32(0);
    out.writeInt32(-1);   // null array, which Kafka encodes as -1
    out.writeInt32(-2);   // damaged

    BufferReader in(out.view());
    CHECK(in.readArrayLength() == 3);
    CHECK(in.readArrayLength() == 0);
    CHECK(in.readArrayLength() == -1);

    // Letting this through would drive a loop counter or a reserve() with a
    // nonsense value.
    CHECK_THROWS_AS(in.readArrayLength(), CorruptData);
}

TEST_CASE("A writer refuses to be used after take()") {
    // take() hands the allocation to the caller, so any span previously obtained
    // from view() now belongs to the returned vector. Continuing to use the
    // writer would mean two owners reasoning about one buffer — the same class
    // of mistake as decoding a temporary.
    BufferWriter out;
    out.writeInt32(7);

    const vector<uint8_t> bytes = out.take();
    CHECK(bytes.size() == 4);
    CHECK(out.isSpent());

    CHECK_THROWS_AS(out.writeInt32(1), Error);
    CHECK_THROWS_AS(out.writeBytes(bytesOf("more")), Error);
    CHECK_THROWS_AS(out.patchInt32(0, 1), Error);
    CHECK_THROWS_AS(out.view(), Error);
    CHECK_THROWS_AS(out.take(), Error);
}

// ===========================================================================
// RecordBatch
// ===========================================================================

TEST_CASE("The header is exactly 61 bytes with the documented field positions") {
    // Three separate pieces of code index into these positions; if they drift,
    // the broker stamps offsets into the middle of a checksum.
    CHECK(kBatchHeaderSize == 61);
    CHECK(field::kBaseOffset == 0);
    CHECK(field::kBatchLength == 8);
    CHECK(field::kPartitionLeaderEpoch == 12);
    CHECK(field::kMagic == 16);
    CHECK(field::kCrc == 17);
    CHECK(field::kAttributes == 21);
    CHECK(kCrcCoverageStart == 21);
}

TEST_CASE("A built batch round-trips through decode") {
    const string keyA = "account-42", valueA = R"({"amt":100})";
    const string valueB = "no key on this one";

    RecordBatchBuilder builder;
    builder.append(1'700'000'000'000, bytesOf(keyA), bytesOf(valueA));
    builder.append(1'700'000'000'005, nullopt, bytesOf(valueB));

    const vector<uint8_t> encoded = builder.build();
    const BatchHeader     header  = RecordBatch::parseHeader(encoded);

    CHECK(header.magic == kMagicV2);
    CHECK(header.recordCount == 2);
    CHECK(header.lastOffsetDelta == 1);
    CHECK(header.firstTimestamp == 1'700'000'000'000);
    CHECK(header.maxTimestamp == 1'700'000'000'005);
    CHECK(header.compression() == Compression::None);
    CHECK(header.totalSize() == encoded.size());

    const vector<Record> records = RecordBatch::decodeRecords(encoded);
    REQUIRE(records.size() == 2);

    CHECK(records[0].offsetDelta == 0);
    CHECK(records[0].timestampDelta == 0);
    REQUIRE(records[0].key.has_value());
    CHECK(stringOf(*records[0].key) == keyA);
    REQUIRE(records[0].value.has_value());
    CHECK(stringOf(*records[0].value) == valueA);

    CHECK(records[1].offsetDelta == 1);
    CHECK(records[1].timestampDelta == 5);
    CHECK_FALSE(records[1].key.has_value());
    CHECK(stringOf(*records[1].value) == valueB);
}

TEST_CASE("Null and empty are different things") {
    // Compaction treats a null value as a tombstone, so collapsing null into
    // empty would delete data rather than store it.
    const string empty;

    RecordBatchBuilder builder;
    builder.append(0, bytesOf(empty), nullopt);   // empty key, null value
    builder.append(0, nullopt, bytesOf(empty));   // null key, empty value

    const vector<uint8_t> encoded = builder.build();
    const vector<Record>  records = RecordBatch::decodeRecords(encoded);
    REQUIRE(records.size() == 2);

    REQUIRE(records[0].key.has_value());
    CHECK(records[0].key->empty());
    CHECK_FALSE(records[0].value.has_value());

    CHECK_FALSE(records[1].key.has_value());
    REQUIRE(records[1].value.has_value());
    CHECK(records[1].value->empty());
}

TEST_CASE("Stamping the base offset and leader epoch leaves the checksum valid") {
    // This is the property the whole field ordering exists to provide: the
    // broker assigns offsets to an arriving batch without recomputing a CRC over
    // a body it may not even be able to read (DESIGN.md decision 13).
    const string key = "k", value = "v";

    RecordBatchBuilder builder;
    builder.append(1'700'000'000'000, bytesOf(key), bytesOf(value));
    vector<uint8_t> encoded = builder.build();

    REQUIRE(RecordBatch::verifyCrc(encoded));
    const uint32_t crcBefore = RecordBatch::parseHeader(encoded).crc;

    RecordBatch::stampBaseOffset(encoded, Offset{4'500'000});
    RecordBatch::stampLeaderEpoch(encoded, 7);

    const BatchHeader header = RecordBatch::parseHeader(encoded);
    CHECK(header.baseOffset == Offset{4'500'000});
    CHECK(header.partitionLeaderEpoch == 7);

    // Same checksum, still valid — nothing under it moved.
    CHECK(header.crc == crcBefore);
    CHECK(RecordBatch::verifyCrc(encoded));

    // And the record's absolute offset now derives from the stamped base.
    const vector<Record> records = RecordBatch::decodeRecords(encoded);
    REQUIRE(records.size() == 1);
    CHECK(records[0].offsetFrom(header.baseOffset) == Offset{4'500'000});
    CHECK(header.lastOffset() == Offset{4'500'000});
}

TEST_CASE("The record count is readable without decoding the body") {
    // The broker advances its log end offset knowing only how many records
    // arrived — never what they are.
    RecordBatchBuilder builder;
    for (int i = 0; i < 5; ++i) builder.append(1'700'000'000'000 + i, nullopt, nullopt);

    const vector<uint8_t> encoded = builder.build();

    // Hand parseHeader only the header. If it needed the body this would throw.
    const span<const uint8_t> headerOnly(encoded.data(), kBatchHeaderSize);
    const BatchHeader         header = RecordBatch::parseHeader(headerOnly);

    CHECK(header.recordCount == 5);
    CHECK(header.lastOffsetDelta == 4);
}

TEST_CASE("totalSizeOf walks from one batch to the next") {
    // How a recovery scan steps over batches it has no reason to decode.
    const string firstValue = "first";

    RecordBatchBuilder first;
    first.append(1, nullopt, bytesOf(firstValue));
    RecordBatchBuilder second;
    second.append(2, nullopt, nullopt);

    const vector<uint8_t> a = first.build();
    const vector<uint8_t> b = second.build();

    vector<uint8_t> log;
    log.insert(log.end(), a.begin(), a.end());
    log.insert(log.end(), b.begin(), b.end());

    const size_t firstSize = RecordBatch::totalSizeOf(log);
    CHECK(firstSize == a.size());

    const span<const uint8_t> rest = span<const uint8_t>(log).subspan(firstSize);
    CHECK(RecordBatch::totalSizeOf(rest) == b.size());
    CHECK(RecordBatch::parseHeader(rest).recordCount == 1);
}

TEST_CASE("A flipped byte in the body fails the checksum") {
    const string value = "the quick brown fox";

    RecordBatchBuilder builder;
    builder.append(1'700'000'000'000, nullopt, bytesOf(value));
    vector<uint8_t> encoded = builder.build();

    REQUIRE(RecordBatch::verifyCrc(encoded));
    encoded[kBatchHeaderSize + 4] ^= 0x01;
    CHECK_FALSE(RecordBatch::verifyCrc(encoded));
}

TEST_CASE("An unrecognised magic byte is corruption") {
    RecordBatchBuilder builder;
    builder.append(1, nullopt, nullopt);
    vector<uint8_t> encoded = builder.build();

    encoded[field::kMagic] = 1;   // v1 batches are not something we can read
    CHECK_THROWS_AS(RecordBatch::parseHeader(encoded), CorruptData);
}

TEST_CASE("A truncated batch is corruption, not a crash") {
    const string payload = "payload";

    RecordBatchBuilder builder;
    builder.append(1, nullopt, bytesOf(payload));
    const vector<uint8_t> encoded = builder.build();

    // Exactly the situation a crash mid-write leaves behind.
    const span<const uint8_t> torn(encoded.data(), encoded.size() - 3);
    CHECK_THROWS_AS(RecordBatch::decodeRecords(torn), CorruptData);

    const span<const uint8_t> headerless(encoded.data(), 20);
    CHECK_THROWS_AS(RecordBatch::parseHeader(headerless), CorruptData);
}

TEST_CASE("A batch length below the header size is rejected") {
    // Otherwise a corrupt length would drive an underflowing subspan downstream.
    vector<uint8_t> bytes(kBatchHeaderSize, 0);
    bytes[field::kBatchLength + 3] = 4;   // batchLength = 4, far too small
    CHECK_THROWS_AS(RecordBatch::totalSizeOf(bytes), CorruptData);
}

TEST_CASE("Record headers round-trip, including a null header value") {
    const string headerKeyA = "trace-id", headerValueA = "abc123";
    const string headerKeyB = "retry-count";
    const string value      = "payload";

    const vector<RecordHeader> headers{
        RecordHeader{bytesOf(headerKeyA), bytesOf(headerValueA)},
        RecordHeader{bytesOf(headerKeyB), nullopt},
    };

    RecordBatchBuilder builder;
    builder.append(1'700'000'000'000, nullopt, bytesOf(value), headers);

    const vector<uint8_t> encoded = builder.build();
    const vector<Record>  records = RecordBatch::decodeRecords(encoded);
    REQUIRE(records.size() == 1);
    REQUIRE(records[0].headers.size() == 2);

    CHECK(stringOf(records[0].headers[0].key) == headerKeyA);
    REQUIRE(records[0].headers[0].value.has_value());
    CHECK(stringOf(*records[0].headers[0].value) == headerValueA);

    CHECK(stringOf(records[0].headers[1].key) == headerKeyB);
    CHECK_FALSE(records[0].headers[1].value.has_value());
}

TEST_CASE("Timestamps are stored as deltas and the maximum is tracked") {
    RecordBatchBuilder builder;
    builder.append(1'700'000'000'000, nullopt, nullopt);
    builder.append(1'700'000'000'900, nullopt, nullopt);   // out of order below
    builder.append(1'700'000'000'400, nullopt, nullopt);

    const vector<uint8_t> encoded = builder.build();
    const BatchHeader     header  = RecordBatch::parseHeader(encoded);

    CHECK(header.firstTimestamp == 1'700'000'000'000);
    CHECK(header.maxTimestamp == 1'700'000'000'900);   // not the last one

    const vector<Record> records = RecordBatch::decodeRecords(encoded);
    REQUIRE(records.size() == 3);
    CHECK(records[0].timestampFrom(header.firstTimestamp) == 1'700'000'000'000);
    CHECK(records[1].timestampFrom(header.firstTimestamp) == 1'700'000'000'900);
    CHECK(records[2].timestampFrom(header.firstTimestamp) == 1'700'000'000'400);
    CHECK(records[2].timestampDelta == 400);
}

TEST_CASE("Producer fields are written as unset until M7 uses them") {
    // Byte-exact from the start: changing the format at M7 would invalidate every
    // segment file and force a rewrite of the codec, index and recovery scan at
    // exactly the point where replication is already the hard problem.
    RecordBatchBuilder builder;
    builder.append(1, nullopt, nullopt);

    const vector<uint8_t> encoded = builder.build();
    const BatchHeader     header  = RecordBatch::parseHeader(encoded);
    CHECK(header.producerId == -1);
    CHECK(header.producerEpoch == -1);
    CHECK(header.baseSequence == -1);
    CHECK(header.partitionLeaderEpoch == kNoEpoch);
    CHECK(header.baseOffset == Offset{0});   // the broker stamps this
}

TEST_CASE("A compressed batch reports that the codec is not implemented") {
    // M1 handles the attribute flag; the algorithms land once there is a broker
    // to measure them on.
    RecordBatchBuilder builder(Compression::Lz4);
    builder.append(1, nullopt, nullopt);

    const vector<uint8_t> encoded = builder.build();
    CHECK(RecordBatch::parseHeader(encoded).compression() == Compression::Lz4);
    CHECK_THROWS_AS(RecordBatch::decodeRecords(encoded), Error);
}

TEST_CASE("Building an empty batch is refused") {
    RecordBatchBuilder builder;
    CHECK(builder.empty());
    CHECK_THROWS_AS(builder.build(), Error);
}
