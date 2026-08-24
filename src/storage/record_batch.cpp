#include "storage/record_batch.hpp"

#include <string>

#include "common/crc32c.hpp"
#include "common/errors.hpp"
#include "common/varint.hpp"

using namespace std;

namespace dariyakyu::storage {

namespace {

// Attribute bits 0-2 hold the compression codec.
constexpr int16_t kCompressionMask = 0x07;

// A length of -1 means null, as distinct from 0 meaning empty.
constexpr int32_t kNullLength = -1;

void requireAtLeast(span<const uint8_t> bytes, size_t needed, const char* what) {
    if (bytes.size() < needed)
        throw CorruptData(string("record batch: ") + what + " needs " + to_string(needed) +
                          " bytes but only " + to_string(bytes.size()) + " available");
}

void writeNullableBytes(BufferWriter& out, const optional<span<const uint8_t>>& value) {
    if (!value) {
        out.writeVarint(kNullLength);
        return;
    }
    out.writeVarint(static_cast<int32_t>(value->size()));
    out.writeBytes(*value);
}

optional<span<const uint8_t>> readNullableBytes(BufferReader& in) {
    const int32_t length = in.readVarint();
    if (length == kNullLength) return nullopt;
    if (length < 0)
        throw CorruptData("record batch: negative length " + to_string(length) +
                          " that is not the null marker");
    return in.readBytes(static_cast<size_t>(length));
}

}  // namespace

Compression BatchHeader::compression() const {
    return static_cast<Compression>(attributes & kCompressionMask);
}

// --------------------------------------------------------------------------
// Parsing
// --------------------------------------------------------------------------

BatchHeader RecordBatch::parseHeader(span<const uint8_t> bytes) {
    requireAtLeast(bytes, kBatchHeaderSize, "header");

    BufferReader in(bytes);
    BatchHeader  header;

    header.baseOffset           = Offset{in.readInt64()};
    header.batchLength          = in.readInt32();
    header.partitionLeaderEpoch = in.readInt32();
    header.magic                = in.readInt8();

    if (header.magic != kMagicV2)
        throw CorruptData("record batch: magic byte " + to_string(header.magic) +
                          ", expected " + to_string(kMagicV2));

    header.crc             = in.readUint32();
    header.attributes      = in.readInt16();
    header.lastOffsetDelta = in.readInt32();
    header.firstTimestamp  = in.readInt64();
    header.maxTimestamp    = in.readInt64();
    header.producerId      = in.readInt64();
    header.producerEpoch   = in.readInt16();
    header.baseSequence    = in.readInt32();
    header.recordCount     = in.readInt32();

    if (header.recordCount < 0)
        throw CorruptData("record batch: negative record count " + to_string(header.recordCount));
    if (header.batchLength < 0)
        throw CorruptData("record batch: negative batch length " + to_string(header.batchLength));

    return header;
}

size_t RecordBatch::totalSizeOf(span<const uint8_t> bytes) {
    // Only the first twelve bytes are needed: a recovery scan reads them, jumps
    // that far forward, and reads the next twelve. It never decodes a body it is
    // only stepping over.
    requireAtLeast(bytes, kBytesBeforeBatchLengthCounted, "length prefix");

    BufferReader in(bytes);
    in.skip(field::kBatchLength);
    const int32_t batchLength = in.readInt32();

    // A batch cannot be shorter than its own header. Checking here rather than
    // at each use means a corrupt length can never produce an underflowing
    // subspan later — the arithmetic downstream assumes total >= header size.
    constexpr int32_t kMinBatchLength =
        static_cast<int32_t>(kBatchHeaderSize - kBytesBeforeBatchLengthCounted);
    if (batchLength < kMinBatchLength)
        throw CorruptData("record batch: batch length " + to_string(batchLength) +
                          " is below the minimum of " + to_string(kMinBatchLength));

    return static_cast<size_t>(batchLength) + kBytesBeforeBatchLengthCounted;
}

uint32_t RecordBatch::computeCrc(span<const uint8_t> bytes) {
    const size_t total = totalSizeOf(bytes);
    requireAtLeast(bytes, total, "batch body");
    return crc32c(bytes.subspan(kCrcCoverageStart, total - kCrcCoverageStart));
}

bool RecordBatch::verifyCrc(span<const uint8_t> bytes) {
    const BatchHeader header = parseHeader(bytes);
    return computeCrc(bytes) == header.crc;
}

vector<Record> RecordBatch::decodeRecords(span<const uint8_t> bytes) {
    const BatchHeader header = parseHeader(bytes);

    if (header.compression() != Compression::None)
        throw Error("record batch: compression codec " +
                    to_string(static_cast<int>(header.compression())) +
                    " is not supported yet (M1 handles the flag, not the algorithm)");

    const size_t total = totalSizeOf(bytes);
    requireAtLeast(bytes, total, "batch body");

    BufferReader   in(bytes.subspan(kBatchHeaderSize, total - kBatchHeaderSize));
    vector<Record> records;
    records.reserve(static_cast<size_t>(header.recordCount));

    for (int32_t i = 0; i < header.recordCount; ++i) {
        const int32_t recordLength = in.readVarint();
        if (recordLength < 0)
            throw CorruptData("record batch: negative record length " + to_string(recordLength));

        const size_t recordEnd = in.position() + static_cast<size_t>(recordLength);

        Record record;
        record.attributes     = in.readInt8();
        record.timestampDelta = in.readVarlong();
        record.offsetDelta    = in.readVarint();
        record.key            = readNullableBytes(in);
        record.value          = readNullableBytes(in);

        const int32_t headerCount = in.readVarint();
        if (headerCount < 0)
            throw CorruptData("record batch: negative header count " + to_string(headerCount));

        record.headers.reserve(static_cast<size_t>(headerCount));
        for (int32_t h = 0; h < headerCount; ++h) {
            RecordHeader recordHeader;
            const int32_t keyLength = in.readVarint();
            if (keyLength < 0)
                throw CorruptData("record batch: header key length " + to_string(keyLength) +
                                  " (header keys may not be null)");
            recordHeader.key   = in.readBytes(static_cast<size_t>(keyLength));
            recordHeader.value = readNullableBytes(in);
            record.headers.push_back(std::move(recordHeader));
        }

        // The record's own length prefix is the authority on where it ends. If
        // the fields inside disagree, the batch is damaged — trusting the field
        // walk instead would silently resynchronise onto garbage.
        if (in.position() != recordEnd)
            throw CorruptData("record batch: record " + to_string(i) + " declared " +
                              to_string(recordLength) + " bytes but its fields consumed a " +
                              "different amount");

        records.push_back(std::move(record));
    }

    return records;
}

// --------------------------------------------------------------------------
// Broker-side stamping
// --------------------------------------------------------------------------

void RecordBatch::stampBaseOffset(span<uint8_t> bytes, Offset baseOffset) {
    requireAtLeast(bytes, kBatchHeaderSize, "header");

    const auto raw = static_cast<uint64_t>(baseOffset.value());
    for (size_t i = 0; i < 8; ++i)
        bytes[field::kBaseOffset + i] = static_cast<uint8_t>(raw >> (56 - 8 * i));
}

void RecordBatch::stampLeaderEpoch(span<uint8_t> bytes, Epoch epoch) {
    requireAtLeast(bytes, kBatchHeaderSize, "header");

    const auto raw = static_cast<uint32_t>(epoch);
    for (size_t i = 0; i < 4; ++i)
        bytes[field::kPartitionLeaderEpoch + i] = static_cast<uint8_t>(raw >> (24 - 8 * i));
}

// --------------------------------------------------------------------------
// Building
// --------------------------------------------------------------------------

RecordBatchBuilder::RecordBatchBuilder(Compression compression) : compression_(compression) {}

void RecordBatchBuilder::append(int64_t timestamp, optional<span<const uint8_t>> key,
                                optional<span<const uint8_t>> value,
                                span<const RecordHeader>      headers) {
    if (count_ == 0) {
        firstTimestamp_ = timestamp;
        maxTimestamp_   = timestamp;
    } else if (timestamp > maxTimestamp_) {
        maxTimestamp_ = timestamp;
    }

    // The record is encoded into scratch first, because its length prefix is a
    // varint of the encoded size — which is not known until the encoding exists.
    BufferWriter scratch;
    scratch.writeInt8(0);                                // per-record attributes: unused in v2
    scratch.writeVarlong(timestamp - firstTimestamp_);   // delta, not absolute
    scratch.writeVarint(count_);                         // offset delta within the batch
    writeNullableBytes(scratch, key);
    writeNullableBytes(scratch, value);

    scratch.writeVarint(static_cast<int32_t>(headers.size()));
    for (const RecordHeader& header : headers) {
        scratch.writeVarint(static_cast<int32_t>(header.key.size()));
        scratch.writeBytes(header.key);
        writeNullableBytes(scratch, header.value);
    }

    const vector<uint8_t> encoded = scratch.take();
    records_.writeVarint(static_cast<int32_t>(encoded.size()));
    records_.writeBytes(encoded);

    ++count_;
}

vector<uint8_t> RecordBatchBuilder::build() {
    if (empty()) throw Error("record batch: refusing to build a batch with no records");

    const span<const uint8_t> body = records_.view();

    BufferWriter out(kBatchHeaderSize + body.size());

    // Left for the broker to stamp on arrival — a producer cannot know either.
    out.writeInt64(0);                                    // baseOffset
    const size_t batchLengthAt = out.position();
    out.writeInt32(0);                                    // batchLength, patched below
    out.writeInt32(kNoEpoch);                             // partitionLeaderEpoch
    out.writeInt8(kMagicV2);

    const size_t crcAt = out.position();
    out.writeUint32(0);                                   // crc, patched below

    out.writeInt16(static_cast<int16_t>(static_cast<uint8_t>(compression_) & kCompressionMask));
    out.writeInt32(count_ - 1);                           // lastOffsetDelta
    out.writeInt64(firstTimestamp_);
    out.writeInt64(maxTimestamp_);
    out.writeInt64(-1);                                   // producerId    — M7
    out.writeInt16(-1);                                   // producerEpoch — M7
    out.writeInt32(-1);                                   // baseSequence  — M7
    out.writeInt32(count_);

    out.writeBytes(body);

    out.patchInt32(batchLengthAt,
                   static_cast<int32_t>(out.position() - kBytesBeforeBatchLengthCounted));

    // Computed last, over everything from `attributes` onward. Note what is NOT
    // covered: baseOffset and partitionLeaderEpoch, precisely so the broker can
    // fill them in later without touching this value.
    const span<const uint8_t> written = out.view();
    out.patchUint32(crcAt, crc32c(written.subspan(kCrcCoverageStart)));

    return out.take();
}

}  // namespace dariyakyu::storage
