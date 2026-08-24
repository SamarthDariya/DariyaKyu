#pragma once

#include <cstdint>
#include <optional>
#include <span>
#include <vector>

#include "common/buffer.hpp"
#include "common/types.hpp"

namespace dariyakyu::storage {

// Kafka's v2 record batch, byte-exact.
//
// A batch is the unit of storage AND of transfer (DESIGN.md decision 13): the
// bytes written to a segment are the bytes sendfile() puts on a consumer's
// socket. Nothing re-encodes them in between — which is why the layout below is
// shaped the way it is rather than the way that would be convenient.
//
//   ┌──── outside the CRC ─────────────────────────────┐
//   │ baseOffset           int64   ← broker stamps     │
//   │ batchLength          int32                       │
//   │ partitionLeaderEpoch int32   ← broker stamps     │
//   │ magic                int8    (= 2)               │
//   ├──────────────────────────────────────────────────┤
//   │ crc32c               uint32                      │
//   ├──── covered by the CRC ──────────────────────────┤
//   │ attributes           int16                       │
//   │ lastOffsetDelta      int32                       │
//   │ firstTimestamp       int64                       │
//   │ maxTimestamp         int64                       │
//   │ producerId           int64   ← idempotence (M7)  │
//   │ producerEpoch        int16                       │
//   │ baseSequence         int32                       │
//   │ recordCount          int32                       │
//   │ [ records ... ]              ← may be compressed │
//   └──────────────────────────────────────────────────┘
//
// Three properties of that ordering, each load-bearing:
//
//  1. baseOffset and partitionLeaderEpoch come BEFORE the crc, so the broker can
//     stamp them into an arriving batch without recomputing a checksum over a
//     body it may not even be able to read.
//  2. lastOffsetDelta and recordCount sit outside the compressed region, so the
//     broker can advance its log end offset knowing only how MANY records
//     arrived, never what they are.
//  3. Records carry deltas, not absolute offsets, so a producer can encode
//     before knowing which offsets the broker will assign.

inline constexpr std::int8_t  kMagicV2         = 2;
inline constexpr std::size_t  kBatchHeaderSize = 61;

// Byte positions within the header. Named because three different pieces of
// code index into them and a stray literal would be a silent corruption.
namespace field {
inline constexpr std::size_t kBaseOffset           = 0;
inline constexpr std::size_t kBatchLength          = 8;
inline constexpr std::size_t kPartitionLeaderEpoch = 12;
inline constexpr std::size_t kMagic                = 16;
inline constexpr std::size_t kCrc                  = 17;
inline constexpr std::size_t kAttributes           = 21;
}  // namespace field

// batchLength counts the bytes that follow it, so the total size of a batch is
// batchLength plus the two fields that precede it.
inline constexpr std::size_t kBytesBeforeBatchLengthCounted = 12;

// The checksum covers everything from `attributes` to the end of the batch.
inline constexpr std::size_t kCrcCoverageStart = field::kAttributes;

enum class Compression : std::uint8_t {
    None   = 0,
    Gzip   = 1,
    Snappy = 2,
    Lz4    = 3,
    Zstd   = 4,
};

// A record header: application metadata attached to one record. Parsed and
// preserved from M1 so the format never has to change, though nothing in the
// broker consumes them before Tier A.
struct RecordHeader {
    std::span<const std::uint8_t>                key;
    std::optional<std::span<const std::uint8_t>> value;   // headers may be null-valued
};

// A decoded record.
//
// Key and value are spans INTO the buffer that was decoded, never copies — the
// broker must not own record bytes. A Record is therefore valid only while that
// buffer lives. Callers that need to outlive it (the CLI printing a message)
// copy explicitly.
//
// optional distinguishes null from empty: a null key is length -1 and means "no
// key at all", while an empty key is length 0. Compaction treats a null VALUE as
// a tombstone, so collapsing the two would delete data.
struct Record {
    std::int32_t                                 offsetDelta    = 0;
    std::int64_t                                 timestampDelta = 0;
    std::optional<std::span<const std::uint8_t>> key;
    std::optional<std::span<const std::uint8_t>> value;
    std::vector<RecordHeader>                    headers;
    std::int8_t                                  attributes = 0;

    Offset       offsetFrom(Offset baseOffset) const { return baseOffset + offsetDelta; }
    std::int64_t timestampFrom(std::int64_t firstTimestamp) const {
        return firstTimestamp + timestampDelta;
    }
};

// The fixed prefix, parsed without touching the body. This is all the broker's
// write path needs.
struct BatchHeader {
    Offset        baseOffset{0};
    std::int32_t  batchLength          = 0;
    Epoch         partitionLeaderEpoch = kNoEpoch;
    std::int8_t   magic                = kMagicV2;
    std::uint32_t crc                  = 0;
    std::int16_t  attributes           = 0;
    std::int32_t  lastOffsetDelta      = 0;
    std::int64_t  firstTimestamp       = 0;
    std::int64_t  maxTimestamp         = 0;
    std::int64_t  producerId           = -1;
    std::int16_t  producerEpoch        = -1;
    std::int32_t  baseSequence         = -1;
    std::int32_t  recordCount          = 0;

    Compression compression() const;

    // The offset of this batch's last record. Advancing a log end offset needs
    // exactly this, and it comes from the header alone.
    Offset lastOffset() const { return baseOffset + lastOffsetDelta; }

    std::size_t totalSize() const {
        return static_cast<std::size_t>(batchLength) + kBytesBeforeBatchLengthCounted;
    }
};

class RecordBatch {
public:
    // Parses the fixed prefix only. Throws CorruptData on a short buffer or an
    // unrecognised magic byte.
    static BatchHeader parseHeader(std::span<const std::uint8_t> bytes);

    // Total size of the batch starting at the front of `bytes`, read from the
    // length field alone. This is how a recovery scan walks from one batch to
    // the next without decoding anything.
    static std::size_t totalSizeOf(std::span<const std::uint8_t> bytes);

    static std::uint32_t computeCrc(std::span<const std::uint8_t> bytes);
    static bool          verifyCrc(std::span<const std::uint8_t> bytes);

    // Decodes every record. Borrows from `bytes`. Throws CorruptData if the
    // batch is damaged, which unlike a routine out-of-range read is genuinely
    // exceptional — it happens once per recovery, not once per poll.
    static std::vector<Record> decodeRecords(std::span<const std::uint8_t> bytes);

    // Decoding a temporary buffer is a use-after-free waiting to happen: the
    // returned Records point into bytes that die at the end of the expression.
    //
    //     auto records = RecordBatch::decodeRecords(builder.build());   // no
    //     auto encoded = builder.build();                               // yes
    //     auto records = RecordBatch::decodeRecords(encoded);
    //
    // A temporary vector would otherwise convert silently to a span, so the
    // rvalue overload is deleted to turn that into a compile error. Note the
    // other entry points need no such guard — they return plain values that
    // borrow nothing.
    static std::vector<Record> decodeRecords(std::vector<std::uint8_t>&&) = delete;

    // The only writes the broker performs on producer bytes: twelve header
    // bytes, all of them ahead of the checksum, so no CRC is recomputed.
    static void stampBaseOffset(std::span<std::uint8_t> bytes, Offset baseOffset);
    static void stampLeaderEpoch(std::span<std::uint8_t> bytes, Epoch epoch);
};

// Producer-side construction. Accumulating records over time (linger.ms,
// batch.size) is client policy and lives in the CLI at M4; this only builds the
// bytes once the caller has decided what goes in.
class RecordBatchBuilder {
public:
    explicit RecordBatchBuilder(Compression compression = Compression::None);

    // `timestamp` is absolute; the encoder stores it as a delta from the
    // batch's first timestamp.
    void append(std::int64_t timestamp, std::optional<std::span<const std::uint8_t>> key,
                std::optional<std::span<const std::uint8_t>> value,
                std::span<const RecordHeader>                headers = {});

    bool         empty() const { return count_ == 0; }
    std::int32_t recordCount() const { return count_; }

    // Writes the header, patches batchLength and the checksum, and returns the
    // finished bytes. baseOffset and partitionLeaderEpoch are left at 0 and -1
    // for the broker to stamp on arrival.
    std::vector<std::uint8_t> build();

private:
    Compression  compression_;
    BufferWriter records_;
    std::int32_t count_          = 0;
    std::int64_t firstTimestamp_ = 0;
    std::int64_t maxTimestamp_   = 0;
};

}  // namespace dariyakyu::storage
