#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

namespace dariyakyu {

// Zigzag + base-128 variable-length integers, exactly as Kafka's v2 record
// format uses them (the encoding originates in Protocol Buffers).
//
// Why records use these at all: every integer inside a record is a *delta* — an
// offset delta, a timestamp delta, a length. Deltas are small, and small numbers
// should cost one byte rather than four. On a 50-byte message, four fixed int32
// length fields would be 32% of the record.
//
// Base-128 stores seven bits of payload per byte, with the top bit meaning
// "another byte follows".
//
// Zigzag exists because base-128 alone is terrible at negative numbers: two's
// complement makes -1 all ones, so it would encode in the maximum number of
// bytes. Zigzag interleaves positives and negatives onto the unsigned line —
// 0, -1, 1, -2, 2 becomes 0, 1, 2, 3, 4 — so numbers small in magnitude are
// small when encoded, whichever sign they carry. Kafka needs that: a null key
// is length -1, and it should not cost five bytes.
//
// Every varint in the v2 record format is zigzag-encoded and signed, including
// lengths, because -1 is a meaningful length meaning null.

// Bytes the value will occupy once encoded. Used to size a buffer before
// writing, and to compute a record's length prefix (which is itself a varint,
// so the length of the length matters).
std::size_t varintSize(std::int32_t value);
std::size_t varlongSize(std::int64_t value);

// Encode into `out`, which must be large enough. Returns bytes written.
std::size_t encodeVarint(std::int32_t value, std::span<std::uint8_t> out);
std::size_t encodeVarlong(std::int64_t value, std::span<std::uint8_t> out);

// Decode from the front of `in`. Returns bytes consumed and writes the value to
// `value`. Throws CorruptData if the input ends mid-number or the encoding runs
// longer than the type can hold — both of which mean the batch is damaged, not
// that the caller made a mistake.
std::size_t decodeVarint(std::span<const std::uint8_t> in, std::int32_t& value);
std::size_t decodeVarlong(std::span<const std::uint8_t> in, std::int64_t& value);

// Largest encodings: ceil(32/7) = 5 bytes, ceil(64/7) = 10 bytes.
inline constexpr std::size_t kMaxVarintBytes  = 5;
inline constexpr std::size_t kMaxVarlongBytes = 10;

}  // namespace dariyakyu
