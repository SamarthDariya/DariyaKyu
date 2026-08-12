#include "common/varint.hpp"

#include "common/errors.hpp"

using namespace std;

namespace dariyakyu {

namespace {

// Zigzag: map signed to unsigned so that small magnitudes stay small.
//   0 -> 0, -1 -> 1, 1 -> 2, -2 -> 3, 2 -> 4, ...
//
// The shift is done on the unsigned type deliberately: left-shifting a negative
// signed value is undefined behaviour, while unsigned shifting is defined to
// wrap. The right shift stays signed because it must be arithmetic — it is
// producing the all-ones or all-zeroes mask.
uint32_t zigzag32(int32_t value) {
    return (static_cast<uint32_t>(value) << 1) ^ static_cast<uint32_t>(value >> 31);
}

uint64_t zigzag64(int64_t value) {
    return (static_cast<uint64_t>(value) << 1) ^ static_cast<uint64_t>(value >> 63);
}

int32_t unzigzag32(uint32_t encoded) {
    return static_cast<int32_t>((encoded >> 1) ^ (~(encoded & 1) + 1));
}

int64_t unzigzag64(uint64_t encoded) {
    return static_cast<int64_t>((encoded >> 1) ^ (~(encoded & 1) + 1));
}

template <typename Unsigned>
size_t base128Size(Unsigned value) {
    size_t bytes = 1;
    while (value >= 0x80) {
        value >>= 7;
        ++bytes;
    }
    return bytes;
}

template <typename Unsigned>
size_t writeBase128(Unsigned value, span<uint8_t> out) {
    size_t written = 0;
    while (value >= 0x80) {
        if (written >= out.size()) throw Error("varint: output buffer too small");
        out[written++] = static_cast<uint8_t>((value & 0x7F) | 0x80);
        value >>= 7;
    }
    if (written >= out.size()) throw Error("varint: output buffer too small");
    out[written++] = static_cast<uint8_t>(value);
    return written;
}

// `maxBytes` bounds the encoding so a damaged batch cannot make us read forever,
// and so an over-long encoding of a small type is rejected rather than silently
// truncated.
template <typename Unsigned>
size_t readBase128(span<const uint8_t> in, Unsigned& value, size_t maxBytes) {
    Unsigned result = 0;
    size_t   read   = 0;
    unsigned shift  = 0;

    while (true) {
        if (read >= in.size())
            throw CorruptData("varint: input ended mid-number after " + to_string(read) +
                              " byte(s)");
        if (read >= maxBytes)
            throw CorruptData("varint: encoding longer than " + to_string(maxBytes) +
                              " bytes, which the target type cannot hold");

        const uint8_t byte = in[read++];
        result |= static_cast<Unsigned>(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) break;
        shift += 7;
    }

    value = result;
    return read;
}

}  // namespace

size_t varintSize(int32_t value) {
    return base128Size(zigzag32(value));
}

size_t varlongSize(int64_t value) {
    return base128Size(zigzag64(value));
}

size_t encodeVarint(int32_t value, span<uint8_t> out) {
    return writeBase128(zigzag32(value), out);
}

size_t encodeVarlong(int64_t value, span<uint8_t> out) {
    return writeBase128(zigzag64(value), out);
}

size_t decodeVarint(span<const uint8_t> in, int32_t& value) {
    uint32_t     encoded = 0;
    const size_t read    = readBase128(in, encoded, kMaxVarintBytes);
    value                = unzigzag32(encoded);
    return read;
}

size_t decodeVarlong(span<const uint8_t> in, int64_t& value) {
    uint64_t     encoded = 0;
    const size_t read    = readBase128(in, encoded, kMaxVarlongBytes);
    value                = unzigzag64(encoded);
    return read;
}

}  // namespace dariyakyu
