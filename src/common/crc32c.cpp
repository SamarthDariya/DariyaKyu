#include "common/crc32c.hpp"

#include <array>

using namespace std;

namespace dariyakyu {

namespace {

// Castagnoli polynomial, bit-reflected. The reflected form lets the algorithm
// shift right and consume bytes least-significant-bit first, which is what makes
// the one-byte-at-a-time table method work.
constexpr uint32_t kReflectedPolynomial = 0x82F63B78u;

// Built at compile time, so there is no static initialisation order to worry
// about and no first-call cost.
constexpr array<uint32_t, 256> makeTable() {
    array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t crc = i;
        for (int bit = 0; bit < 8; ++bit)
            crc = (crc & 1u) ? (crc >> 1) ^ kReflectedPolynomial : (crc >> 1);
        table[i] = crc;
    }
    return table;
}

constexpr array<uint32_t, 256> kTable = makeTable();

}  // namespace

uint32_t crc32cInit() {
    // All ones, so that leading zero bytes still change the checksum.
    return 0xFFFFFFFFu;
}

uint32_t crc32cUpdate(uint32_t crc, span<const uint8_t> data) {
    for (const uint8_t byte : data) crc = kTable[(crc ^ byte) & 0xFFu] ^ (crc >> 8);
    return crc;
}

uint32_t crc32cFinish(uint32_t crc) {
    // Final inversion, mirroring the all-ones start. Together these make the
    // checksum sensitive to trailing zeroes as well as leading ones.
    return crc ^ 0xFFFFFFFFu;
}

uint32_t crc32c(span<const uint8_t> data) {
    return crc32cFinish(crc32cUpdate(crc32cInit(), data));
}

}  // namespace dariyakyu
