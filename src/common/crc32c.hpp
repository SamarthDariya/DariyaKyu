#pragma once

#include <cstdint>
#include <span>

namespace dariyakyu {

// CRC-32C (Castagnoli), the checksum Kafka's v2 record batch carries.
//
// Note this is NOT zlib's CRC-32. Same width, different polynomial —
// 0x1EDC6F41 rather than 0x04C11DB7 — so the two produce different values and
// are not interchangeable. Castagnoli was chosen by Kafka (and by iSCSI, ext4,
// and others) for two reasons: better error detection at the message sizes that
// matter, and a dedicated x86 instruction (`crc32`, SSE4.2) that runs roughly
// an order of magnitude faster than a table lookup.
//
// This is the portable table-driven implementation. The hardware path is banked
// for M9, behind a runtime CPU check and justified with benchmarks rather than
// assumed — see DESIGN.md.
//
// What the checksum is for: a crash mid-write leaves a partial batch on disk,
// and those bytes look like perfectly plausible data. Recovery walks the active
// segment validating checksums and truncates at the first failure. Without it
// there is no way to tell a complete record from a torn one.

// Checksum of a whole buffer.
std::uint32_t crc32c(std::span<const std::uint8_t> data);

// Incremental form, for checksumming a batch whose header and body are not
// contiguous in memory yet. Start with crc32cInit(), feed chunks, then finish.
//
//   uint32_t crc = crc32cInit();
//   crc = crc32cUpdate(crc, header);
//   crc = crc32cUpdate(crc, body);
//   crc = crc32cFinish(crc);
std::uint32_t crc32cInit();
std::uint32_t crc32cUpdate(std::uint32_t crc, std::span<const std::uint8_t> data);
std::uint32_t crc32cFinish(std::uint32_t crc);

}  // namespace dariyakyu
