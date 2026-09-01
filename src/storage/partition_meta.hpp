#pragma once

#include <cstdint>
#include <filesystem>
#include <vector>

#include "common/types.hpp"
#include "storage/log.hpp"

namespace dariyakyu::storage {

// `partition.meta` — what a partition knows about itself.
//
// Without this file, a partition's roll and retention rules would live only in
// the controller's metadata, and a broker that boots while the controller is
// unreachable would have to either refuse to serve or guess at defaults. Guessing
// is the dangerous option: a partition configured to keep one hour of data,
// reopened with the seven-day default, quietly stops deleting anything.
//
// So the partition is self-describing. LogManager recovers every partition's
// configuration from disk alone (DESIGN.md Part II chunk 2).
//
// # Format
//
// Binary, big-endian, matching everything else this codebase writes:
//
//   magic                uint32   "DKPM"
//   version              int16
//   topic                int16 length + bytes
//   partition            int32
//   maxSegmentBytes      int64
//   maxSegmentAgeMs      int64
//   indexIntervalBytes   int64
//   maxIndexBytes        int64
//   retentionMs          int64
//   retentionBytes       int64    -1 for unlimited
//   segmentDeleteDelayMs int64
//
// Binary rather than text because it is broker state, not user configuration,
// and it reuses the codec that already exists. `dariyakyu-dump` is the intended
// way to read one.
//
// The magic exists so pointing this at the wrong file fails immediately rather
// than decoding lengths out of unrelated bytes. There is no checksum, and it does
// not need one: the file is replaced atomically (see below), so a reader sees the
// whole old version or the whole new one, never a torn mixture.
//
// `retentionBytes` is where the -1 sentinel lives. RetentionPolicy holds an
// optional so that "unlimited" and "keep almost nothing" cannot be confused in
// memory; the translation happens here, at the file boundary, and nowhere else.
struct PartitionMeta {
    static constexpr std::int16_t kVersion  = 1;
    static constexpr std::uint32_t kMagic   = 0x444B504D;   // "DKPM"
    static constexpr const char*  kFileName = "partition.meta";

    TopicPartition tp;
    LogConfig      config;
};

std::vector<std::uint8_t> encodePartitionMeta(const PartitionMeta& meta);

// Writes `dir/partition.meta`, replacing any existing one atomically.
//
// Three steps, and each is load-bearing:
//
//   1. write a temporary file beside it
//   2. fsync the temporary — a rename makes the NAME change atomically, but
//      says nothing about whether the bytes reached the disk. Without this, a
//      crash can leave a correctly-named file full of nothing.
//   3. rename over the target, then fsync the DIRECTORY so the rename itself
//      survives a crash
//
// A partition whose meta file is empty or half-written cannot be interpreted, and
// unlike an index it cannot be rebuilt — the configuration is not derivable from
// the log. So this is one of the few places worth paying for durability.
void writePartitionMeta(const std::filesystem::path& dir, const PartitionMeta& meta);

}  // namespace dariyakyu::storage
