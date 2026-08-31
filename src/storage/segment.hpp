#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>

#include "common/types.hpp"
#include "storage/offset_index.hpp"

namespace dariyakyu::storage {

// A segment's two files are named for the offset of their first record:
//
//   00000000000000000000.log     00000000000000000000.index
//   00000000000001073741.log     00000000000001073741.index
//
// Twenty zero-padded digits, so that sorting the names as STRINGS gives the same
// order as sorting the offsets as NUMBERS. Without padding, "9.log" sorts after
// "10.log", because '9' > '1' as a character. With it, "…009" sorts before
// "…010" and the two orders agree.
//
// That is the property Log will rely on: to find which segment holds an offset,
// it searches the filenames, and only then opens that segment's index. Twenty
// digits because the largest int64 is 19 digits, plus one to spare.

// Offset(1073741) -> "00000000000001073741"
std::string segmentBaseName(Offset baseOffset);

// The two paths inside a partition directory.
std::filesystem::path segmentLogPath(const std::filesystem::path& dir, Offset baseOffset);
std::filesystem::path segmentIndexPath(const std::filesystem::path& dir, Offset baseOffset);

// The reverse: "…/00000000000001073741.log" -> Offset(1073741).
//
// Strict, and throws CorruptData on anything that is not exactly twenty digits
// followed by ".log". M3's LogManager will iterate a partition directory it does
// not fully own — partition.meta, leader-epoch-checkpoint, an editor's swap
// file, whatever someone drops in. A name this cannot interpret must not
// silently become Offset(0), because a phantom segment claiming to start at
// offset 0 would shadow the real first segment and hide records.
Offset baseOffsetFromLogPath(const std::filesystem::path& logFile);

// When to stop writing to a segment and start a new one.
//
// Rolling is what bounds three separate things, and a partition that never rolls
// breaks all of them:
//
//   - retention deletes whole segments, so a log with one segment never shrinks
//   - recovery only scans the newest segment, so one that grows forever makes
//     startup time unbounded
//   - only the newest segment is mutable, so a log with one segment has nothing
//     that can be read without synchronisation
struct RollPolicy {
    // Kafka's default, and it is a trade rather than a magic number: bigger
    // segments mean fewer files and less frequent rolling, but retention can
    // only free space a whole segment at a time, so a 1 GiB segment means
    // deletion happens in 1 GiB steps.
    std::uint64_t maxSegmentBytes = 1ull << 30;   // 1 GiB

    // Age of the segment's FIRST append, measured on the wall clock — not the
    // largest record timestamp. Record timestamps are chosen by producers, and
    // letting a client's clock decide the broker's file layout means one skewed
    // producer can hold a segment open forever, or force a roll per record.
    //
    // A low-traffic partition needs this: without it, a topic receiving one
    // record a day would keep the same active segment for years, and since
    // retention only touches sealed segments, nothing would ever be deleted.
    std::int64_t maxSegmentAgeMs = 7ll * 24 * 60 * 60 * 1000;   // 7 days

    // Bytes of log between index entries — the sparseness knob. One entry per
    // 4 KB means a lookup lands within 4 KB of its target, so the forward scan
    // that finishes the job touches a page or two.
    //
    // Smaller: faster lookups, bigger index. Larger: the opposite. 4 KB is one
    // page, which is the natural unit for a scan the kernel will fault in.
    std::uint64_t indexIntervalBytes = 4096;

    // Preallocated up front, because extending a live mmap and then touching the
    // new pages raises SIGBUS. Trimmed back to the entries actually used when
    // the segment is sealed.
    std::size_t maxIndexBytes = 10ull << 20;   // 10 MiB
};

// The index must be able to describe a full-size segment without filling up.
//
// A full index forces a roll — everything claiming to be "bounded by one index
// interval" stops being bounded once entries are dropped — so if these defaults
// were mismatched, segments would roll on a full index long before reaching
// maxSegmentBytes, and the size knob would silently do nothing.
//
//   10 MiB / 8 bytes  = 1,310,720 entries
//   1,310,720 x 4 KiB = 5 GiB of log covered, against a 1 GiB segment
static_assert((RollPolicy{}.maxIndexBytes / OffsetIndex::kEntrySize) *
                      RollPolicy{}.indexIntervalBytes >
                  RollPolicy{}.maxSegmentBytes,
              "the default index is too small to describe a default-sized segment, so segments "
              "would roll on a full index rather than on their size");

}  // namespace dariyakyu::storage
