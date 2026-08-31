#pragma once

#include <filesystem>
#include <string>

#include "common/types.hpp"

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

}  // namespace dariyakyu::storage
