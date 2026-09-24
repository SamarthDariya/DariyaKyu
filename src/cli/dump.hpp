#pragma once

#include <cstdint>
#include <filesystem>

namespace dariyakyu::cli {

// Prints what is inside a partition directory, READ-ONLY.
//
// Deliberately does not go through Log::open, which recovers its newest segment
// and truncates at the first bad batch — exactly the wrong thing for a tool whose
// job is to show you what the damage looks like. Every segment is opened as a
// SealedSegment, which reads headers and never writes.
//
// The one command here that needs no broker, which is precisely when it is
// wanted: when the broker will not start.
void inspectPartition(const std::filesystem::path& dir);

// Writes a partition full of records, for looking at. Destroys whatever was
// there.
// `compact` writes the partition as a compacted topic, runs one cleaner pass over
// it, and leaves the result — so the dump below shows real holes and a real
// tombstone rather than a description of them.
void generatePartition(const std::filesystem::path& dir, int records,
                       std::uint64_t segmentBytes, bool compact = false);

}  // namespace dariyakyu::cli
