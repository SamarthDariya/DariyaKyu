#pragma once

#include <compare>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>

namespace dariyakyu {

// A record's position within a partition, counted in records.
//
// This is a distinct type rather than an alias for int64_t on purpose. This
// codebase's entire job is translating between "record number 4,500,000" and
// "byte 2,847,232 of this file", and both of those are 64-bit integers. Passing
// one where the other belongs compiles, runs, and returns plausible garbage —
// so the compiler is made to reject it instead.
//
// An offset is also the whole of a consumer's state (DESIGN.md decision 2),
// which is why it stays a single number and gains no other members.
class Offset {
public:
    constexpr Offset() = default;
    constexpr explicit Offset(std::int64_t value) : value_(value) {}

    constexpr std::int64_t value() const { return value_; }

    // Ordering matters as much as equality: segment lookup is "greatest base
    // offset <= target", and Log keeps its sealed segments in a std::map.
    constexpr auto operator<=>(const Offset&) const = default;
    constexpr bool operator==(const Offset&) const  = default;

    // offset + a count of records = another offset.
    constexpr Offset  operator+(std::int64_t records) const { return Offset(value_ + records); }
    constexpr Offset  operator-(std::int64_t records) const { return Offset(value_ - records); }
    constexpr Offset& operator+=(std::int64_t records) {
        value_ += records;
        return *this;
    }
    constexpr Offset& operator++() {
        ++value_;
        return *this;
    }

    // offset - offset = a *count of records*, not an offset. Encoding that in
    // the return type is most of the reason this class exists: the difference
    // between two positions is a distance, and distances are what get stored as
    // relative offsets inside a segment index and a record batch.
    constexpr std::int64_t operator-(Offset other) const { return value_ - other.value_; }

    std::string toString() const;

private:
    std::int64_t value_ = 0;
};

using PartitionId = std::int32_t;
using NodeId      = std::int32_t;

// Leader epochs and consumer-group generations are both fencing tokens: a
// number that only ever goes up, used to make a stale participant's requests
// self-evidently invalid (DESIGN.md decision 18).
using Epoch = std::int32_t;

inline constexpr Offset kUnknownOffset{-1};
inline constexpr Epoch  kNoEpoch = -1;

// Identifies one partition, which in this system is simultaneously:
//   - one directory of segment files on disk
//   - one Log object in memory
//   - one unit of ordering, and one unit of parallelism
// (DESIGN.md decisions 5-7).
struct TopicPartition {
    std::string topic;
    PartitionId partition = 0;

    bool operator==(const TopicPartition&) const = default;

    // "orders-3" — also the on-disk directory name, so this doubles as the path
    // component rather than being a debug-only convenience.
    //
    // Note the encoding is ambiguous in reverse, because topic names may
    // themselves contain '-': "my-topic-5" could split either way. Parsing back
    // (LogManager's startup scan) must always split at the LAST dash. Kafka has
    // the same ambiguity and resolves it the same way.
    std::string toString() const;
};

// A location in a file. Never the bytes themselves.
//
// This is what the storage layer returns from a read: it reports *where* the
// bytes are, and the network layer hands that straight to sendfile(). If a read
// path ever returns a vector<uint8_t> instead, zero-copy is dead — so the
// return type enforces DESIGN.md decision 13 rather than a comment asking
// politely.
//
// A zero-length range is a valid, successful result: it is what a caught-up
// consumer gets when it polls for an offset the producer has not written yet,
// which is the most common read in the whole system.
struct FileRange {
    int           fd       = -1;
    std::uint64_t position = 0;
    std::size_t   length   = 0;

    bool empty() const { return length == 0; }
};

}  // namespace dariyakyu

template <>
struct std::hash<dariyakyu::Offset> {
    std::size_t operator()(const dariyakyu::Offset& offset) const noexcept {
        return std::hash<std::int64_t>{}(offset.value());
    }
};

template <>
struct std::hash<dariyakyu::TopicPartition> {
    std::size_t operator()(const dariyakyu::TopicPartition& tp) const noexcept {
        // Standard hash_combine. A broker's keys are dominated by one topic with
        // many partitions, so the string hash is identical across them and only
        // the integer differs — and std::hash<int> is usually the identity
        // function. Without proper mixing every partition of a topic would land
        // in adjacent buckets.
        std::size_t h = std::hash<std::string>{}(tp.topic);
        h ^= std::hash<dariyakyu::PartitionId>{}(tp.partition) + 0x9e3779b97f4a7c15ULL +
             (h << 6) + (h >> 2);
        return h;
    }
};
