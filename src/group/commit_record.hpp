#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "common/types.hpp"

namespace dariyakyu::group {

// What a group has read of one partition.
//
// The KEY is what compaction collapses on, so it has to identify exactly one
// consumer position and nothing else: which group, which partition of which
// topic. Two groups reading the same partition are two independent positions and
// must never share a key.
struct CommitKey {
    std::string group;
    std::string topic;
    PartitionId partition = 0;

    bool operator==(const CommitKey&) const = default;
    auto operator<=>(const CommitKey&) const = default;
};

// And where it has got to.
struct CommitValue {
    // The NEXT offset to read, not the last one processed.
    //
    // Handle 500 through 509, commit 510. Getting this wrong reprocesses or skips
    // exactly one message per restart, survives every test anyone writes by hand,
    // and shows up in production. Both are an Offset, so the type system cannot
    // help — which is why it is said at every boundary that touches one.
    Offset nextOffset{0};

    // Opaque to the broker, as everything a client attaches is. Somewhere to put
    // a deploy id or a reason, for whoever is reading the topic later.
    std::string metadata;

    std::int64_t commitTimestampMs = 0;
};

// Encoded as ordinary record bytes: the key becomes the record key, which is what
// compaction needs, and the value becomes the record value.
//
// A NULL value is a tombstone — how a group's position is forgotten, and why
// null and empty had to stay distinguishable all the way back in M1.
std::vector<std::uint8_t> encodeCommitKey(const CommitKey& key);
CommitKey                 decodeCommitKey(std::span<const std::uint8_t> bytes);

std::vector<std::uint8_t> encodeCommitValue(const CommitValue& value);
CommitValue               decodeCommitValue(std::span<const std::uint8_t> bytes);

}  // namespace dariyakyu::group
