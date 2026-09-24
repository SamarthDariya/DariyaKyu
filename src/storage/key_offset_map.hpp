#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <unordered_map>

#include "common/types.hpp"

namespace dariyakyu::storage {

// "What is the newest offset that wrote this key?"
//
// The one question compaction asks, and the answer to it for every key in the
// range being cleaned has to be in memory at once — so THIS, not the number of
// segments, is what bounds a pass. Kafka sizes its cleaner in megabytes of map
// for the same reason.
//
// # Why a digest rather than the key
//
// Keys are arbitrary bytes and can be large. Storing them would make the map's
// memory depend on what producers happen to write, which is not something a
// broker can size for. A 128-bit digest is a fixed 24 bytes per entry whatever
// the key is.
//
// The cost is that a digest collision makes two distinct keys look like one, and
// the newer would delete the older's record. At 128 bits that is not a risk worth
// engineering against — a partition would need on the order of 2^64 keys before a
// collision became likely, and it would run out of disk first. Kafka uses MD5 for
// exactly this, and this uses the same two-round mixing already in types.hpp
// rather than pulling in a hash library.
//
// # Why an entry budget, and why filling it is not an error
//
// The budget is the memory limit made explicit. A pass that hit it and FAILED
// would stop compacting precisely the partitions with the most keys — the ones
// that most need it. So filling up is a normal outcome: `put` reports that it
// took nothing, the caller stops mapping there, and the range that was mapped is
// the range that gets cleaned. The next pass starts where this one stopped.
class KeyOffsetMap {
public:
    explicit KeyOffsetMap(std::size_t maxEntries);

    // Records that `key` was written at `offset`, keeping the higher offset if
    // the key is already here.
    //
    // Returns false when the map is full AND this key is new. A key already in
    // the map is always updated, full or not: refusing would leave a stale, LOWER
    // offset behind, and the rewrite would then keep the older record and delete
    // the newer one — silently serving a value the producer has already replaced.
    bool put(std::span<const std::uint8_t> key, Offset offset);

    // The newest offset seen for `key`, or nothing if it was never mapped.
    std::optional<Offset> get(std::span<const std::uint8_t> key) const;

    // Whether `offset` is the newest record for `key` — the retain question, in
    // the form the rewrite actually asks it.
    //
    // A key that is not in the map is retained. That is the safe direction: an
    // unmapped key means this pass never looked at it, so nothing here knows
    // whether a newer record exists, and deleting on that basis would lose data.
    bool isLatest(std::span<const std::uint8_t> key, Offset offset) const;

    std::size_t size() const { return entries_.size(); }
    std::size_t capacity() const { return maxEntries_; }
    bool        full() const { return entries_.size() >= maxEntries_; }

    void clear();

private:
    // 128 bits, as two 64-bit halves, so it fits a standard hash map key without
    // an allocation or a custom allocator.
    struct Digest {
        std::uint64_t high = 0;
        std::uint64_t low  = 0;

        bool operator==(const Digest&) const = default;
    };

    struct DigestHash {
        std::size_t operator()(const Digest& digest) const {
            return static_cast<std::size_t>(digest.high ^ (digest.low * 0x9E3779B97F4A7C15ull));
        }
    };

    static Digest digestOf(std::span<const std::uint8_t> key);

    std::size_t                                       maxEntries_;
    std::unordered_map<Digest, Offset, DigestHash>    entries_;
};

}  // namespace dariyakyu::storage
