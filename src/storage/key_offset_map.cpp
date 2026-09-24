#include "storage/key_offset_map.hpp"

using namespace std;

namespace dariyakyu::storage {

namespace {

// FNV-1a, run twice with different offset bases to give two independent 64-bit
// halves. Not a cryptographic hash and does not need to be: nothing adversarial
// chooses these keys, and the only property that matters is that distinct keys
// land on distinct digests.
constexpr uint64_t kPrime = 0x100000001B3ull;

uint64_t fnv1a(span<const uint8_t> bytes, uint64_t basis) {
    uint64_t hash = basis;
    for (const uint8_t byte : bytes) {
        hash ^= byte;
        hash *= kPrime;
    }
    return hash;
}

}  // namespace

KeyOffsetMap::KeyOffsetMap(size_t maxEntries) : maxEntries_(maxEntries) {}

KeyOffsetMap::Digest KeyOffsetMap::digestOf(span<const uint8_t> key) {
    return {fnv1a(key, 0xCBF29CE484222325ull), fnv1a(key, 0x9E3779B97F4A7C15ull)};
}

bool KeyOffsetMap::put(span<const uint8_t> key, Offset offset) {
    const Digest digest = digestOf(key);

    const auto found = entries_.find(digest);
    if (found != entries_.end()) {
        // Always updated, even when full. Leaving a stale lower offset would make
        // the rewrite keep the older record and delete the newer one.
        if (offset > found->second) found->second = offset;
        return true;
    }

    if (entries_.size() >= maxEntries_) return false;

    entries_.emplace(digest, offset);
    return true;
}

optional<Offset> KeyOffsetMap::get(span<const uint8_t> key) const {
    const auto found = entries_.find(digestOf(key));
    if (found == entries_.end()) return nullopt;
    return found->second;
}

bool KeyOffsetMap::isLatest(span<const uint8_t> key, Offset offset) const {
    const auto found = entries_.find(digestOf(key));

    // Never mapped: this pass did not look at the key, so nothing here knows
    // whether a newer record exists. Retaining is the direction that cannot lose
    // data.
    if (found == entries_.end()) return true;

    return offset >= found->second;
}

void KeyOffsetMap::clear() { entries_.clear(); }

}  // namespace dariyakyu::storage
