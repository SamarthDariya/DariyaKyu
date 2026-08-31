#include "storage/offset_index.hpp"

#include <limits>
#include <string>
#include <utility>

#include "common/errors.hpp"

using namespace std;

namespace dariyakyu::storage {

namespace {

// Big-endian by hand, for the reason given in the header: a mapping overlaid as
// a native struct array would reinterpret a data directory carried between
// machines of different endianness as plausible nonsense.
uint32_t readBe32(const uint8_t* p) {
    return (static_cast<uint32_t>(p[0]) << 24) | (static_cast<uint32_t>(p[1]) << 16) |
           (static_cast<uint32_t>(p[2]) << 8) | static_cast<uint32_t>(p[3]);
}

void writeBe32(uint8_t* p, uint32_t value) {
    p[0] = static_cast<uint8_t>(value >> 24);
    p[1] = static_cast<uint8_t>(value >> 16);
    p[2] = static_cast<uint8_t>(value >> 8);
    p[3] = static_cast<uint8_t>(value);
}

// How many entries a mapped index actually holds.
//
// A trimmed index answers this with its file size, but an index whose segment
// was still active when the broker died is preallocated to its full length and
// zero-filled past the last real entry. Trusting the file size there would hand
// out a tail of {0, 0} entries as though they were data.
//
// `position == 0` is the marker, which is sound because append() reserves it:
// every real entry has a position greater than zero. Real entries strictly
// increase and the tail is all zeroes, so the predicate is false-then-true and
// one binary search finds the boundary. A trimmed index has no tail, so the
// search probes a handful of pages and reports the whole file — the alternative,
// scanning forward, would fault in every page of a ten-megabyte index at
// startup for every segment of every partition.
size_t countEntries(span<const uint8_t> bytes) {
    // A size that is not a whole number of entries means the process died
    // between two writes. The complete prefix is still perfectly good, and the
    // partial entry at the end is dropped rather than being an error — this
    // file is a rebuildable hint, not a record of anything.
    const size_t raw = bytes.size() / OffsetIndex::kEntrySize;
    if (raw == 0) return 0;

    const auto positionAt = [bytes](size_t index) {
        return readBe32(bytes.data() + index * OffsetIndex::kEntrySize + 4);
    };

    if (positionAt(raw - 1) != 0) return raw;   // full, no zero tail to find

    size_t low = 0, high = raw - 1;             // positionAt(high) == 0
    while (low < high) {
        const size_t mid = low + (high - low) / 2;
        if (positionAt(mid) == 0)
            high = mid;
        else
            low = mid + 1;
    }
    return low;
}

}  // namespace

OffsetIndex::OffsetIndex(MappedFile map, Offset baseOffset, size_t entryCount, size_t maxEntries)
    : map_(std::move(map)), baseOffset_(baseOffset), entryCount_(entryCount),
      maxEntries_(maxEntries) {}

OffsetIndex OffsetIndex::create(const filesystem::path& path, Offset baseOffset,
                                size_t maxBytes) {
    const size_t maxEntries = maxBytes / kEntrySize;
    if (maxEntries == 0)
        throw Error("OffsetIndex: maxBytes of " + to_string(maxBytes) + " for " + path.string() +
                    " leaves room for no entries at all");

    // Discard anything already here. A segment that rolled and then died before
    // its first append leaves an index describing a different segment's bytes,
    // and those entries would binary search perfectly and point at the wrong
    // records. Truncating to zero also guarantees the preallocated tail is
    // zeroes, which is what countEntries() relies on after the next crash.
    error_code ec;
    if (filesystem::exists(path, ec)) {
        filesystem::resize_file(path, 0, ec);
        if (ec) throw IoError("resize_file", path, ec.value());
    }

    return OffsetIndex(MappedFile(path, maxEntries * kEntrySize), baseOffset, 0, maxEntries);
}

OffsetIndex OffsetIndex::openSealed(const filesystem::path& path, Offset baseOffset) {
    MappedFile   map        = MappedFile::openReadOnly(path);
    const size_t entryCount = countEntries(map.bytes());
    // A sealed index never grows, so its capacity is whatever it ended up with.
    return OffsetIndex(std::move(map), baseOffset, entryCount, entryCount);
}

OffsetIndex::OffsetIndex(OffsetIndex&& other) noexcept
    : map_(std::move(other.map_)),
      baseOffset_(other.baseOffset_),
      entryCount_(other.entryCount_.load(memory_order_relaxed)),
      maxEntries_(exchange(other.maxEntries_, 0)),
      spent_(exchange(other.spent_, true)) {
    other.entryCount_.store(0, memory_order_relaxed);
}

OffsetIndex& OffsetIndex::operator=(OffsetIndex&& other) noexcept {
    if (this != &other) {
        map_        = std::move(other.map_);
        baseOffset_ = other.baseOffset_;
        entryCount_.store(other.entryCount_.load(memory_order_relaxed), memory_order_relaxed);
        maxEntries_ = exchange(other.maxEntries_, 0);
        spent_      = exchange(other.spent_, true);
        other.entryCount_.store(0, memory_order_relaxed);
    }
    return *this;
}

void OffsetIndex::requireLive() const {
    if (spent_)
        throw Error("OffsetIndex: use after flushAndTrim() of " + path().string() +
                    " — the mapping is gone; reopen the trimmed file with openSealed()");
}

OffsetIndex::Entry OffsetIndex::entryAt(size_t index) const {
    const uint8_t* at = map_.bytes().data() + index * kEntrySize;
    return Entry{readBe32(at), readBe32(at + 4)};
}

void OffsetIndex::append(Offset offset, uint32_t position) {
    requireLive();
    if (!map_.isWritable())
        throw Error("OffsetIndex: append to a read-only index of " + path().string());

    // Position zero is how countEntries() tells a real entry from preallocated
    // padding, so it cannot also be a legal entry. Nothing is lost: lookup()
    // already answers "the start of this segment" when it has nothing better,
    // so an entry pointing at position zero would carry no information anyway.
    if (position == 0)
        throw OffsetInvariantViolated("OffsetIndex: position 0 is reserved as the empty marker; "
                                      "the first byte of a segment needs no index entry");

    if (offset < baseOffset_)
        throw OffsetInvariantViolated("OffsetIndex: offset " + offset.toString() +
                                      " is below the base offset " + baseOffset_.toString() +
                                      " of " + path().string());

    const int64_t delta = offset - baseOffset_;
    if (delta > static_cast<int64_t>(numeric_limits<uint32_t>::max()))
        throw OffsetInvariantViolated("OffsetIndex: offset " + offset.toString() + " is " +
                                      to_string(delta) +
                                      " records past the base offset, beyond what a 32-bit "
                                      "relative offset holds — the segment should have rolled");

    const auto relativeOffset = static_cast<uint32_t>(delta);

    // Only the appender writes entryCount_, so it can read its own value
    // without ordering. The release below is for the readers.
    const size_t count = entryCount_.load(memory_order_relaxed);

    if (count > 0) {
        const Entry last = entryAt(count - 1);
        if (relativeOffset <= last.relativeOffset)
            throw OffsetInvariantViolated(
                "OffsetIndex: offset " + offset.toString() + " does not advance past the last "
                "indexed relative offset " + to_string(last.relativeOffset) + " in " +
                path().string());
        if (position <= last.position)
            throw OffsetInvariantViolated(
                "OffsetIndex: position " + to_string(position) +
                " does not advance past the last indexed position " + to_string(last.position) +
                " in " + path().string());
    }

    // Checked after the invariants deliberately: a caller whose bookkeeping is
    // broken should hear about it whether or not the index happens to be full.
    if (count >= maxEntries_) return;

    uint8_t* at = map_.mutableBytes().data() + count * kEntrySize;
    writeBe32(at, relativeOffset);
    writeBe32(at + 4, position);

    // Publish. The entry's bytes are complete before the count that admits they
    // exist, so a reader can never observe half of one.
    entryCount_.store(count + 1, memory_order_release);
}

OffsetIndex::Entry OffsetIndex::lookup(Offset target) const {
    requireLive();
    if (target < baseOffset_)
        throw OffsetInvariantViolated("OffsetIndex: lookup of " + target.toString() +
                                      " in a segment based at " + baseOffset_.toString() +
                                      " — Log routed this read to the wrong segment");

    const size_t count = entryCount_.load(memory_order_acquire);
    if (count == 0) return Entry{};   // scan from the start of the segment

    const int64_t delta = target - baseOffset_;
    // Saturating rather than rejecting: a target past everything the index
    // describes still wants the last entry, and Log has already bounded the
    // target by the segment's next offset.
    const auto targetRelative =
        delta > static_cast<int64_t>(numeric_limits<uint32_t>::max())
            ? numeric_limits<uint32_t>::max()
            : static_cast<uint32_t>(delta);

    if (entryAt(0).relativeOffset > targetRelative) return Entry{};

    // Greatest entry whose relative offset is <= the target.
    size_t low = 0, high = count - 1;
    while (low < high) {
        const size_t mid = low + (high - low + 1) / 2;   // rounds up, so low advances
        if (entryAt(mid).relativeOffset <= targetRelative)
            low = mid;
        else
            high = mid - 1;
    }
    return entryAt(low);
}

void OffsetIndex::flushAndTrim() {
    requireLive();
    if (!map_.isWritable())
        throw Error("OffsetIndex: flushAndTrim on a read-only index of " + path().string());

    const size_t used = usedBytes();
    // unmapAndTrim msyncs before it unmaps, so this is the final flush too.
    map_.unmapAndTrim(used);

    // The mapping is gone, so every further use is a bug rather than a
    // degraded answer. Sealing reopens the trimmed file through openSealed().
    spent_ = true;
}

}  // namespace dariyakyu::storage
