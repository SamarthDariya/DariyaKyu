#include "storage/segment.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <format>
#include <limits>
#include <stdexcept>
#include <utility>

#include "common/errors.hpp"
#include "storage/record_batch.hpp"

using namespace std;

namespace dariyakyu::storage {

namespace {

constexpr char   kLogSuffix[]   = ".log";
constexpr char   kIndexSuffix[] = ".index";
constexpr size_t kNameDigits    = 20;

// sizeof on a string literal counts the trailing NUL, which is not part of the
// text — so ".log" is sizeof 5 and length 4.
constexpr size_t kLogSuffixLength = sizeof(kLogSuffix) - 1;

}  // namespace

string segmentBaseName(Offset baseOffset) {
    // "{:020d}" is one placeholder with a format spec after the colon:
    //   0   pad with '0' rather than the default space
    //   20  minimum field width
    //   d   render as a decimal integer
    // The width is a minimum, so a number needing more than twenty digits comes
    // out wider rather than truncated.
    //
    // .value() unwraps the strong Offset type into the int64_t inside it.
    // std::format cannot print an Offset, and that is the point of the type —
    // see the comment on Offset in common/types.hpp.
    return format("{:020d}", baseOffset.value());
}

filesystem::path segmentLogPath(const filesystem::path& dir, Offset baseOffset) {
    // operator/ on a filesystem::path appends a path component, inserting the
    // platform's separator. The parenthesised part is ordinary string
    // concatenation, producing "00000000000001073741.log" first.
    return dir / (segmentBaseName(baseOffset) + kLogSuffix);
}

filesystem::path segmentIndexPath(const filesystem::path& dir, Offset baseOffset) {
    return dir / (segmentBaseName(baseOffset) + kIndexSuffix);
}

Offset baseOffsetFromLogPath(const filesystem::path& logFile) {
    // .filename() drops the directories; .string() turns the path back into
    // characters we can index.
    const string name = logFile.filename().string();

    // Length and suffix in one check. compare(pos, count, str) compares a
    // substring against a C string and returns 0 when they match.
    if (name.size() != kNameDigits + kLogSuffixLength ||
        name.compare(kNameDigits, kLogSuffixLength, kLogSuffix) != 0)
        throw CorruptData("segment: '" + name + "' is not a segment log file name — expected " +
                          to_string(kNameDigits) + " digits followed by " + kLogSuffix);

    // isdigit() is a C function taking an int, and its behaviour is undefined
    // for values outside unsigned char. Plain `char` is signed on this platform,
    // so a byte above 127 would arrive negative — hence the cast. This is why
    // stoll() alone cannot do the validating: it skips leading whitespace,
    // accepts a leading '+' or '-', and stops at the first non-digit rather
    // than complaining about it.
    for (size_t i = 0; i < kNameDigits; ++i)
        if (isdigit(static_cast<unsigned char>(name[i])) == 0)
            throw CorruptData("segment: '" + name + "' has a non-digit where its base offset "
                              "should be");

    // Twenty digits can describe a number larger than int64 holds — the maximum
    // is 19 digits — and stoll signals that with out_of_range. That is still a
    // malformed file name, so it is translated rather than escaping as a
    // different exception type than the two checks above.
    try {
        return Offset{stoll(name.substr(0, kNameDigits))};
    } catch (const out_of_range&) {
        throw CorruptData("segment: '" + name + "' encodes an offset too large for int64");
    }
}

// --------------------------------------------------------------------------
// SegmentBase
// --------------------------------------------------------------------------

// nextOffset_ starts AT the base offset, not at zero: an empty segment's "one
// past the last offset it holds" is the offset it would hold first. That is what
// makes isEmpty() a comparison rather than a special case, and it is why
// contains() is false for everything on a fresh segment.
SegmentBase::SegmentBase(FileHandle log, OffsetIndex index, Offset baseOffset)
    : log_(std::move(log)),
      index_(std::move(index)),
      baseOffset_(baseOffset),
      nextOffset_(baseOffset) {}

FileRange SegmentBase::read(Offset offset, size_t maxBytes) const {
    if (offset < baseOffset_)
        throw OffsetInvariantViolated("segment " + logFilePath().string() + " was asked for " +
                                      offset.toString() + ", below its base offset " +
                                      baseOffset_.toString() + " — Log routed this read to the "
                                      "wrong segment");

    // ONE snapshot of the size, used for every step below. Take it twice and a
    // batch the appender adds mid-scan could be visible to one check and not the
    // next.
    const uint64_t limit = log_.size();

    // Two steps, and the split between them is the whole design.
    //
    // The index is sparse — one entry per 4 KB — so this gets us to within an
    // interval of the target, cheaply, from a memory-mapped binary search. It
    // cannot get us exactly there, and is not meant to: a dense index would cost
    // as much memory as the data.
    uint64_t position = index_.lookup(offset).position;

    // Then the log itself finishes the job, one batch header at a time. Bounded
    // by one index interval, so a page or two, normally already page-cached.
    //
    // Note this DOES read the file — the "resolves a location without doing I/O"
    // line in DESIGN.md is wrong, and cannot be right while the index is sparse.
    // What is true, and is what matters, is that it reads headers only and never
    // a record body: the payload still goes out by sendfile, untouched.
    while (auto at = batchAt(position, limit)) {
        // The FIRST batch whose last offset reaches the target is the batch that
        // contains it. `>=` rather than `==` because the target may sit in the
        // middle of a multi-record batch.
        if (at->header.lastOffset() >= offset) {
            // Bytes existing from this batch onward. batchAt has already checked
            // that the batch itself fits inside this, so totalSize <= available.
            const uint64_t available = limit - at->position;

            // Read it as two rules stacked:
            //   min(maxBytes, available)  — honour the fetch size, but do not
            //                               promise bytes that are not there
            //   max(totalSize, ...)       — never return less than one whole
            //                               batch, whatever the fetch size says
            //
            // The max cannot push the result past `available`, because
            // totalSize <= available.
            const uint64_t length = max<uint64_t>(at->totalSize, min<uint64_t>(maxBytes, available));

            return FileRange{log_.fd(), at->position, static_cast<size_t>(length)};
        }

        position += at->totalSize;
    }

    // Ran off the end without finding it: the caller is caught up with this
    // segment. Zero length, positioned at the end, and a success.
    return FileRange{log_.fd(), limit, 0};
}

optional<SegmentBase::BatchAt> SegmentBase::batchAt(uint64_t position, uint64_t limit) const {
    // Nothing left at all. Checked first so the subtraction below cannot wrap:
    // both operands are unsigned, and limit - position with position > limit
    // would produce an enormous number rather than a negative one.
    if (position >= limit) return nullopt;

    // Not even room for a header. A batch cannot be smaller than its own fixed
    // prefix, so there is nothing here to interpret.
    if (limit - position < kBatchHeaderSize) return nullopt;

    // std::array, not vector: exactly 61 bytes, on the stack, no allocation.
    // This runs once per batch in every forward scan, so it is a hot path.
    array<uint8_t, kBatchHeaderSize> headerBytes{};

    // pread under the hood — it moves no file cursor, so many threads can do
    // this on one descriptor at once. That is what makes sealed segments
    // lock-free to read.
    if (log_.readAt(position, headerBytes) < kBatchHeaderSize) return nullopt;

    BatchAt at;
    at.header    = RecordBatch::parseHeader(headerBytes);
    at.position  = position;
    at.totalSize = RecordBatch::totalSizeOf(headerBytes);

    // The header parsed, but it claims a batch longer than the bytes available.
    // On an active segment that is an append in flight; the batch simply is not
    // there yet as far as this caller is concerned.
    if (at.totalSize > limit - position) return nullopt;

    return at;
}

// --------------------------------------------------------------------------
// ActiveSegment
// --------------------------------------------------------------------------

ActiveSegment::ActiveSegment(FileHandle log, OffsetIndex index, Offset baseOffset,
                             const RollPolicy& policy)
    : SegmentBase(std::move(log), std::move(index), baseOffset), policy_(policy) {}

unique_ptr<ActiveSegment> ActiveSegment::create(const filesystem::path& dir, Offset baseOffset,
                                                const RollPolicy& policy) {
    // The two resources are acquired in named steps rather than inline in the
    // constructor call, and the order is load-bearing.
    //
    // The order of evaluation of a function's arguments is UNSPECIFIED in C++ —
    // the compiler may run them in any order. So building both inline would let
    // OffsetIndex::create run FIRST, and it truncates an existing index file to
    // zero. A create() that then failed on the .log would have destroyed a
    // perfectly good index belonging to a segment it refused to touch.
    //
    // Named locals force the sequence: claim the log first, and if that throws,
    // nothing has been modified.
    FileHandle log(segmentLogPath(dir, baseOffset), FileHandle::Mode::CreateNew);
    OffsetIndex index =
        OffsetIndex::create(segmentIndexPath(dir, baseOffset), baseOffset, policy.maxIndexBytes);

    // make_unique cannot be used here: the constructor is private, and
    // make_unique is not a member so it has no access. `new` inside a static
    // member function does.
    return unique_ptr<ActiveSegment>(
        new ActiveSegment(std::move(log), std::move(index), baseOffset, policy));
}

void ActiveSegment::append(Offset baseOffset, span<const uint8_t> batchBytes) {
    // Reads the fixed 61-byte prefix and stops. The body is never touched here:
    // the broker does not know or care what the records say, and on a compressed
    // batch it could not read them anyway.
    const BatchHeader header = RecordBatch::parseHeader(batchBytes);

    // Two different mistakes, two checks.
    //
    // First: the header must already say what the caller says it says. If Log
    // computed an offset but forgot to stamp it into the bytes, the header still
    // reads 0 while baseOffset reads the real value — caught here rather than
    // written to disk as a batch claiming to start at zero.
    if (header.baseOffset != baseOffset)
        throw OffsetInvariantViolated("segment append: batch header says " +
                                      header.baseOffset.toString() + " but the caller says " +
                                      baseOffset.toString() + " — the stamp did not happen");

    // Second: a log is contiguous. The new batch has to start exactly where the
    // last one ended, or the offsets in this file stop being a sequence and
    // every later lookup is wrong.
    const Offset expected = nextOffset();
    if (baseOffset != expected)
        throw OffsetInvariantViolated("segment append: batch starts at " + baseOffset.toString() +
                                      " but the segment ends at " + expected.toString() +
                                      " — a gap or overlap in Log's bookkeeping");

    // Where this batch is about to land. Taken BEFORE the write, because an
    // index entry has to record where the batch begins, not where it ends.
    const uint64_t position = log_.size();

    // An index entry maps an offset to a byte position, and the position is a
    // uint32 — half the entry, which doubles the entries per page. That bounds a
    // segment at 4 GiB. A silent cast would wrap a larger position into a small
    // one that binary searches perfectly and points at the wrong record, so it
    // is checked instead. shouldRoll() should have prevented this long before;
    // reaching it means the roll policy was configured past what the index can
    // describe.
    if (position > numeric_limits<uint32_t>::max())
        throw OffsetInvariantViolated("segment append: byte position " + to_string(position) +
                                      " exceeds what a 32-bit index position holds — "
                                      "maxSegmentBytes is set above 4 GiB");

    // One entry per interval of log written, not one per batch: 4 KB of records
    // share an entry, so a 1 GiB segment costs a couple of megabytes of index
    // rather than hundreds. The cost is that a lookup lands slightly BEFORE its
    // target and the caller scans forward — which is the bargain the whole
    // design is built on.
    //
    // `position > 0` keeps the segment's very first batch out of the index. Two
    // reasons, and either alone would be enough: position 0 is OffsetIndex's
    // marker for unwritten padding, and an entry pointing at the front of the
    // file tells a lookup nothing it does not already fall back to.
    if (bytesSinceIndexEntry_ >= policy_.indexIntervalBytes && position > 0) {
        // A full index drops this silently and stays correct — just sparser.
        // shouldRoll() watches for that and seals the segment.
        index_.append(baseOffset, static_cast<uint32_t>(position));
        bytesSinceIndexEntry_ = 0;
    }

    log_.append(batchBytes);
    bytesSinceIndexEntry_ += batchBytes.size();

    // relaxed on the load: only this thread ever writes largestTimestamp_, so it
    // is reading its own last value and needs no ordering to do that. The store
    // is release because readers do consume it.
    largestTimestamp_.store(max(largestTimestamp_.load(memory_order_relaxed),
                                header.maxTimestamp),
                            memory_order_release);

    // Published LAST, and with release ordering. This is the moment the batch
    // becomes visible: a reader that sees the new next offset is guaranteed to
    // see the bytes behind it, because the release store cannot be reordered
    // before the writes above it.
    //
    // lastOffset() is baseOffset + lastOffsetDelta, and lastOffsetDelta lives
    // outside the compressed region of the batch on purpose — so the segment
    // learns how many records arrived without decompressing anything.
    nextOffset_.store(header.lastOffset() + 1, memory_order_release);
}

}  // namespace dariyakyu::storage
