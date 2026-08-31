#include "storage/segment.hpp"

#include <cctype>
#include <format>
#include <stdexcept>
#include <utility>

#include "common/errors.hpp"

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

}  // namespace dariyakyu::storage
