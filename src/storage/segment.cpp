#include "storage/segment.hpp"

#include <format>

using namespace std;

namespace dariyakyu::storage {

namespace {

constexpr char kLogSuffix[]   = ".log";
constexpr char kIndexSuffix[] = ".index";

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

}  // namespace dariyakyu::storage
