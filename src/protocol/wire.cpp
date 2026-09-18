#include "protocol/wire.hpp"

#include <cstddef>
#include <string>

#include "common/errors.hpp"

using namespace std;

namespace dariyakyu::protocol {

namespace {

constexpr int16_t kNullLength = -1;

}  // namespace

void writeString(BufferWriter& out, string_view value) {
    if (value.size() > static_cast<size_t>(INT16_MAX))
        throw Error("wire: string of " + to_string(value.size()) +
                    " bytes does not fit a 16-bit length");

    out.writeInt16(static_cast<int16_t>(value.size()));
    out.writeBytes({reinterpret_cast<const uint8_t*>(value.data()), value.size()});
}

string readString(BufferReader& in) {
    const int16_t length = in.readInt16();

    if (length < 0) {
        if (length != kNullLength)
            throw CorruptData("wire: string length " + to_string(length));
        return {};
    }

    // readBytes throws of its own accord when the length runs past the end, so a
    // lying length needs no separate check.
    const auto bytes = in.readBytes(static_cast<size_t>(length));
    return string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
}

int32_t readElementCount(BufferReader& in, const char* what) {
    const int32_t count = in.readArrayLength();

    // -1 is the null array. Nothing in this protocol distinguishes it from an
    // empty one, so callers get a single case.
    if (count < 0) return 0;

    // A count is not a length, so readArrayLength cannot bound it against the
    // buffer — a caller could claim two billion elements in a four-byte frame.
    // Each element is at least four bytes, so anything beyond what remains could
    // not possibly be there.
    if (static_cast<size_t>(count) > in.remaining())
        throw CorruptData(string("wire: ") + what + " count " + to_string(count) +
                          " exceeds the " + to_string(in.remaining()) + " byte(s) remaining");

    return count;
}

}  // namespace dariyakyu::protocol
