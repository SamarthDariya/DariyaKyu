#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "common/buffer.hpp"

namespace dariyakyu::protocol {

// The two encodings every API shares.
//
// Strings are length-prefixed rather than NUL-terminated: topic names come from
// clients, and a length says exactly how many bytes to read without trusting the
// content to contain a terminator.
//
// A length of -1 is Kafka's null marker and decodes to an empty string. Unlike a
// record key, where null and empty are different facts and collapsing them would
// delete data, no string in this protocol carries meaning by being absent rather
// than empty — so the two are treated alike and a decoder never has to ask.
void        writeString(BufferWriter& out, std::string_view value);
std::string readString(BufferReader& in);

// Array counts go through BufferReader::readArrayLength, which was written at M1
// with a comment saying it is for this layer: it validates the count and permits
// -1 for a null array. Callers loop, so the loop body stays visible at the call
// site rather than hidden behind a callback.
//
// A null array and an empty one both mean "nothing here" to every API in this
// protocol, so this normalises -1 to 0 and callers get one case instead of two.
std::int32_t readElementCount(BufferReader& in, const char* what);

}  // namespace dariyakyu::protocol
