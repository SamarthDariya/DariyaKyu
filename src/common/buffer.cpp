#include "common/buffer.hpp"

#include <string>

#include "common/errors.hpp"
#include "common/varint.hpp"

using namespace std;

namespace dariyakyu {

// --------------------------------------------------------------------------
// BufferReader
// --------------------------------------------------------------------------

void BufferReader::require(size_t count) const {
    if (remaining() < count)
        throw CorruptData("buffer: wanted " + to_string(count) + " byte(s) at position " +
                          to_string(position_) + " but only " + to_string(remaining()) +
                          " remain");
}

int8_t BufferReader::readInt8() {
    require(1);
    return static_cast<int8_t>(bytes_[position_++]);
}

int16_t BufferReader::readInt16() {
    require(2);
    const auto high = static_cast<uint16_t>(bytes_[position_]);
    const auto low  = static_cast<uint16_t>(bytes_[position_ + 1]);
    position_ += 2;
    return static_cast<int16_t>(static_cast<uint16_t>(high << 8) | low);
}

int32_t BufferReader::readInt32() {
    return static_cast<int32_t>(readUint32());
}

uint32_t BufferReader::readUint32() {
    require(4);
    uint32_t value = 0;
    for (size_t i = 0; i < 4; ++i) value = (value << 8) | bytes_[position_ + i];
    position_ += 4;
    return value;
}

int64_t BufferReader::readInt64() {
    require(8);
    uint64_t value = 0;
    for (size_t i = 0; i < 8; ++i) value = (value << 8) | bytes_[position_ + i];
    position_ += 8;
    return static_cast<int64_t>(value);
}

int32_t BufferReader::readVarint() {
    int32_t    value = 0;
    const auto read  = decodeVarint(bytes_.subspan(position_), value);
    position_ += read;
    return value;
}

int64_t BufferReader::readVarlong() {
    int64_t    value = 0;
    const auto read  = decodeVarlong(bytes_.subspan(position_), value);
    position_ += read;
    return value;
}

span<const uint8_t> BufferReader::readBytes(size_t count) {
    require(count);
    const auto view = bytes_.subspan(position_, count);
    position_ += count;
    return view;
}

int32_t BufferReader::readArrayLength() {
    const int32_t length = readInt32();
    // -1 is the null array; anything below that is damaged input, and letting it
    // through would drive a loop counter or a reserve() with a nonsense value.
    if (length < -1)
        throw CorruptData("buffer: array length " + to_string(length) + " at position " +
                          to_string(position_ - 4) + " is below the null marker");
    return length;
}

void BufferReader::skip(size_t count) {
    require(count);
    position_ += count;
}

// --------------------------------------------------------------------------
// BufferWriter
// --------------------------------------------------------------------------

void BufferWriter::requireLive() const {
    if (taken_)
        throw Error("buffer: writer used after take() handed its bytes to someone else");
}

span<const uint8_t> BufferWriter::view() const {
    requireLive();
    return bytes_;
}

vector<uint8_t> BufferWriter::take() {
    requireLive();
    taken_ = true;
    return std::move(bytes_);
}

void BufferWriter::writeInt8(int8_t value) {
    requireLive();
    bytes_.push_back(static_cast<uint8_t>(value));
}

void BufferWriter::writeInt16(int16_t value) {
    requireLive();
    const auto raw = static_cast<uint16_t>(value);
    bytes_.push_back(static_cast<uint8_t>(raw >> 8));
    bytes_.push_back(static_cast<uint8_t>(raw));
}

void BufferWriter::writeInt32(int32_t value) {
    writeUint32(static_cast<uint32_t>(value));
}

void BufferWriter::writeUint32(uint32_t value) {
    requireLive();
    bytes_.push_back(static_cast<uint8_t>(value >> 24));
    bytes_.push_back(static_cast<uint8_t>(value >> 16));
    bytes_.push_back(static_cast<uint8_t>(value >> 8));
    bytes_.push_back(static_cast<uint8_t>(value));
}

void BufferWriter::writeInt64(int64_t value) {
    requireLive();
    const auto raw = static_cast<uint64_t>(value);
    for (int shift = 56; shift >= 0; shift -= 8)
        bytes_.push_back(static_cast<uint8_t>(raw >> shift));
}

void BufferWriter::writeVarint(int32_t value) {
    requireLive();
    uint8_t      scratch[kMaxVarintBytes];
    const size_t written = encodeVarint(value, scratch);
    bytes_.insert(bytes_.end(), scratch, scratch + written);
}

void BufferWriter::writeVarlong(int64_t value) {
    requireLive();
    uint8_t      scratch[kMaxVarlongBytes];
    const size_t written = encodeVarlong(value, scratch);
    bytes_.insert(bytes_.end(), scratch, scratch + written);
}

void BufferWriter::writeBytes(span<const uint8_t> value) {
    requireLive();
    bytes_.insert(bytes_.end(), value.begin(), value.end());
}

void BufferWriter::requirePatchRange(size_t at, size_t count) const {
    requireLive();
    if (at + count > bytes_.size())
        throw Error("buffer: patch of " + to_string(count) + " byte(s) at " + to_string(at) +
                    " runs past the end of a " + to_string(bytes_.size()) + " byte buffer");
}

void BufferWriter::patchInt32(size_t at, int32_t value) {
    patchUint32(at, static_cast<uint32_t>(value));
}

void BufferWriter::patchUint32(size_t at, uint32_t value) {
    requirePatchRange(at, 4);
    bytes_[at]     = static_cast<uint8_t>(value >> 24);
    bytes_[at + 1] = static_cast<uint8_t>(value >> 16);
    bytes_[at + 2] = static_cast<uint8_t>(value >> 8);
    bytes_[at + 3] = static_cast<uint8_t>(value);
}

}  // namespace dariyakyu
