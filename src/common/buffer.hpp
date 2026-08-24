#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace dariyakyu {

// Sequential readers and writers over a byte buffer.
//
// Fixed-width integers are big-endian, matching Kafka and network byte order.
// Endianness is done by hand rather than by memcpy-ing native integers: it must
// not depend on the host, and a file written on one machine has to be readable
// on another.
//
// BufferReader borrows. readBytes() returns a span into the caller's buffer, so
// a 1 MB batch costs nothing to describe. The buffer must outlive every span
// taken from it — that is the same lifetime rule the whole read path follows.
class BufferReader {
public:
    explicit BufferReader(std::span<const std::uint8_t> bytes) : bytes_(bytes) {}

    std::int8_t   readInt8();
    std::int16_t  readInt16();
    std::int32_t  readInt32();
    std::int64_t  readInt64();
    std::uint32_t readUint32();

    std::int32_t readVarint();
    std::int64_t readVarlong();

    // A view into the underlying buffer. No copy.
    std::span<const std::uint8_t> readBytes(std::size_t count);

    // Reads an int32 element count and validates it. Kafka encodes a null array
    // as -1, which is why the return type is signed and -1 is permitted.
    //
    // Deliberately not a readArray<T>(fn) template: the caller loops, so the
    // loop body is visible at the call site rather than hidden behind a
    // callback. Used by the protocol layer, where nearly every request carries
    // arrays of topics carrying arrays of partitions.
    std::int32_t readArrayLength();

    void skip(std::size_t count);

    std::size_t position() const { return position_; }
    std::size_t remaining() const { return bytes_.size() - position_; }
    bool        empty() const { return remaining() == 0; }

private:
    // Throws CorruptData when the buffer ends early. A short buffer means a torn
    // record on disk or a malformed request on the wire — in both cases the data
    // is wrong, not the caller.
    void require(std::size_t count) const;

    std::span<const std::uint8_t> bytes_;
    std::size_t                   position_ = 0;
};

class BufferWriter {
public:
    BufferWriter() = default;
    explicit BufferWriter(std::size_t reserveBytes) { bytes_.reserve(reserveBytes); }

    void writeInt8(std::int8_t value);
    void writeInt16(std::int16_t value);
    void writeInt32(std::int32_t value);
    void writeInt64(std::int64_t value);
    void writeUint32(std::uint32_t value);

    void writeVarint(std::int32_t value);
    void writeVarlong(std::int64_t value);

    void writeBytes(std::span<const std::uint8_t> value);

    // Overwrite an already-written field. A record batch cannot know its own
    // length or checksum until its body exists, so those two fields are written
    // as placeholders and patched afterwards. Same mechanism a response uses for
    // its length prefix.
    void patchInt32(std::size_t at, std::int32_t value);
    void patchUint32(std::size_t at, std::uint32_t value);

    std::size_t position() const { return bytes_.size(); }

    // Read what has been written so far, in place. build() uses this to checksum
    // its own output without copying it.
    std::span<const std::uint8_t> view() const;

    // Hands the buffer to the caller. This ENDS the writer's life: every span
    // previously obtained from view() now belongs to the returned vector and
    // dies with it, so continuing to use this writer would be reasoning about
    // two owners of one allocation.
    //
    // Rather than document that and hope, the writer marks itself spent — any
    // subsequent write, patch, or view() throws instead of quietly working.
    // Same class of mistake as decoding a temporary buffer, caught the same way.
    std::vector<std::uint8_t> take();

    bool isSpent() const { return taken_; }

private:
    void requireLive() const;
    void requirePatchRange(std::size_t at, std::size_t count) const;

    std::vector<std::uint8_t> bytes_;
    bool                      taken_ = false;
};

}  // namespace dariyakyu
