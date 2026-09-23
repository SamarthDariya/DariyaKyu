#include "protocol/frame.hpp"

#include <unistd.h>

#include <cerrno>
#include <limits>
#include <string>

#include "common/errors.hpp"

using namespace std;

namespace dariyakyu::protocol {

namespace {

constexpr size_t kPrefixBytes = 4;

}  // namespace

bool readExactly(int fd, span<uint8_t> out) {
    size_t filled = 0;
    while (filled < out.size()) {
        const ssize_t got = ::read(fd, out.data() + filled, out.size() - filled);

        if (got < 0) {
            // A signal arriving mid-read is not a failure. Treating it as one
            // makes a broker that dies whenever anything sends it a signal.
            if (errno == EINTR) continue;

            // A reset is the peer going away, and it is treated exactly like a
            // clean close below.
            //
            // Not a nicety: a server shutting down does shutdown() then close(),
            // and whether the client sees a FIN or an RST depends on timing it
            // cannot influence. A client that threw on one and not the other
            // would crash on roughly half of all normal broker restarts.
            if (errno != ECONNRESET) throw IoError("read", "socket", errno);
        }

        if (got <= 0) {
            // End of stream. Clean only if nothing had been read yet — a peer
            // that vanished part-way through promised bytes it did not send,
            // and that is corruption whether it left politely or not.
            if (filled == 0) return false;
            throw CorruptData("frame: stream ended after " + to_string(filled) + " of " +
                              to_string(out.size()) + " byte(s)");
        }

        filled += static_cast<size_t>(got);
    }
    return true;
}

void writeAll(int fd, span<const uint8_t> bytes) {
    size_t sent = 0;
    while (sent < bytes.size()) {
        const ssize_t wrote = ::write(fd, bytes.data() + sent, bytes.size() - sent);
        if (wrote < 0) {
            if (errno == EINTR) continue;
            throw IoError("write", "socket", errno);
        }
        sent += static_cast<size_t>(wrote);
    }
}

optional<vector<uint8_t>> readFrame(int fd, size_t maxFrameBytes) {
    uint8_t prefix[kPrefixBytes];
    if (!readExactly(fd, prefix)) return nullopt;   // closed between frames

    const int32_t length = (static_cast<int32_t>(prefix[0]) << 24) |
                           (static_cast<int32_t>(prefix[1]) << 16) |
                           (static_cast<int32_t>(prefix[2]) << 8) |
                           static_cast<int32_t>(prefix[3]);

    if (length < 0)
        throw CorruptData("frame: negative length " + to_string(length));

    // Checked BEFORE allocating, which is the entire point. A frame claiming two
    // gigabytes must be refused on the strength of four bytes, not after trying
    // to make room for it.
    if (static_cast<size_t>(length) > maxFrameBytes)
        throw CorruptData("frame: length " + to_string(length) + " exceeds the maximum of " +
                          to_string(maxFrameBytes));

    vector<uint8_t> payload(static_cast<size_t>(length));

    // A zero-length frame is legal in shape but meaningless — every request
    // carries at least a header — so the caller's decoder rejects it rather than
    // this one guessing.
    if (length > 0 && !readExactly(fd, payload))
        throw CorruptData("frame: stream ended before its " + to_string(length) +
                          " byte payload");

    return payload;
}

void writeFrameLength(int fd, size_t payloadBytes) {
    if (payloadBytes > static_cast<size_t>(numeric_limits<int32_t>::max()))
        throw Error("frame: payload of " + to_string(payloadBytes) +
                    " bytes does not fit a 32-bit length");

    const auto    length = static_cast<int32_t>(payloadBytes);
    const uint8_t prefix[kPrefixBytes] = {
        static_cast<uint8_t>(length >> 24), static_cast<uint8_t>(length >> 16),
        static_cast<uint8_t>(length >> 8), static_cast<uint8_t>(length)};

    writeAll(fd, prefix);
}

void writeFrame(int fd, span<const uint8_t> payload) {
    writeFrameLength(fd, payload.size());
    writeAll(fd, payload);
}

}  // namespace dariyakyu::protocol
