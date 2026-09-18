#include "protocol/response.hpp"

#include <unistd.h>

#include <cerrno>
#include <string>
#include <utility>

#include "common/errors.hpp"
#include "common/platform.hpp"
#include "protocol/frame.hpp"

using namespace std;

namespace dariyakyu::protocol {

void Response::append(vector<uint8_t> buffer) {
    if (buffer.empty()) return;

    // Coalesced into the previous segment when that is also a buffer.
    //
    // Adjacent buffers are adjacent bytes on the wire, so keeping them apart
    // buys nothing and costs a write() each. Every response has at least two —
    // the correlation id that dispatch writes, and the handler's first field —
    // so without this the cheapest possible reply is two syscalls.
    //
    // The copy is bounded by design: only metadata is ever a buffer segment.
    // Record bytes are file ranges and are never touched here.
    if (!segments_.empty() && !segments_.back().isFile()) {
        auto& previous = segments_.back().buffer;
        previous.insert(previous.end(), buffer.begin(), buffer.end());
        return;
    }

    segments_.push_back(ResponseSegment{std::move(buffer), FileRange{}});
}

void Response::append(FileRange range) {
    // A caught-up fetch produces exactly this, and it is the common case rather
    // than an edge one — so callers append unconditionally and this drops it.
    if (range.length == 0) return;
    segments_.push_back(ResponseSegment{{}, range});
}

size_t Response::totalBytes() const {
    size_t total = 0;
    for (const auto& segment : segments_) total += segment.size();
    return total;
}

vector<uint8_t> Response::materialise() const {
    vector<uint8_t> flat;
    flat.reserve(totalBytes());

    for (const auto& segment : segments_) {
        if (!segment.isFile()) {
            flat.insert(flat.end(), segment.buffer.begin(), segment.buffer.end());
            continue;
        }

        const size_t before = flat.size();
        flat.resize(before + segment.range.length);

        // pread rather than read: it moves no file cursor, so this cannot
        // disturb anything else reading the same descriptor.
        const ssize_t got = ::pread(segment.range.fd, flat.data() + before,
                                    segment.range.length,
                                    static_cast<off_t>(segment.range.position));
        if (got < 0) throw IoError("pread", "response segment", errno);
        if (static_cast<size_t>(got) != segment.range.length)
            throw CorruptData("response: a segment promised " +
                              to_string(segment.range.length) + " byte(s) but the file had " +
                              to_string(got));
    }

    return flat;
}

void writeResponse(int socketFd, const Response& response) {
    // First, and computed by arithmetic over the segments — including the file
    // bytes, which is the only reason a length-prefixed frame can carry a
    // sendfile payload at all.
    writeFrameLength(socketFd, response.totalBytes());

    for (const auto& segment : response.segments()) {
        if (!segment.isFile()) {
            writeAll(socketFd, segment.buffer);
            continue;
        }

        // sendFileRange reports what it managed rather than looping internally,
        // so the loop lives here — where at M9 it becomes parked state instead.
        size_t sent = 0;
        while (sent < segment.range.length) {
            sent += sendFileRange(socketFd, segment.range.fd, segment.range.position + sent,
                                  segment.range.length - sent);
        }
    }
}

}  // namespace dariyakyu::protocol
