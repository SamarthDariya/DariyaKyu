#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "common/types.hpp"

namespace dariyakyu::protocol {

// One piece of a response: bytes we own, or a place in a file.
//
// Never empty. append() drops a zero-length piece rather than storing one, so
// every segment here has something to send — which matters because a caught-up
// fetch produces a zero-length FileRange, and a response full of nothings would
// be a loop that writes nothing per iteration.
struct ResponseSegment {
    std::vector<std::uint8_t> buffer;
    FileRange                 range;

    // A segment is a file segment exactly when it has a range, because empty
    // ranges are never stored.
    bool        isFile() const { return range.length > 0; }
    std::size_t size() const { return isFile() ? range.length : buffer.size(); }
};

// A response, as the sequence of pieces that will be written to a socket.
//
// NOT a vector<uint8_t>, and that is the central decision of this layer.
//
// A Fetch response interleaves two kinds of bytes: metadata built in memory, and
// record batches that already exist in a file. Decision 13 says those record
// bytes must never enter user space — that is what sendfile is for, and what
// Log::read returning a FileRange enforces. But sendfile can only send FILE
// bytes; it cannot also send the metadata. So a response cannot be one buffer
// and cannot be one syscall.
//
// Meanwhile the frame length has to go out FIRST, before any of it — which means
// knowing the total, including bytes not yet sent. That is affordable only
// because Log::read resolves a location rather than reading: every range's
// length is known without touching it. So totalBytes() is arithmetic.
//
// Three things fall out of this shape, and each is why it is the right one:
//
//   1. Encoders never learn about sockets. A handler builds a Response; the
//      connection decides how it reaches the wire.
//   2. A half-sent response has a small, obvious resumption state — which
//      segment, and how far into it. At M4 that is a loop; at M9, with
//      non-blocking sockets, it is exactly what a parked write must remember.
//   3. Every other API still works. A Produce or Metadata response is one buffer
//      segment and no ranges, and pays nothing for the abstraction.
//
// Kafka arrived at the same answer and calls it Send, with MultiRecordsSend for
// the multi-partition case.
class Response {
public:
    // Both drop empty pieces. A caller appending a caught-up read's range should
    // not have to check first, and neither should one appending an encoder's
    // output that happened to produce nothing.
    void append(std::vector<std::uint8_t> buffer);
    void append(FileRange range);

    // The frame length. Summed on demand rather than tracked, for the same
    // reason Log::totalSizeBytes is: a running counter is a second source of
    // truth, and the sum is over a handful of segments.
    std::size_t totalBytes() const;

    bool empty() const { return segments_.empty(); }

    std::span<const ResponseSegment> segments() const { return segments_; }

    // Reads every file range and returns the response as one flat buffer.
    //
    // For TESTS and for inspecting a response, never for the broker's write
    // path: it does precisely the copying the segment list exists to avoid. The
    // client does not need it either — a client receives a flat frame off a
    // socket and never sees a Response at all.
    std::vector<std::uint8_t> materialise() const;

private:
    std::vector<ResponseSegment> segments_;
};

// Writes a response to a socket: the frame length, then each segment in order —
// write() for the buffers, sendfile() for the ranges.
//
// The payload never enters this process. A 900 KB fetch costs four bytes of
// prefix, a few dozen bytes of metadata, and one sendfile that the kernel
// services from its own page cache.
//
// Both loop over short writes, because a socket accepts what fits in its buffer
// and nothing more. Nothing here is non-blocking yet, so a full buffer blocks
// this thread — which is M4's whole threading model, and M9's problem.
void writeResponse(int socketFd, const Response& response);

}  // namespace dariyakyu::protocol
