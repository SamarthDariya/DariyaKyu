#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

namespace dariyakyu::protocol {

// Every request and response is one length-prefixed frame:
//
//   frameLength int32   bytes that follow
//   [ payload ]
//
// A length rather than a delimiter, because record bytes are arbitrary and any
// delimiter would need escaping — which means touching the payload, the one
// thing this design refuses to do.
//
// The default ceiling is generous but finite. Without one, a client sending
// 0x7FFFFFFF makes the broker try to allocate two gigabytes before it has read a
// single byte of the request, which is a one-packet denial of service.
inline constexpr std::size_t kMaxFrameBytes = 100u << 20;   // 100 MiB

// Reads one whole frame's payload.
//
// Returns nothing on a CLEAN end of stream — the peer closed between frames,
// which is how a connection normally ends and is not an error. An end of stream
// part-way through a frame is different: bytes were promised and not delivered,
// so that throws CorruptData.
//
// Loops over short reads. A socket read returns what has arrived, not what was
// asked for, so a single read() is only correct by luck on small frames and
// silently truncates large ones.
std::optional<std::vector<std::uint8_t>> readFrame(int fd, std::size_t maxFrameBytes);

// Writes the length prefix and the payload.
void writeFrame(int fd, std::span<const std::uint8_t> payload);

// Just the prefix. A Response cannot use writeFrame: its payload is not one
// buffer, and most of it never passes through this process.
void writeFrameLength(int fd, std::size_t payloadBytes);

// Fills `out` completely, or reports a clean end of stream before any byte was
// read. Shared by the two above and by the CLI.
//
// Retries on EINTR: a signal arriving mid-read is not a failure, and treating it
// as one makes a broker that dies whenever anything sends it a signal.
bool readExactly(int fd, std::span<std::uint8_t> out);

// Writes every byte, looping over short writes for the same reason.
void writeAll(int fd, std::span<const std::uint8_t> bytes);

}  // namespace dariyakyu::protocol
