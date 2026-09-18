#pragma once

#include <cstddef>
#include <cstdint>

namespace dariyakyu {

// The platform-specific syscalls, in one place.
//
// DESIGN.md's platform note: developed on macOS, so kqueue, BSD sendfile and
// F_NOCACHE rather than epoll, Linux sendfile and O_DIRECT. Keeping them behind
// one header is what keeps a Linux port small — and sendfile is the first of
// them to be needed, because the two platforms disagree about almost everything
// in its signature.
//
//   macOS: int     sendfile(int fd, int s, off_t offset, off_t *len,
//                           struct sf_hdtr *hdtr, int flags)
//          file first, socket second, length in and out through a pointer,
//          returns 0 or -1.
//   Linux: ssize_t sendfile(int out_fd, int in_fd, off_t *offset, size_t count)
//          socket first, file second, returns the count.
//
// Reversed argument order, different return convention. A call written for one
// compiles on the other and sends bytes between the wrong descriptors.

// Sends up to `length` bytes of `fileFd`, starting at `offset`, to `socketFd`,
// without the bytes entering this process at all.
//
// Returns how many were ACTUALLY sent, which may be fewer than asked for: a
// socket accepts what fits in its buffer. The caller loops. Returning a count
// rather than looping internally is deliberate — at M9 a partial send has to
// become parked state rather than a blocked thread, and this signature already
// says what to remember.
//
// A short send is not an error and neither is a signal; both come back as a
// count. Anything else throws IoError.
std::size_t sendFileRange(int socketFd, int fileFd, std::uint64_t offset, std::size_t length);

}  // namespace dariyakyu
