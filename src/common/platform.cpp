#include "common/platform.hpp"

#include <sys/types.h>

#if defined(__APPLE__)
#include <sys/socket.h>
#include <sys/uio.h>
#elif defined(__linux__)
#include <sys/sendfile.h>
#endif

#include <cerrno>

#include "common/errors.hpp"

using namespace std;

namespace dariyakyu {

size_t sendFileRange(int socketFd, int fileFd, uint64_t offset, size_t length) {
    if (length == 0) return 0;

#if defined(__APPLE__)
    // On the way in, `sent` is how many bytes to send; on the way out, how many
    // were. It is filled in even when the call returns -1, which is what makes a
    // short send recoverable rather than a mystery.
    off_t     sent = static_cast<off_t>(length);
    const int rc   = ::sendfile(fileFd, socketFd, static_cast<off_t>(offset), &sent, nullptr, 0);

    if (rc < 0 && errno != EAGAIN && errno != EINTR)
        throw IoError("sendfile", "socket", errno);

    return static_cast<size_t>(sent);
#elif defined(__linux__)
    off_t         position = static_cast<off_t>(offset);
    const ssize_t sent     = ::sendfile(socketFd, fileFd, &position, length);

    if (sent < 0) {
        if (errno == EAGAIN || errno == EINTR) return 0;
        throw IoError("sendfile", "socket", errno);
    }

    return static_cast<size_t>(sent);
#else
#error "sendFileRange needs a platform implementation"
#endif
}

}  // namespace dariyakyu
