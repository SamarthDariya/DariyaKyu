#include "protocol/socket.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <utility>

#include "common/errors.hpp"

using namespace std;

namespace dariyakyu::protocol {

namespace {

sockaddr_in addressFor(const string& host, int32_t port) {
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port   = htons(static_cast<uint16_t>(port));

    if (::inet_pton(AF_INET, host.c_str(), &address.sin_addr) != 1)
        throw Error("socket: '" + host + "' is not an IPv4 address");

    return address;
}

}  // namespace

Socket::~Socket() {
    // Destructors must not throw, so a failing close is swallowed here. Callers
    // that need to observe it call close() explicitly.
    if (fd_ >= 0) ::close(fd_);
}

Socket::Socket(Socket&& other) noexcept : fd_(exchange(other.fd_, -1)) {}

Socket& Socket::operator=(Socket&& other) noexcept {
    if (this != &other) {
        if (fd_ >= 0) ::close(fd_);
        fd_ = exchange(other.fd_, -1);
    }
    return *this;
}

Socket Socket::listenOn(const string& host, int32_t port, int backlog) {
    Socket socket(::socket(AF_INET, SOCK_STREAM, 0));
    if (!socket.isOpen()) throw IoError("socket", host, errno);

    // Without this, restarting a broker fails for a couple of minutes while the
    // previous socket sits in TIME_WAIT — which turns every deploy into a wait.
    const int on = 1;
    if (::setsockopt(socket.fd(), SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) < 0)
        throw IoError("setsockopt", host, errno);

    const sockaddr_in address = addressFor(host, port);
    if (::bind(socket.fd(), reinterpret_cast<const sockaddr*>(&address), sizeof(address)) < 0)
        throw IoError("bind", host + ":" + to_string(port), errno);

    if (::listen(socket.fd(), backlog) < 0)
        throw IoError("listen", host + ":" + to_string(port), errno);

    return socket;
}

Socket Socket::accept() const {
    const int accepted = ::accept(fd_, nullptr, nullptr);

    if (accepted < 0) {
        // The listening socket was closed underneath us, which is how the
        // acceptor is stopped. Not an error: a closed Socket says "no more".
        if (errno == EBADF || errno == EINVAL || errno == ECONNABORTED || errno == EINTR)
            return Socket{};
        throw IoError("accept", "listening socket", errno);
    }

    // Requests are small and answers follow immediately, so Nagle's algorithm
    // only ever delays a reply waiting for more data that is not coming.
    const int on = 1;
    ::setsockopt(accepted, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));

    return Socket(accepted);
}

Socket Socket::connectTo(const string& host, int32_t port) {
    Socket socket(::socket(AF_INET, SOCK_STREAM, 0));
    if (!socket.isOpen()) throw IoError("socket", host, errno);

    const sockaddr_in address = addressFor(host, port);
    if (::connect(socket.fd(), reinterpret_cast<const sockaddr*>(&address), sizeof(address)) < 0)
        throw IoError("connect", host + ":" + to_string(port), errno);

    const int on = 1;
    ::setsockopt(socket.fd(), IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));

    return socket;
}

int32_t Socket::localPort() const {
    sockaddr_in address{};
    socklen_t   length = sizeof(address);

    if (::getsockname(fd_, reinterpret_cast<sockaddr*>(&address), &length) < 0)
        throw IoError("getsockname", "socket", errno);

    return ntohs(address.sin_port);
}

void Socket::shutdown() {
    if (fd_ >= 0) ::shutdown(fd_, SHUT_RDWR);
}

void Socket::close() {
    if (fd_ < 0) return;
    const int fd = exchange(fd_, -1);
    if (::close(fd) < 0) throw IoError("close", "socket", errno);
}

}  // namespace dariyakyu::protocol
