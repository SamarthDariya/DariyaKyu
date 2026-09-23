#pragma once

#include <cstdint>
#include <string>

namespace dariyakyu::protocol {

// RAII around a socket descriptor.
//
// In protocol/ rather than server/ because both sides need one: the broker
// accepts them and the CLI connects with them. Keeping it here means the CLI
// links only the wire format and cannot reach a request handler by accident.
//
// Move-only, like FileHandle and for the same reason: a descriptor has exactly
// one owner, and closing one twice is a bug this makes unrepresentable. The
// number can be reused the instant it is closed, so a double close does not fail
// — it closes whatever unrelated file happened to take the number, which is the
// worst kind of bug to find.
class Socket {
public:
    Socket() = default;
    explicit Socket(int fd) : fd_(fd) {}
    ~Socket();

    Socket(Socket&& other) noexcept;
    Socket& operator=(Socket&& other) noexcept;
    Socket(const Socket&)            = delete;
    Socket& operator=(const Socket&) = delete;

    // A listening socket, bound to `host` and `port`.
    //
    // Port 0 asks the OS for a free one, which localPort() then reports. Tests
    // use that: a fixed port makes a suite fail on a machine already running
    // something, and unsafe to run twice at once.
    static Socket listenOn(const std::string& host, std::int32_t port, int backlog = 128);

    // Blocks until a client arrives. Returns a closed Socket when the listening
    // socket was closed underneath it, which is how the acceptor is stopped —
    // see Acceptor.
    Socket accept() const;

    static Socket connectTo(const std::string& host, std::int32_t port);

    // What the OS actually bound to, which is the only way to learn the port
    // after asking for 0.
    std::int32_t localPort() const;

    int  fd() const { return fd_; }
    bool isOpen() const { return fd_ >= 0; }

    // Unblocks a thread sitting in read() on this socket, by telling the kernel
    // no more data will arrive. Distinct from close(): the descriptor stays
    // valid, so a thread that wakes up and touches it gets a clean end of stream
    // rather than a number that may already have been reused.
    void shutdown();

    void close();

private:
    int fd_ = -1;
};

}  // namespace dariyakyu::protocol
