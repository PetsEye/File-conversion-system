#include "net/socket.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <vector>
#include <stdexcept>

namespace mr::net {

namespace {

[[noreturn]] void throw_errno(const std::string& what) {
    throw std::runtime_error(what + ": " + std::strerror(errno));
}

bool write_all(int fd, const char* data, std::size_t n) {
    while (n > 0) {
        ssize_t w = ::send(fd, data, n, 0);
        if (w < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        data += w;
        n -= static_cast<std::size_t>(w);
    }
    return true;
}

bool read_all(int fd, char* data, std::size_t n) {
    while (n > 0) {
        ssize_t r = ::recv(fd, data, n, 0);
        if (r < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (r == 0) return false;
        data += r;
        n -= static_cast<std::size_t>(r);
    }
    return true;
}

// Without this, converter child processes inherit our sockets and keep them open after we die.
int cloexec(int fd) {
    if (fd >= 0) ::fcntl(fd, F_SETFD, FD_CLOEXEC);
    return fd;
}

void set_nodelay(int fd) {
    int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
}

}  // namespace

Socket::~Socket() {
    if (fd_ >= 0) ::close(fd_);
}

Socket& Socket::operator=(Socket&& other) noexcept {
    if (this != &other) {
        if (fd_ >= 0) ::close(fd_);
        fd_ = other.release();
    }
    return *this;
}

int Socket::release() {
    int fd = fd_;
    fd_ = -1;
    return fd;
}

void Socket::shutdown() {
    if (fd_ >= 0) ::shutdown(fd_, SHUT_RDWR);
}

Socket listen_tcp(std::uint16_t port) {
    Socket s(cloexec(::socket(AF_INET, SOCK_STREAM, 0)));
    if (!s.valid()) throw_errno("socket");

    int one = 1;
    ::setsockopt(s.fd(), SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);
    if (::bind(s.fd(), reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0)
        throw_errno("bind port " + std::to_string(port));
    if (::listen(s.fd(), 64) < 0) throw_errno("listen");
    return s;
}

Socket connect_tcp(const std::string& host, std::uint16_t port) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* res = nullptr;
    std::string port_str = std::to_string(port);
    if (int rc = ::getaddrinfo(host.c_str(), port_str.c_str(), &hints, &res); rc != 0)
        throw std::runtime_error("resolve " + host + ": " + ::gai_strerror(rc));

    std::string last_err = "no addresses";
    for (addrinfo* ai = res; ai; ai = ai->ai_next) {
        Socket s(cloexec(::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol)));
        if (!s.valid()) continue;
        if (::connect(s.fd(), ai->ai_addr, ai->ai_addrlen) == 0) {
            ::freeaddrinfo(res);
            set_nodelay(s.fd());
            return s;
        }
        last_err = std::strerror(errno);
    }
    ::freeaddrinfo(res);
    throw std::runtime_error("connect " + host + ":" + port_str + ": " + last_err);
}

Socket accept_conn(const Socket& listener, std::string* peer) {
    sockaddr_storage addr{};
    socklen_t len = sizeof(addr);
    int fd;
    do {
        fd = ::accept(listener.fd(), reinterpret_cast<sockaddr*>(&addr), &len);
    } while (fd < 0 && errno == EINTR);
    if (fd < 0) return Socket();

    cloexec(fd);
    set_nodelay(fd);
    if (peer) {
        char host[NI_MAXHOST], serv[NI_MAXSERV];
        if (::getnameinfo(reinterpret_cast<sockaddr*>(&addr), len, host, sizeof(host),
                          serv, sizeof(serv), NI_NUMERICHOST | NI_NUMERICSERV) == 0)
            *peer = std::string(host) + ":" + serv;
    }
    return Socket(fd);
}

void set_recv_timeout(const Socket& s, std::uint32_t ms) {
    timeval tv{};
    tv.tv_sec = ms / 1000;
    tv.tv_usec = static_cast<decltype(tv.tv_usec)>((ms % 1000) * 1000);
    ::setsockopt(s.fd(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
}

bool peer_closed(const Socket& s) {
    char c;
    ssize_t r = ::recv(s.fd(), &c, 1, MSG_PEEK | MSG_DONTWAIT);
    return r == 0 || (r < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR);
}

bool send_frame(const Socket& s, std::string_view payload) {
    if (payload.size() > kMaxFrameSize) return false;
    std::uint32_t len = htonl(static_cast<std::uint32_t>(payload.size()));
    std::string buf(sizeof(len) + payload.size(), '\0');
    std::memcpy(buf.data(), &len, sizeof(len));
    std::memcpy(buf.data() + sizeof(len), payload.data(), payload.size());
    return write_all(s.fd(), buf.data(), buf.size());
}

std::optional<std::string> recv_frame(const Socket& s) {
    std::uint32_t len_be;
    if (!read_all(s.fd(), reinterpret_cast<char*>(&len_be), sizeof(len_be)))
        return std::nullopt;
    std::uint32_t len = ntohl(len_be);
    if (len > kMaxFrameSize) return std::nullopt;
    std::string payload(len, '\0');
    if (!read_all(s.fd(), payload.data(), len)) return std::nullopt;
    return payload;
}

bool send_file(const Socket& s, const std::filesystem::path& path, std::uint64_t size) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return false;
    std::vector<char> buf(256 * 1024);
    while (size > 0) {
        auto n = static_cast<std::size_t>(std::min<std::uint64_t>(size, buf.size()));
        if (!in.read(buf.data(), static_cast<std::streamsize>(n))) return false;
        if (!write_all(s.fd(), buf.data(), n)) return false;
        size -= n;
    }
    return true;
}

bool recv_file(const Socket& s, const std::filesystem::path& path, std::uint64_t size) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) return false;
    std::vector<char> buf(256 * 1024);
    while (size > 0) {
        auto n = static_cast<std::size_t>(std::min<std::uint64_t>(size, buf.size()));
        if (!read_all(s.fd(), buf.data(), n)) return false;
        if (!out.write(buf.data(), static_cast<std::streamsize>(n))) return false;
        size -= n;
    }
    return static_cast<bool>(out.flush());
}

std::pair<std::string, std::uint16_t> parse_host_port(const std::string& s) {
    auto colon = s.rfind(':');
    if (colon == std::string::npos || colon == 0 || colon + 1 == s.size())
        throw std::invalid_argument("expected host:port, got '" + s + "'");
    unsigned long port = std::stoul(s.substr(colon + 1));
    if (port == 0 || port > 65535) throw std::invalid_argument("bad port in '" + s + "'");
    return {s.substr(0, colon), static_cast<std::uint16_t>(port)};
}

}  // namespace mr::net
