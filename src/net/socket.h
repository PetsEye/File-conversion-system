#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>

namespace mr::net {

inline constexpr std::uint32_t kMaxFrameSize = 64u * 1024 * 1024;

class Socket {
public:
    Socket() = default;
    explicit Socket(int fd) : fd_(fd) {}
    ~Socket();

    Socket(Socket&& other) noexcept : fd_(other.release()) {}
    Socket& operator=(Socket&& other) noexcept;
    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;

    int fd() const { return fd_; }
    bool valid() const { return fd_ >= 0; }
    int release();
    void shutdown();

private:
    int fd_ = -1;
};

Socket listen_tcp(std::uint16_t port);
Socket connect_tcp(const std::string& host, std::uint16_t port);
Socket accept_conn(const Socket& listener, std::string* peer = nullptr);
void set_recv_timeout(const Socket& s, std::uint32_t ms);
bool peer_closed(const Socket& s);

// Frame: u32 big-endian length, then payload.
bool send_frame(const Socket& s, std::string_view payload);
std::optional<std::string> recv_frame(const Socket& s);

// Exactly `size` raw bytes between a file and the socket.
bool send_file(const Socket& s, const std::filesystem::path& path, std::uint64_t size);
bool recv_file(const Socket& s, const std::filesystem::path& path, std::uint64_t size);

std::pair<std::string, std::uint16_t> parse_host_port(const std::string& s);

}  // namespace mr::net
