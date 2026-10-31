#pragma once

#include <optional>
#include <variant>

#include "net/socket.h"
#include "proto/messages.h"

namespace mr::proto {

inline bool send_msg(const net::Socket& s, Message m) {
    return net::send_frame(s, encode(std::move(m)));
}

inline std::optional<Message> recv_msg(const net::Socket& s) {
    auto frame = net::recv_frame(s);
    if (!frame) return std::nullopt;
    return decode(*frame);
}

template <typename T>
std::optional<T> recv_as(const net::Socket& s) {
    auto m = recv_msg(s);
    if (!m) return std::nullopt;
    if (auto* t = std::get_if<T>(&*m)) return std::move(*t);
    return std::nullopt;
}

}  // namespace mr::proto
