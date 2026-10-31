#include "proto/messages.h"

#include "proto/wire.h"

namespace mr::proto {

namespace {

template <std::size_t I = 0>
std::optional<Message> decode_as(std::uint8_t type, Reader& r) {
    if constexpr (I == std::variant_size_v<Message>) {
        return std::nullopt;
    } else {
        using T = std::variant_alternative_t<I, Message>;
        if (static_cast<std::uint8_t>(T::type) != type) return decode_as<I + 1>(type, r);
        T m;
        bool ok = std::apply([&](auto&... f) { return (r.get(f) && ...); }, m.fields());
        if (!ok) return std::nullopt;
        return Message{std::move(m)};
    }
}

}  // namespace

std::string encode(Message m) {
    Writer w;
    std::visit(
        [&](auto& x) {
            w.put_type(static_cast<std::uint8_t>(x.type));
            std::apply([&](auto&... f) { (w.put(f), ...); }, x.fields());
        },
        m);
    return w.take();
}

std::optional<Message> decode(std::string_view payload) {
    Reader r(payload);
    std::uint8_t type;
    if (!r.get_type(type)) return std::nullopt;
    auto msg = decode_as(type, r);
    if (!msg || !r.done()) return std::nullopt;
    return msg;
}

const char* type_name(const Message& m) {
    return std::visit([](const auto& x) -> const char* { return x.kTypeName; }, m);
}

}  // namespace mr::proto
