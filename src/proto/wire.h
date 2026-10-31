#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace mr::proto {

class Writer {
public:
    void put(bool v) { put_be(v ? 1 : 0, 1); }
    void put(std::uint32_t v) { put_be(v, 4); }
    void put(std::uint64_t v) { put_be(v, 8); }
    void put(std::string_view s) {
        put(static_cast<std::uint32_t>(s.size()));
        buf_.append(s);
    }
    void put_type(std::uint8_t t) { put_be(t, 1); }
    std::string take() { return std::move(buf_); }

private:
    void put_be(std::uint64_t v, int bytes) {
        for (int i = bytes - 1; i >= 0; --i)
            buf_.push_back(static_cast<char>((v >> (8 * i)) & 0xff));
    }
    std::string buf_;
};

class Reader {
public:
    explicit Reader(std::string_view data) : data_(data) {}

    bool get(bool& v) {
        std::uint64_t x;
        if (!get_be(1, x) || x > 1) return false;
        v = x == 1;
        return true;
    }
    bool get(std::uint32_t& v) {
        std::uint64_t x;
        if (!get_be(4, x)) return false;
        v = static_cast<std::uint32_t>(x);
        return true;
    }
    bool get(std::uint64_t& v) { return get_be(8, v); }
    bool get(std::string& s) {
        std::uint32_t n;
        if (!get(n) || n > remaining()) return false;
        s.assign(data_.substr(pos_, n));
        pos_ += n;
        return true;
    }
    bool get_type(std::uint8_t& t) {
        std::uint64_t x;
        if (!get_be(1, x)) return false;
        t = static_cast<std::uint8_t>(x);
        return true;
    }

    std::size_t remaining() const { return data_.size() - pos_; }
    bool done() const { return remaining() == 0; }

private:
    bool get_be(std::size_t bytes, std::uint64_t& v) {
        if (remaining() < bytes) return false;
        v = 0;
        for (std::size_t i = 0; i < bytes; ++i)
            v = (v << 8) | static_cast<std::uint8_t>(data_[pos_ + i]);
        pos_ += bytes;
        return true;
    }
    std::string_view data_;
    std::size_t pos_ = 0;
};

}  // namespace mr::proto
