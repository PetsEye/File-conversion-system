#include <sys/socket.h>

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <thread>

#include "convert/converters.h"
#include "net/socket.h"
#include "proto/messages.h"

namespace {

int failures = 0;

#define CHECK(cond)                                                                \
    do {                                                                           \
        if (!(cond)) {                                                             \
            std::fprintf(stderr, "%s:%d: CHECK(%s)\n", __FILE__, __LINE__, #cond); \
            ++failures;                                                            \
        }                                                                          \
    } while (0)

using namespace mr;
namespace fs = std::filesystem;

void test_roundtrip() {
    proto::Message msgs[] = {
        proto::RegisterWorker{"raspberrypi/0", "image,media"},
        proto::RegisterAck{7},
        proto::RequestTask{},
        proto::NoTask{},
        proto::Task{42, "image", "png", "webp", 123456789012ull},
        proto::Heartbeat{},
        proto::TaskResult{42, false, "ffmpeg exited 1", 0},
        proto::SubmitJob{"pdf", 3},
        proto::FileUpload{"report.docx", 99},
        proto::FileResult{2, true, "", "thinkpad/1", 1500, 4096},
        proto::JobDone{2, 1},
        proto::StatusRequest{},
        proto::StatusReply{"workers: 0\n"},
    };
    for (const auto& m : msgs) {
        auto decoded = proto::decode(proto::encode(m));
        CHECK(decoded && *decoded == m);
    }
}

void test_big_endian_layout() {
    std::string b = proto::encode(proto::RegisterAck{0x01020304});
    CHECK(b == std::string({2, 1, 2, 3, 4}));
}

void test_rejects_malformed() {
    CHECK(!proto::decode(""));
    CHECK(!proto::decode(std::string(1, '\x63')));
    std::string ok = proto::encode(proto::Task{1, "media", "mp4", "mp3", 2});
    for (std::size_t n = 0; n < ok.size(); ++n) CHECK(!proto::decode(ok.substr(0, n)));
    CHECK(!proto::decode(ok + "x"));
    CHECK(!proto::decode(std::string({1, 0x7f, 0, 0, 0, 'a'})));
    std::string bad_bool = proto::encode(proto::JobDone{});
    bad_bool = proto::encode(proto::TaskResult{1, true, "", 0});
    bad_bool[9] = 2;
    CHECK(!proto::decode(bad_bool));
}

void test_framing_and_files() {
    int fds[2];
    CHECK(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);
    net::Socket a(fds[0]), b(fds[1]);

    fs::path dir = fs::temp_directory_path() / "mreduce-test";
    fs::create_directories(dir);
    std::string big(3 * 1024 * 1024 + 17, '\0');
    for (std::size_t i = 0; i < big.size(); ++i) big[i] = static_cast<char>(i * 31);
    std::ofstream(dir / "src.bin", std::ios::binary) << big;

    std::thread writer([&] {
        CHECK(net::send_frame(a, "hello"));
        CHECK(net::send_file(a, dir / "src.bin", big.size()));
        CHECK(net::send_frame(a, ""));
    });
    CHECK(net::recv_frame(b) == "hello");
    CHECK(net::recv_file(b, dir / "dst.bin", big.size()));
    CHECK(net::recv_frame(b) == "");
    writer.join();

    std::ifstream in(dir / "dst.bin", std::ios::binary);
    std::string got((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    CHECK(got == big);

    a = net::Socket();
    CHECK(!net::recv_frame(b));
    fs::remove_all(dir);
}

void test_kind_routing() {
    CHECK(conv::kind_for("png", "webp") == "image");
    CHECK(conv::kind_for("gif", "png") == "image");
    CHECK(conv::kind_for("mp4", "gif") == "media");
    CHECK(conv::kind_for("mov", "mp3") == "media");
    CHECK(conv::kind_for("docx", "pdf") == "office");
    CHECK(conv::kind_for("docx", "md") == "document");
    CHECK(conv::kind_for("md", "html") == "document");
    CHECK(!conv::kind_for("png", "png"));
    CHECK(!conv::kind_for("mp3", "pdf"));
    CHECK(conv::ext_of("Photo.JPG") == "jpg");
    CHECK(conv::ext_of("noext") == "");
}

void test_parse_host_port() {
    auto [h, p] = net::parse_host_port("raspberrypi.local:7070");
    CHECK(h == "raspberrypi.local");
    CHECK(p == 7070);
    bool threw = false;
    try {
        net::parse_host_port("nohost");
    } catch (...) {
        threw = true;
    }
    CHECK(threw);
}

}  // namespace

int main() {
    test_roundtrip();
    test_big_endian_layout();
    test_rejects_malformed();
    test_framing_and_files();
    test_kind_routing();
    test_parse_host_port();
    if (failures) {
        std::fprintf(stderr, "%d check(s) failed\n", failures);
        return 1;
    }
    std::puts("all tests passed");
    return 0;
}
