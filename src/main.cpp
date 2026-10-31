#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <string>
#include <vector>

#include "net/socket.h"
#include "node/coordinator.h"
#include "node/worker.h"
#include "proto/io.h"

namespace fs = std::filesystem;

namespace {

void usage() {
    std::fprintf(stderr,
                 "usage:\n"
                 "  mrnode coordinator [--port N] [--spool DIR]\n"
                 "  mrnode worker [--coordinator HOST:PORT] [--name NAME] [--slots N]\n"
                 "                [--task-timeout SECONDS]\n"
                 "  mrnode convert [--coordinator HOST:PORT] --to EXT [--out DIR] FILE...\n"
                 "  mrnode status [--coordinator HOST:PORT]\n"
                 "\n"
                 "HOST:PORT defaults to $MREDUCE_COORDINATOR, then localhost:7070.\n");
}

std::string default_coordinator() {
    const char* env = std::getenv("MREDUCE_COORDINATOR");
    return env && *env ? env : "localhost:7070";
}

fs::path unique_path(const fs::path& dir, const std::string& stem, const std::string& ext) {
    fs::path p = dir / (stem + "." + ext);
    for (int i = 1; fs::exists(p); ++i) p = dir / (stem + " (" + std::to_string(i) + ")." + ext);
    return p;
}

int convert(const std::string& coord, const std::string& to, const fs::path& out_dir,
            const std::vector<fs::path>& files) {
    std::vector<std::uint64_t> sizes;
    for (auto& f : files) {
        if (!fs::is_regular_file(f)) {
            std::fprintf(stderr, "not a file: %s\n", f.c_str());
            return 2;
        }
        sizes.push_back(fs::file_size(f));
    }
    fs::create_directories(out_dir);
    std::string ext = to;
    if (!ext.empty() && ext[0] == '.') ext.erase(0, 1);

    auto [host, port] = mr::net::parse_host_port(coord);
    auto conn = mr::net::connect_tcp(host, port);
    auto n = static_cast<std::uint32_t>(files.size());
    if (!mr::proto::send_msg(conn, mr::proto::SubmitJob{ext, n})) return 1;
    for (std::size_t i = 0; i < files.size(); ++i) {
        std::printf("uploading %s\n", files[i].filename().c_str());
        if (!mr::proto::send_msg(conn, mr::proto::FileUpload{files[i].filename().string(), sizes[i]}) ||
            !mr::net::send_file(conn, files[i], sizes[i])) {
            std::fprintf(stderr, "lost coordinator during upload\n");
            return 1;
        }
    }

    for (std::uint32_t done = 0;;) {
        auto m = mr::proto::recv_msg(conn);
        if (!m) {
            std::fprintf(stderr, "lost coordinator\n");
            return 1;
        }
        if (auto* jd = std::get_if<mr::proto::JobDone>(&*m)) {
            std::printf("done: %u converted, %u failed\n", jd->succeeded, jd->failed);
            return jd->failed ? 1 : 0;
        }
        auto* r = std::get_if<mr::proto::FileResult>(&*m);
        if (!r || r->index >= files.size()) {
            std::fprintf(stderr, "protocol error\n");
            return 1;
        }
        const fs::path& src = files[r->index];
        ++done;
        if (!r->ok) {
            std::printf("[%u/%u] %s FAILED: %s\n", done, n, src.filename().c_str(), r->message.c_str());
            continue;
        }
        fs::path dest = unique_path(out_dir, src.stem().string(), ext);
        if (!mr::net::recv_file(conn, dest, r->size)) {
            std::fprintf(stderr, "lost coordinator while downloading %s\n", dest.c_str());
            return 1;
        }
        std::printf("[%u/%u] %s -> %s  (%s, %.1fs)\n", done, n, src.filename().c_str(),
                    dest.c_str(), r->worker.c_str(), static_cast<double>(r->millis) / 1000.0);
    }
}

int status(const std::string& coord) {
    auto [host, port] = mr::net::parse_host_port(coord);
    auto conn = mr::net::connect_tcp(host, port);
    if (!mr::proto::send_msg(conn, mr::proto::StatusRequest{})) return 1;
    auto reply = mr::proto::recv_as<mr::proto::StatusReply>(conn);
    if (!reply) return 1;
    std::fputs(reply->text.c_str(), stdout);
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    std::signal(SIGPIPE, SIG_IGN);
    if (argc < 2) {
        usage();
        return 2;
    }
    std::string cmd = argv[1];

    try {
        auto next = [&](int& i) -> std::string {
            if (i + 1 >= argc) throw std::invalid_argument(std::string(argv[i]) + " needs a value");
            return argv[++i];
        };
        std::string coord = default_coordinator();

        if (cmd == "coordinator") {
            mr::Coordinator::Options o;
            for (int i = 2; i < argc; ++i) {
                std::string a = argv[i];
                if (a == "--port") o.port = static_cast<std::uint16_t>(std::stoul(next(i)));
                else if (a == "--spool") o.spool = next(i);
                else throw std::invalid_argument("unknown flag " + a);
            }
            mr::Coordinator(o).run();
        } else if (cmd == "worker") {
            mr::Worker::Options o;
            for (int i = 2; i < argc; ++i) {
                std::string a = argv[i];
                if (a == "--coordinator") coord = next(i);
                else if (a == "--name") o.name = next(i);
                else if (a == "--slots") o.slots = std::stoul(next(i));
                else if (a == "--task-timeout") o.task_timeout_s = std::stoul(next(i));
                else throw std::invalid_argument("unknown flag " + a);
            }
            std::tie(o.coord_host, o.coord_port) = mr::net::parse_host_port(coord);
            mr::Worker(o).run();
        } else if (cmd == "convert") {
            std::string to;
            fs::path out = ".";
            std::vector<fs::path> files;
            for (int i = 2; i < argc; ++i) {
                std::string a = argv[i];
                if (a == "--coordinator") coord = next(i);
                else if (a == "--to") to = next(i);
                else if (a == "--out") out = next(i);
                else if (a.rfind("--", 0) == 0) throw std::invalid_argument("unknown flag " + a);
                else files.emplace_back(a);
            }
            if (to.empty() || files.empty()) {
                usage();
                return 2;
            }
            return convert(coord, to, out, files);
        } else if (cmd == "status") {
            for (int i = 2; i < argc; ++i) {
                std::string a = argv[i];
                if (a == "--coordinator") coord = next(i);
                else throw std::invalid_argument("unknown flag " + a);
            }
            return status(coord);
        } else {
            usage();
            return 2;
        }
    } catch (const std::exception& e) {
        std::fprintf(stderr, "mrnode: %s\n", e.what());
        return 1;
    }
    return 0;
}
