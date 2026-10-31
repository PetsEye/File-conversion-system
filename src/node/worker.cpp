#include "node/worker.h"

#include <fcntl.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <csignal>
#include <atomic>
#include <chrono>
#include <exception>
#include <fstream>
#include <iterator>
#include <sstream>
#include <stdexcept>
#include <thread>

#include "convert/converters.h"
#include "log.h"
#include "proto/io.h"

namespace mr {

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

constexpr int kMaxSlots = 64;
std::atomic<pid_t> g_children[kMaxSlots];

void on_terminate(int sig) {
    for (auto& c : g_children)
        if (pid_t p = c.load(); p > 0) ::kill(-p, SIGKILL);
    ::_exit(128 + sig);
}

std::string last_line(const fs::path& p, std::size_t max = 300) {
    std::ifstream in(p);
    std::string line, last;
    while (std::getline(in, line))
        if (line.find_first_not_of(" \t\r") != std::string::npos) last = line;
    return last.size() > max ? last.substr(0, max) + "..." : last;
}

}  // namespace

Worker::Worker(Options opts) : opts_(std::move(opts)) {
    if (opts_.name.empty()) {
        char host[256] = {};
        ::gethostname(host, sizeof(host) - 1);
        opts_.name = host[0] ? host : "worker";
    }
    if (opts_.slots == 0) opts_.slots = std::max(1u, std::thread::hardware_concurrency() / 2);
    opts_.slots = std::min<unsigned>(opts_.slots, kMaxSlots);
    if (opts_.workdir.empty()) opts_.workdir = fs::temp_directory_path() / ("mreduce-worker-" + std::to_string(::getpid()));
}

void Worker::run() {
    kinds_ = conv::local_kinds();
    if (kinds_.empty())
        throw std::runtime_error(
            "no converters found on PATH; install some of: ffmpeg imagemagick pandoc libreoffice");
    std::string k;
    for (auto& x : kinds_) k += (k.empty() ? "" : ",") + x;
    log("worker", opts_.name, ": ", opts_.slots, " slot(s), can do [", k, "]");

    std::signal(SIGTERM, on_terminate);
    std::signal(SIGINT, on_terminate);

    std::vector<std::thread> threads;
    for (unsigned i = 0; i < opts_.slots; ++i) threads.emplace_back(&Worker::slot_loop, this, i);
    for (auto& t : threads) t.join();
}

void Worker::slot_loop(unsigned slot) {
    auto backoff = 500ms;
    for (;;) {
        try {
            session(slot);
            backoff = 500ms;
        } catch (const std::exception& e) {
            if (slot == 0) log("worker", e.what());
        }
        std::this_thread::sleep_for(backoff);
        backoff = std::min(backoff * 2, std::chrono::milliseconds(8000));
    }
}

void Worker::session(unsigned slot) {
    net::Socket conn = net::connect_tcp(opts_.coord_host, opts_.coord_port);
    net::set_recv_timeout(conn, 30000);

    std::string kinds;
    for (auto& x : kinds_) kinds += (kinds.empty() ? "" : ",") + x;
    std::string name = opts_.name + "/" + std::to_string(slot);
    if (!proto::send_msg(conn, proto::RegisterWorker{name, kinds})) return;
    if (!proto::recv_as<proto::RegisterAck>(conn)) return;
    log("worker", name, " connected to ", opts_.coord_host, ":", opts_.coord_port);

    for (;;) {
        if (!proto::send_msg(conn, proto::RequestTask{})) break;
        auto m = proto::recv_msg(conn);
        if (!m) break;
        if (std::holds_alternative<proto::NoTask>(*m)) continue;
        auto* task = std::get_if<proto::Task>(&*m);
        if (!task || !run_task(conn, *task, slot)) break;
    }
    log("worker", name, " lost coordinator");
}

bool Worker::run_task(const net::Socket& conn, const proto::Task& task, unsigned slot) {
    fs::path dir = opts_.workdir / ("slot-" + std::to_string(slot));
    fs::remove_all(dir);
    fs::create_directories(dir);
    fs::path input = dir / ("in." + task.in_ext);
    if (!net::recv_file(conn, input, task.size)) return false;

    auto start = std::chrono::steady_clock::now();
    std::string error;
    RunOutcome outcome = RunOutcome::Failed;
    auto cmd = conv::build_command(task.kind, input, task.out_ext, dir);
    if (!cmd)
        error = "no tool for " + task.kind + " on this worker";
    else
        outcome = run_process(conn, cmd->argv, dir / "log.txt", slot, error);
    if (outcome == RunOutcome::LostCoordinator) return false;

    std::error_code ec;
    std::uint64_t size = 0;
    if (outcome == RunOutcome::Ok) {
        size = fs::exists(cmd->output, ec) ? fs::file_size(cmd->output, ec) : 0;
        if (size == 0) {
            outcome = RunOutcome::Failed;
            error = "converter produced no output: " + last_line(dir / "log.txt");
        }
    }
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::steady_clock::now() - start).count();
    bool ok = outcome == RunOutcome::Ok;
    log("worker", task.in_ext, " -> ", task.out_ext, ok ? " ok in " : " failed in ", ms, "ms",
        ok ? "" : ": " + error);

    bool sent = proto::send_msg(conn, proto::TaskResult{task.task_id, ok, error, size}) &&
                (!ok || net::send_file(conn, cmd->output, size));
    fs::remove_all(dir, ec);
    return sent;
}

Worker::RunOutcome Worker::run_process(const net::Socket& conn,
                                       const std::vector<std::string>& argv,
                                       const fs::path& log_path, unsigned slot,
                                       std::string& error) {
    std::vector<char*> cargv;
    for (auto& a : argv) cargv.push_back(const_cast<char*>(a.c_str()));
    cargv.push_back(nullptr);
    std::string log_str = log_path.string();
    std::string cwd = log_path.parent_path().string();

    pid_t pid = ::fork();
    if (pid < 0) {
        error = "fork failed";
        return RunOutcome::Failed;
    }
    if (pid == 0) {
        // Child: only async-signal-safe calls until exec.
        ::setpgid(0, 0);
        int logfd = ::open(log_str.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        int nullfd = ::open("/dev/null", O_RDONLY);
        if (logfd >= 0) {
            ::dup2(logfd, 1);
            ::dup2(logfd, 2);
        }
        if (nullfd >= 0) ::dup2(nullfd, 0);
        if (::chdir(cwd.c_str()) != 0) ::_exit(126);
        ::execv(cargv[0], cargv.data());
        ::_exit(127);
    }

    ::setpgid(pid, pid);
    g_children[slot] = pid;
    struct Clear {
        unsigned slot;
        ~Clear() { g_children[slot] = 0; }
    } clear{slot};

    auto start = std::chrono::steady_clock::now();
    auto last_beat = start;
    int status = 0;
    for (;;) {
        pid_t r = ::waitpid(pid, &status, WNOHANG);
        if (r == pid) break;
        auto now = std::chrono::steady_clock::now();
        if (now - start > std::chrono::seconds(opts_.task_timeout_s)) {
            ::kill(-pid, SIGKILL);
            ::waitpid(pid, &status, 0);
            error = "timed out after " + std::to_string(opts_.task_timeout_s) + "s";
            return RunOutcome::Failed;
        }
        if (now - last_beat >= 1s) {
            if (!proto::send_msg(conn, proto::Heartbeat{})) {
                ::kill(-pid, SIGKILL);
                ::waitpid(pid, &status, 0);
                return RunOutcome::LostCoordinator;
            }
            last_beat = now;
        }
        std::this_thread::sleep_for(100ms);
    }

    if (WIFEXITED(status) && WEXITSTATUS(status) == 0) return RunOutcome::Ok;
    std::ostringstream os;
    if (WIFEXITED(status))
        os << fs::path(argv[0]).filename().string() << " exited " << WEXITSTATUS(status);
    else
        os << fs::path(argv[0]).filename().string() << " killed by signal " << WTERMSIG(status);
    if (auto t = last_line(log_path); !t.empty()) os << ": " << t;
    error = os.str();
    return RunOutcome::Failed;
}

}  // namespace mr
