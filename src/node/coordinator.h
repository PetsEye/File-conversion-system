#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>

#include "net/socket.h"
#include "proto/messages.h"

namespace mr {

class Coordinator {
public:
    struct Options {
        std::uint16_t port = 7070;
        std::filesystem::path spool;
        std::uint32_t dead_after_ms = 15000;
        int max_attempts = 3;
    };

    explicit Coordinator(Options opts);
    void run();

private:
    using Clock = std::chrono::steady_clock;

    struct Result {
        std::uint32_t index = 0;
        bool ok = false;
        std::string message;
        std::string worker;
        std::uint64_t millis = 0;
        std::filesystem::path output;
        std::uint64_t size = 0;
    };

    struct Job {
        std::uint64_t id = 0;
        std::string out_ext;
        std::filesystem::path dir;
        std::deque<Result> done;
        std::uint32_t remaining = 0;
        bool cancelled = false;
        std::condition_variable cv;
    };

    struct Task {
        std::uint64_t id = 0;
        std::shared_ptr<Job> job;
        std::uint32_t index = 0;
        std::string name;
        std::string kind;
        std::string in_ext;
        std::filesystem::path input;
        std::uint64_t size = 0;
        int attempts = 0;
    };

    struct WorkerInfo {
        std::string name;
        std::string peer;
        std::set<std::string> kinds;
        std::string current;
    };

    void handle(net::Socket conn, std::string peer);
    void serve_worker(const net::Socket& conn, const proto::RegisterWorker& reg,
                      const std::string& peer);
    void serve_client(const net::Socket& conn, const proto::SubmitJob& submit);
    void serve_status(const net::Socket& conn);

    std::shared_ptr<Task> take_task(const std::set<std::string>& kinds);
    void retry_or_fail(const std::shared_ptr<Task>& task, const std::string& why);
    void finish(Task& task, Result r);        // requires mu_
    bool any_worker_can(const std::string& kind) const;  // requires mu_

    Options opts_;
    std::mutex mu_;
    std::condition_variable task_cv_;
    std::deque<std::shared_ptr<Task>> queue_;
    std::map<std::uint32_t, WorkerInfo> workers_;
    std::uint32_t next_worker_id_ = 1;
    std::uint64_t next_job_id_ = 1;
    std::uint64_t next_task_id_ = 1;
    std::uint32_t active_jobs_ = 0;
};

}  // namespace mr
