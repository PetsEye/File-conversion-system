#include "node/coordinator.h"

#include <cctype>
#include <sstream>
#include <thread>

#include "convert/converters.h"
#include "log.h"
#include "proto/io.h"

namespace mr {

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

std::set<std::string> split_kinds(const std::string& s) {
    std::set<std::string> out;
    std::stringstream ss(s);
    std::string k;
    while (std::getline(ss, k, ','))
        if (!k.empty()) out.insert(k);
    return out;
}

bool safe_ext(const std::string& e) {
    if (e.empty() || e.size() > 10) return false;
    for (char c : e)
        if (!std::isalnum(static_cast<unsigned char>(c))) return false;
    return true;
}

}  // namespace

Coordinator::Coordinator(Options opts) : opts_(std::move(opts)) {
    if (opts_.spool.empty()) opts_.spool = fs::temp_directory_path() / "mreduce-coordinator";
}

void Coordinator::run() {
    fs::remove_all(opts_.spool);
    fs::create_directories(opts_.spool);
    net::Socket listener = net::listen_tcp(opts_.port);
    log("coord", "listening on :", opts_.port, ", spool ", opts_.spool.string());

    for (;;) {
        std::string peer;
        net::Socket conn = net::accept_conn(listener, &peer);
        if (!conn.valid()) continue;
        std::thread(&Coordinator::handle, this, std::move(conn), std::move(peer)).detach();
    }
}

void Coordinator::handle(net::Socket conn, std::string peer) {
    net::set_recv_timeout(conn, opts_.dead_after_ms);
    auto first = proto::recv_msg(conn);
    if (!first) return;
    if (auto* reg = std::get_if<proto::RegisterWorker>(&*first))
        serve_worker(conn, *reg, peer);
    else if (auto* sub = std::get_if<proto::SubmitJob>(&*first))
        serve_client(conn, *sub);
    else if (std::holds_alternative<proto::StatusRequest>(*first))
        serve_status(conn);
    else
        log("coord", peer, ": unexpected ", proto::type_name(*first));
}

void Coordinator::serve_worker(const net::Socket& conn, const proto::RegisterWorker& reg,
                               const std::string& peer) {
    std::uint32_t id;
    auto kinds = split_kinds(reg.kinds);
    {
        std::lock_guard lock(mu_);
        id = next_worker_id_++;
        workers_[id] = WorkerInfo{reg.name, peer, kinds, ""};
    }
    log("coord", "worker ", reg.name, " joined from ", peer, " [", reg.kinds, "]");
    if (!proto::send_msg(conn, proto::RegisterAck{id})) return;

    std::string left_because = "disconnected";
    for (;;) {
        if (!proto::recv_as<proto::RequestTask>(conn)) break;
        auto task = take_task(kinds);
        if (!task) {
            if (!proto::send_msg(conn, proto::NoTask{})) break;
            continue;
        }
        {
            std::lock_guard lock(mu_);
            workers_[id].current = task->name;
        }
        log("coord", task->name, " -> ", reg.name);
        auto start = Clock::now();

        proto::Task msg{task->id, task->kind, task->in_ext, task->job->out_ext, task->size};
        if (!proto::send_msg(conn, msg) || !net::send_file(conn, task->input, task->size)) {
            retry_or_fail(task, "lost " + reg.name + " while sending input");
            break;
        }

        std::optional<proto::TaskResult> result;
        for (;;) {
            auto m = proto::recv_msg(conn);
            if (!m) break;
            if (std::holds_alternative<proto::Heartbeat>(*m)) continue;
            if (auto* r = std::get_if<proto::TaskResult>(&*m); r && r->task_id == task->id)
                result = *r;
            break;
        }
        if (!result) {
            left_because = "went silent during " + task->name;
            retry_or_fail(task, reg.name + " went silent");
            break;
        }

        Result r{task->index, result->ok, result->message, reg.name, 0, {}, 0};
        if (result->ok) {
            r.output = task->job->dir / ("out-" + std::to_string(task->index) + "." +
                                         task->job->out_ext);
            r.size = result->size;
            std::error_code ec;
            fs::create_directories(task->job->dir, ec);
            if (!net::recv_file(conn, r.output, r.size)) {
                retry_or_fail(task, "lost " + reg.name + " while receiving output");
                break;
            }
        }
        r.millis = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start).count());
        log("coord", task->name, (r.ok ? " done by " : " FAILED on "), reg.name, " in ",
            r.millis, "ms", r.ok ? "" : ": " + r.message);

        std::lock_guard lock(mu_);
        workers_[id].current.clear();
        finish(*task, std::move(r));
    }

    std::lock_guard lock(mu_);
    workers_.erase(id);
    log("coord", "worker ", reg.name, " left (", left_because, ")");
}

std::shared_ptr<Coordinator::Task> Coordinator::take_task(const std::set<std::string>& kinds) {
    std::unique_lock lock(mu_);
    std::shared_ptr<Task> found;
    task_cv_.wait_for(lock, 2s, [&] {
        for (auto it = queue_.begin(); it != queue_.end();) {
            if ((*it)->job->cancelled) {
                it = queue_.erase(it);
                continue;
            }
            if (kinds.count((*it)->kind)) {
                found = *it;
                queue_.erase(it);
                return true;
            }
            ++it;
        }
        return false;
    });
    return found;
}

void Coordinator::retry_or_fail(const std::shared_ptr<Task>& task, const std::string& why) {
    std::lock_guard lock(mu_);
    if (task->job->cancelled) return;
    if (++task->attempts >= opts_.max_attempts) {
        log("coord", task->name, " giving up after ", task->attempts, " attempts: ", why);
        finish(*task, Result{task->index, false, "gave up after retries: " + why, "", 0, {}, 0});
        return;
    }
    log("coord", task->name, " re-queued: ", why);
    queue_.push_front(task);
    task_cv_.notify_all();
}

void Coordinator::finish(Task& task, Result r) {
    auto& job = *task.job;
    if (job.cancelled) {
        std::error_code ec;
        if (!r.output.empty()) fs::remove(r.output, ec);
        return;
    }
    job.done.push_back(std::move(r));
    --job.remaining;
    job.cv.notify_all();
}

bool Coordinator::any_worker_can(const std::string& kind) const {
    for (auto& [id, w] : workers_)
        if (w.kinds.count(kind)) return true;
    return false;
}

void Coordinator::serve_client(const net::Socket& conn, const proto::SubmitJob& submit) {
    auto job = std::make_shared<Job>();
    {
        std::lock_guard lock(mu_);
        job->id = next_job_id_++;
        ++active_jobs_;
    }
    job->out_ext = conv::ext_of("x." + submit.out_ext);
    job->dir = opts_.spool / ("job-" + std::to_string(job->id));
    job->remaining = submit.count;
    fs::create_directories(job->dir);
    log("coord", "job ", job->id, ": ", submit.count, " file(s) -> ", job->out_ext);

    auto cancel = [&](const char* why) {
        std::lock_guard lock(mu_);
        job->cancelled = true;
        --active_jobs_;
        log("coord", "job ", job->id, " cancelled: ", why);
    };

    for (std::uint32_t i = 0; i < submit.count; ++i) {
        auto up = proto::recv_as<proto::FileUpload>(conn);
        if (!up) return cancel("client went away during upload");

        auto task = std::make_shared<Task>();
        task->job = job;
        task->index = i;
        task->name = "job" + std::to_string(job->id) + "/" + fs::path(up->name).filename().string();
        task->in_ext = conv::ext_of(up->name);
        task->size = up->size;
        bool ext_ok = safe_ext(task->in_ext);
        task->input = job->dir / ("in-" + std::to_string(i) + (ext_ok ? "." + task->in_ext : ""));
        if (!net::recv_file(conn, task->input, up->size))
            return cancel("client went away during upload");

        auto kind = ext_ok && safe_ext(job->out_ext) ? conv::kind_for(task->in_ext, job->out_ext)
                                                      : std::nullopt;
        std::lock_guard lock(mu_);
        if (!kind) {
            finish(*task, Result{i, false,
                                 "don't know how to convert ." + task->in_ext + " to ." + job->out_ext,
                                 "", 0, {}, 0});
        } else if (!any_worker_can(*kind)) {
            finish(*task, Result{i, false, "no connected worker can do " + *kind +
                                               " conversions (missing tools?)", "", 0, {}, 0});
        } else {
            task->kind = *kind;
            task->id = next_task_id_++;
            queue_.push_back(task);
            task_cv_.notify_all();
        }
    }

    proto::JobDone summary;
    std::unique_lock lock(mu_);
    for (;;) {
        bool ready = job->cv.wait_for(lock, 1s, [&] { return !job->done.empty() || job->remaining == 0; });
        if (!ready) {
            if (!net::peer_closed(conn)) continue;
            lock.unlock();
            fs::remove_all(job->dir);
            return cancel("client went away");
        }
        while (!job->done.empty()) {
            Result r = std::move(job->done.front());
            job->done.pop_front();
            lock.unlock();
            (r.ok ? summary.succeeded : summary.failed)++;
            bool sent = proto::send_msg(conn, proto::FileResult{r.index, r.ok, r.message, r.worker,
                                                                r.millis, r.size}) &&
                        (!r.ok || net::send_file(conn, r.output, r.size));
            if (r.ok) fs::remove(r.output);
            lock.lock();
            if (!sent) {
                lock.unlock();
                fs::remove_all(job->dir);
                return cancel("client went away");
            }
        }
        if (job->remaining == 0) break;
    }
    --active_jobs_;
    lock.unlock();

    proto::send_msg(conn, summary);
    fs::remove_all(job->dir);
    log("coord", "job ", job->id, " finished: ", summary.succeeded, " ok, ", summary.failed,
        " failed");
}

void Coordinator::serve_status(const net::Socket& conn) {
    std::ostringstream os;
    {
        std::lock_guard lock(mu_);
        os << "workers: " << workers_.size() << "\n";
        for (auto& [id, w] : workers_) {
            os << "  " << w.name << " (" << w.peer << ") [";
            bool first = true;
            for (auto& k : w.kinds) os << (first ? "" : ",") << k, first = false;
            os << "] " << (w.current.empty() ? "idle" : "busy: " + w.current) << "\n";
        }
        os << "queued tasks: " << queue_.size() << "\n";
        os << "active jobs: " << active_jobs_ << "\n";
    }
    proto::send_msg(conn, proto::StatusReply{os.str()});
}

}  // namespace mr
