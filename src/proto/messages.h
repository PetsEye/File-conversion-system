#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <variant>

namespace mr::proto {

// Wire values: never renumber.
enum class MsgType : std::uint8_t {
    RegisterWorker = 1,
    RegisterAck = 2,
    RequestTask = 3,
    NoTask = 4,
    Task = 5,
    Heartbeat = 6,
    TaskResult = 7,
    SubmitJob = 8,
    FileUpload = 9,
    FileResult = 10,
    JobDone = 11,
    StatusRequest = 12,
    StatusReply = 13,
};

#define MR_MESSAGE(Name, ...)                                         \
    static constexpr MsgType type = MsgType::Name;                    \
    static constexpr const char* kTypeName = #Name;                        \
    auto fields() { return std::tie(__VA_ARGS__); }                   \
    bool operator==(const Name&) const = default;

#define MR_EMPTY_MESSAGE(Name)                                        \
    static constexpr MsgType type = MsgType::Name;                    \
    static constexpr const char* kTypeName = #Name;                        \
    auto fields() { return std::tie(); }                              \
    bool operator==(const Name&) const = default;

// Worker -> coordinator. kinds is comma-separated, e.g. "image,media".
struct RegisterWorker {
    std::string name;
    std::string kinds;
    MR_MESSAGE(RegisterWorker, name, kinds)
};
struct RegisterAck {
    std::uint32_t worker_id = 0;
    MR_MESSAGE(RegisterAck, worker_id)
};
struct RequestTask {
    MR_EMPTY_MESSAGE(RequestTask)
};
struct NoTask {
    MR_EMPTY_MESSAGE(NoTask)
};
// Followed on the wire by `size` raw bytes of input.
struct Task {
    std::uint64_t task_id = 0;
    std::string kind;
    std::string in_ext;
    std::string out_ext;
    std::uint64_t size = 0;
    MR_MESSAGE(Task, task_id, kind, in_ext, out_ext, size)
};
struct Heartbeat {
    MR_EMPTY_MESSAGE(Heartbeat)
};
// Followed by `size` raw bytes of output when ok.
struct TaskResult {
    std::uint64_t task_id = 0;
    bool ok = false;
    std::string message;
    std::uint64_t size = 0;
    MR_MESSAGE(TaskResult, task_id, ok, message, size)
};

// Client -> coordinator: SubmitJob, then `count` FileUploads.
struct SubmitJob {
    std::string out_ext;
    std::uint32_t count = 0;
    MR_MESSAGE(SubmitJob, out_ext, count)
};
// Followed by `size` raw bytes.
struct FileUpload {
    std::string name;
    std::uint64_t size = 0;
    MR_MESSAGE(FileUpload, name, size)
};
// Coordinator -> client, one per file in completion order. Followed by `size` raw bytes when ok.
struct FileResult {
    std::uint32_t index = 0;
    bool ok = false;
    std::string message;
    std::string worker;
    std::uint64_t millis = 0;
    std::uint64_t size = 0;
    MR_MESSAGE(FileResult, index, ok, message, worker, millis, size)
};
struct JobDone {
    std::uint32_t succeeded = 0;
    std::uint32_t failed = 0;
    MR_MESSAGE(JobDone, succeeded, failed)
};

struct StatusRequest {
    MR_EMPTY_MESSAGE(StatusRequest)
};
struct StatusReply {
    std::string text;
    MR_MESSAGE(StatusReply, text)
};

#undef MR_MESSAGE
#undef MR_EMPTY_MESSAGE

using Message = std::variant<RegisterWorker, RegisterAck, RequestTask, NoTask, Task, Heartbeat,
                             TaskResult, SubmitJob, FileUpload, FileResult, JobDone,
                             StatusRequest, StatusReply>;

std::string encode(Message m);
std::optional<Message> decode(std::string_view payload);
const char* type_name(const Message& m);

}  // namespace mr::proto
