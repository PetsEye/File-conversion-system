#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "net/socket.h"
#include "proto/messages.h"

namespace mr {

class Worker {
public:
    struct Options {
        std::string coord_host;
        std::uint16_t coord_port = 7070;
        std::string name;
        unsigned slots = 0;
        std::uint32_t task_timeout_s = 3600;
        std::filesystem::path workdir;
    };

    explicit Worker(Options opts);
    void run();

private:
    enum class RunOutcome { Ok, Failed, LostCoordinator };

    void slot_loop(unsigned slot);
    void session(unsigned slot);
    bool run_task(const net::Socket& conn, const proto::Task& task, unsigned slot);
    RunOutcome run_process(const net::Socket& conn, const std::vector<std::string>& argv,
                           const std::filesystem::path& log_path, unsigned slot,
                           std::string& error);

    Options opts_;
    std::vector<std::string> kinds_;
};

}  // namespace mr
