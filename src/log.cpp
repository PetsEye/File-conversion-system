#include "log.h"

#include <chrono>
#include <cstdio>
#include <ctime>
#include <mutex>

namespace mr {

void log_line(const char* tag, const std::string& msg) {
    static std::mutex mu;
    using namespace std::chrono;
    auto now = system_clock::now();
    auto ms = duration_cast<milliseconds>(now.time_since_epoch()).count() % 1000;
    std::time_t t = system_clock::to_time_t(now);
    std::tm tm{};
    localtime_r(&t, &tm);

    std::lock_guard lock(mu);
    std::fprintf(stderr, "%02d:%02d:%02d.%03lld [%s] %s\n", tm.tm_hour, tm.tm_min,
                 tm.tm_sec, static_cast<long long>(ms), tag, msg.c_str());
}

}  // namespace mr
