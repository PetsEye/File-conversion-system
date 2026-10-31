#pragma once

#include <sstream>
#include <string>

namespace mr {

// Thread-safe line logger: "HH:MM:SS.mmm [tag] message".
void log_line(const char* tag, const std::string& msg);

template <typename... Args>
void log(const char* tag, Args&&... args) {
    std::ostringstream os;
    (os << ... << args);
    log_line(tag, os.str());
}

}  // namespace mr
