#pragma once

#include <filesystem>
#include <optional>
#include <string>
#include <vector>

namespace mr::conv {

namespace fs = std::filesystem;

// Kinds: "image" (ImageMagick, ffmpeg fallback), "media" (ffmpeg),
// "document" (pandoc), "office" (LibreOffice -> pdf).
std::string ext_of(const fs::path& p);
std::optional<std::string> kind_for(const std::string& in_ext, const std::string& out_ext);
std::vector<std::string> local_kinds();
std::string find_in_path(const std::string& name);

struct Command {
    std::vector<std::string> argv;
    fs::path output;
};
std::optional<Command> build_command(const std::string& kind, const fs::path& input,
                                     const std::string& out_ext, const fs::path& workdir);

}  // namespace mr::conv
