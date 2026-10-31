#include "convert/converters.h"

#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <set>
#include <sstream>

namespace mr::conv {

namespace {

const std::set<std::string> kImage = {"jpg", "jpeg", "png", "webp", "gif", "bmp",
                                      "tif", "tiff", "heic", "avif", "ico"};
const std::set<std::string> kMedia = {"mp3", "wav", "flac", "ogg", "opus", "m4a", "aac",
                                      "mp4", "mkv", "mov", "webm", "avi", "gif"};
const std::set<std::string> kDocument = {"md", "markdown", "html", "rst", "org", "txt",
                                         "docx", "odt", "epub", "tex"};
const std::set<std::string> kOffice = {"doc", "docx", "odt", "rtf", "ppt", "pptx",
                                       "odp", "xls", "xlsx", "ods"};

bool has(const std::set<std::string>& s, const std::string& e) { return s.count(e) > 0; }

std::string first_in_path(std::initializer_list<const char*> names) {
    for (const char* n : names)
        if (auto p = find_in_path(n); !p.empty()) return p;
    return {};
}

std::string image_tool() { return first_in_path({"magick", "convert"}); }
std::string office_tool() { return first_in_path({"soffice", "libreoffice"}); }

}  // namespace

std::string ext_of(const fs::path& p) {
    std::string e = p.extension().string();
    if (!e.empty() && e[0] == '.') e.erase(0, 1);
    std::transform(e.begin(), e.end(), e.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return e;
}

std::optional<std::string> kind_for(const std::string& in_ext, const std::string& out_ext) {
    if (in_ext.empty() || out_ext.empty() || in_ext == out_ext) return std::nullopt;
    if (has(kImage, in_ext) && has(kImage, out_ext)) return "image";
    if (has(kMedia, in_ext) && has(kMedia, out_ext)) return "media";
    if (has(kOffice, in_ext) && out_ext == "pdf") return "office";
    if (has(kDocument, in_ext) && has(kDocument, out_ext)) return "document";
    return std::nullopt;
}

std::string find_in_path(const std::string& name) {
    const char* path = std::getenv("PATH");
    if (!path) return {};
    std::stringstream ss(path);
    std::string dir;
    while (std::getline(ss, dir, ':')) {
        if (dir.empty()) continue;
        fs::path cand = fs::path(dir) / name;
        if (::access(cand.c_str(), X_OK) == 0) return cand.string();
    }
    return {};
}

std::vector<std::string> local_kinds() {
    std::vector<std::string> kinds;
    bool ffmpeg = !find_in_path("ffmpeg").empty();
    if (ffmpeg || !image_tool().empty()) kinds.push_back("image");
    if (ffmpeg) kinds.push_back("media");
    if (!find_in_path("pandoc").empty()) kinds.push_back("document");
    if (!office_tool().empty()) kinds.push_back("office");
    return kinds;
}

std::optional<Command> build_command(const std::string& kind, const fs::path& input,
                                     const std::string& out_ext, const fs::path& workdir) {
    Command c;
    c.output = workdir / ("out." + out_ext);
    const std::string in = input.string(), out = c.output.string();
    const std::string in_ext = ext_of(input);

    if (kind == "image") {
        if (auto im = image_tool(); !im.empty()) {
            bool multi_frame = in_ext == "gif" || in_ext == "tif" || in_ext == "tiff";
            c.argv = {im, multi_frame && out_ext != "gif" ? in + "[0]" : in, out};
            return c;
        }
    }
    if (kind == "image" || kind == "media") {
        auto ff = find_in_path("ffmpeg");
        if (ff.empty()) return std::nullopt;
        c.argv = {ff, "-nostdin", "-hide_banner", "-loglevel", "error", "-y", "-i", in};
        if (has(kImage, out_ext) && !has(kMedia, out_ext)) {
            c.argv.push_back("-frames:v");
            c.argv.push_back("1");
        }
        c.argv.push_back(out);
        return c;
    }
    if (kind == "document") {
        auto pd = find_in_path("pandoc");
        if (pd.empty()) return std::nullopt;
        c.argv = {pd, in, "-s"};
        if (out_ext == "txt") c.argv.insert(c.argv.end(), {"-t", "plain"});
        c.argv.insert(c.argv.end(), {"-o", out});
        return c;
    }
    if (kind == "office") {
        auto so = office_tool();
        if (so.empty()) return std::nullopt;
        // Separate profile per run: concurrent soffice processes sharing one hang on its lock.
        c.argv = {so, "-env:UserInstallation=file://" + (workdir / "lo-profile").string(),
                  "--headless", "--convert-to", out_ext, "--outdir", workdir.string(), in};
        c.output = workdir / (input.stem().string() + "." + out_ext);
        return c;
    }
    return std::nullopt;
}

}  // namespace mr::conv
