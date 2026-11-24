// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/protocol_versions.h"

#include <algorithm>
#include <cctype>
#include <regex>

namespace vgi_rpc {

namespace {

std::string trim(const std::string& s) {
    size_t a = 0, b = s.size();
    while (a < b && std::isspace(static_cast<unsigned char>(s[a]))) ++a;
    while (b > a && std::isspace(static_cast<unsigned char>(s[b - 1]))) --b;
    return s.substr(a, b - a);
}

// One non-negative integer with no leading zeros (except "0"), at most 9 digits.
bool parse_component(const std::string& text, int& out) {
    if (text.empty() || text.size() > 9) return false;
    if (!std::all_of(text.begin(), text.end(), [](unsigned char c) { return std::isdigit(c); })) {
        return false;
    }
    if (text.size() > 1 && text[0] == '0') return false;
    out = std::stoi(text);
    return true;
}

bool valid_name(const std::string& name) {
    if (name.empty()) return false;
    return std::all_of(name.begin(), name.end(), [](unsigned char c) {
        return std::isalnum(c) || c == '_' || c == '.' || c == ':' || c == '/' || c == '-';
    });
}

}  // namespace

MajorMinor parse_major_minor(const std::string& raw) {
    const std::string text = trim(raw);
    std::vector<std::string> parts;
    size_t start = 0;
    for (;;) {
        const size_t dot = text.find('.', start);
        parts.push_back(
            text.substr(start, dot == std::string::npos ? std::string::npos : dot - start));
        if (dot == std::string::npos) break;
        start = dot + 1;
    }
    if (parts.size() != 2 && parts.size() != 3) {
        throw std::invalid_argument("not a MAJOR.MINOR version: '" + text + "'");
    }
    int values[3] = {0, 0, 0};
    for (size_t i = 0; i < parts.size(); ++i) {
        if (!parse_component(parts[i], values[i])) {
            throw std::invalid_argument("not a MAJOR.MINOR version: '" + text + "'");
        }
    }
    return {values[0], values[1]};
}

std::string format_major_minor(MajorMinor version) {
    return std::to_string(version.first) + "." + std::to_string(version.second);
}

VersionRange VersionRange::parse(const std::string& raw) {
    const std::string text = trim(raw);
    const size_t dash = text.find('-');
    VersionRange range;
    range.min = parse_major_minor(dash == std::string::npos ? text : text.substr(0, dash));
    range.max = dash == std::string::npos ? range.min : parse_major_minor(text.substr(dash + 1));
    if (range.min > range.max) {
        throw std::invalid_argument("protocol version range '" + text + "' has min above max");
    }
    return range;
}

VersionRange VersionRange::exactly(const std::string& version) {
    const auto mm = parse_major_minor(version);
    return VersionRange{mm, mm};
}

std::string VersionRange::to_string() const {
    return format_major_minor(min) + "-" + format_major_minor(max);
}

std::string format_versions(const std::vector<std::pair<std::string, VersionRange>>& ranges) {
    std::string out;
    for (const auto& [name, range] : ranges) {
        if (!out.empty()) out += ", ";
        out += name + "=" + range.to_string();
    }
    return out;
}

std::map<std::string, VersionRange> parse_versions(const std::string& text) {
    std::map<std::string, VersionRange> out;
    size_t start = 0;
    for (;;) {
        const size_t comma = text.find(',', start);
        const std::string entry = trim(
            text.substr(start, comma == std::string::npos ? std::string::npos : comma - start));
        const size_t eq = entry.find('=');
        if (eq != std::string::npos) {
            const std::string name = trim(entry.substr(0, eq));
            if (valid_name(name) && out.count(name) == 0) {
                try {
                    out.emplace(name, VersionRange::parse(entry.substr(eq + 1)));
                } catch (const std::invalid_argument&) {
                    // skipped: unparseable entries are treated as unadvertised
                }
            }
        }
        if (comma == std::string::npos) break;
        start = comma + 1;
    }
    return out;
}

std::optional<MajorMinor> highest_common(const VersionRange& a, const VersionRange& b) {
    const MajorMinor top = std::min(a.max, b.max);
    if (top < a.min || top < b.min) return std::nullopt;
    return top;
}

ClientVersioning ClientVersioning::make(std::string protocol, std::string version,
                                        const std::string& version_range) {
    ClientVersioning out;
    out.protocol = std::move(protocol);
    const auto mm = parse_major_minor(version);
    out.version = std::move(version);
    if (version_range.empty()) {
        out.range = VersionRange{mm, mm};
    } else {
        out.range = VersionRange::parse(version_range);
        if (out.range.max != mm) {
            throw std::invalid_argument("protocol_version_range '" + version_range +
                                        "' must end at protocol_version " + out.version +
                                        "'s major.minor (" + format_major_minor(mm) +
                                        "): the range widens the newest version downward only");
        }
    }
    return out;
}

std::string ClientVersioning::wire_version(MajorMinor negotiated) const {
    // The client's own patch rides along with whatever is negotiated.
    std::string patch = "0";
    const size_t first = version.find('.');
    const size_t second = first == std::string::npos ? first : version.find('.', first + 1);
    if (second != std::string::npos) patch = version.substr(second + 1);
    return format_major_minor(negotiated) + "." + patch;
}

std::string ClientVersioning::advertised() const {
    return format_versions({{protocol, range}});
}

namespace {

std::string no_common_message(const std::string& protocol, const VersionRange& client,
                              const VersionRange& server, const std::string& endpoint) {
    const std::string where = endpoint.empty() ? "" : " at " + endpoint;
    std::string advice;
    if (server.max < client.min) {
        advice =
            "The server is too old (this client is too new for it): upgrade the worker to one "
            "supporting " +
            protocol + " " + format_major_minor(client.min) +
            " or newer, or use a client that supports " + protocol + " " +
            format_major_minor(server.max) + ".";
    } else {
        advice =
            "The client is too old (the server is too new for it): upgrade the client to one "
            "supporting " +
            protocol + " " + format_major_minor(server.min) +
            " or newer, or use a worker that supports " + protocol + " " +
            format_major_minor(client.max) + ".";
    }
    return std::string(ERROR_KIND_PROTOCOL_VERSION_UNSUPPORTED) + ": no common " + protocol +
           " version. This client supports " + protocol + " " + client.to_string() +
           "; the server" + where + " supports " + protocol + " " + server.to_string() + ". " +
           advice;
}

}  // namespace

NoCommonVersionError::NoCommonVersionError(const std::string& protocol,
                                           const VersionRange& client_range,
                                           const VersionRange& server_range,
                                           const std::string& endpoint)
    : std::runtime_error(no_common_message(protocol, client_range, server_range, endpoint)),
      client_range_(client_range),
      server_range_(server_range) {}

bool is_version_refusal_kind(const std::string& kind) noexcept {
    return kind == ERROR_KIND_PROTOCOL_VERSION_UNSUPPORTED || kind == "protocol_version_mismatch";
}

void EndpointNegotiation::observe(const std::string& value) {
    auto parsed = parse_versions(value);
    std::lock_guard<std::mutex> lock(mutex_);
    advertised_ = std::move(parsed);
    seen_ = true;
    legacy_.clear();
}

void EndpointNegotiation::observe_refusal(const std::string& protocol,
                                          const std::string& error_kind, const std::string& message,
                                          const RemoteStatus& status) {
    if (!is_version_refusal_kind(error_kind)) return;
    if (auto info = status.error_info()) {
        auto it = info->metadata.find(keys::PROTOCOL_VERSIONS);
        if (it != info->metadata.end()) {
            auto parsed = parse_versions(it->second);
            if (!parsed.empty()) {
                std::lock_guard<std::mutex> lock(mutex_);
                advertised_ = std::move(parsed);
                seen_ = true;
                legacy_.clear();
                return;
            }
        }
    }
    // A pre-spec refusal names one server version: "Server: 3.4.0" in the
    // message, or "server requires 3.4.0" in the precondition description.
    static const std::regex legacy_re(R"((?:Server:\s*|server requires\s+)(\d+\.\d+)(?:\.\d+)?)");
    std::vector<std::string> texts{message};
    if (auto failure = status.precondition_failure()) {
        for (const auto& v : failure->violations) texts.push_back(v.description);
    }
    for (const auto& text : texts) {
        std::smatch m;
        if (std::regex_search(text, m, legacy_re)) {
            try {
                const auto mm = parse_major_minor(m[1].str());
                std::lock_guard<std::mutex> lock(mutex_);
                if (!seen_) legacy_[protocol] = mm;
            } catch (const std::invalid_argument&) {
            }
            return;
        }
    }
}

MajorMinor EndpointNegotiation::choose(const ClientVersioning& client) const {
    std::optional<VersionRange> server;
    std::optional<MajorMinor> legacy;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (auto it = advertised_.find(client.protocol); it != advertised_.end()) {
            server = it->second;
        }
        if (auto it = legacy_.find(client.protocol); it != legacy_.end()) legacy = it->second;
    }
    if (server) {
        auto common = highest_common(client.range, *server);
        if (!common) throw NoCommonVersionError(client.protocol, client.range, *server, endpoint_);
        return *common;
    }
    if (legacy) {
        if (!client.range.contains(*legacy)) {
            throw NoCommonVersionError(client.protocol, client.range,
                                       VersionRange{*legacy, *legacy}, endpoint_);
        }
        return *legacy;
    }
    return client.range.max;
}

bool EndpointNegotiation::has_advertisement() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return seen_;
}

std::map<std::string, VersionRange> EndpointNegotiation::advertised() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return advertised_;
}

}  // namespace vgi_rpc
