// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/server.h"
#include "vgi_rpc/metadata.h"
#include "vgi_rpc/describe.h"

#include <array>
#include <optional>
#include <stdexcept>
#include <stdexcept>
#include <utility>

namespace vgi_rpc {

// ServerBuilder

void ServerBuilder::check_duplicate(const std::string& name) const {
    for (const auto& m : methods_) {
        if (m.name == name) {
            throw std::logic_error("Duplicate method name: '" + name + "'");
        }
    }
}

ServerBuilder& ServerBuilder::add_unary(const std::string& name,
                                        std::shared_ptr<arrow::Schema> params_schema,
                                        std::shared_ptr<arrow::Schema> result_schema,
                                        std::function<Result(const Request&, CallContext&)> handler,
                                        const std::string& doc) {
    if (!params_schema) throw std::invalid_argument("params_schema must not be null");
    if (!result_schema) throw std::invalid_argument("result_schema must not be null");
    check_duplicate(name);

    MethodInfo info;
    info.name = name;
    info.method_type = MethodType::UNARY;
    info.params_schema = std::move(params_schema);
    info.result_schema = std::move(result_schema);
    info.handler = std::move(handler);
    info.doc = doc;
    info.has_return = true;
    methods_.push_back(std::move(info));
    return *this;
}

ServerBuilder& ServerBuilder::add_void(const std::string& name,
                                       std::shared_ptr<arrow::Schema> params_schema,
                                       std::function<void(const Request&, CallContext&)> handler,
                                       const std::string& doc) {
    if (!params_schema) throw std::invalid_argument("params_schema must not be null");
    check_duplicate(name);

    MethodInfo info;
    info.name = name;
    info.method_type = MethodType::UNARY;
    info.params_schema = std::move(params_schema);
    info.result_schema = empty_schema();
    info.has_return = false;
    info.doc = doc;
    info.handler = [h = std::move(handler)](const Request& req, CallContext& ctx) -> Result {
        h(req, ctx);
        return Result::void_result();
    };
    methods_.push_back(std::move(info));
    return *this;
}

ServerBuilder& ServerBuilder::add_producer(
    const std::string& name, std::shared_ptr<arrow::Schema> params_schema,
    std::shared_ptr<arrow::Schema> output_schema,
    std::function<Stream(const Request&, CallContext&)> factory, const std::string& doc,
    std::shared_ptr<arrow::Schema> header_schema) {
    if (!params_schema) throw std::invalid_argument("params_schema must not be null");
    if (!output_schema) throw std::invalid_argument("output_schema must not be null");
    check_duplicate(name);

    MethodInfo info;
    info.name = name;
    info.method_type = MethodType::STREAM;
    info.params_schema = std::move(params_schema);
    info.result_schema = empty_schema();
    info.input_schema = empty_schema();
    info.output_schema = std::move(output_schema);
    info.header_schema = std::move(header_schema);
    // A stream yields batches; it has no return value.  __describe__ reports
    // has_return=false for every stream method, producer and exchange alike.
    info.has_return = false;
    info.doc = doc;
    info.stream_factory = std::move(factory);
    methods_.push_back(std::move(info));
    return *this;
}

ServerBuilder& ServerBuilder::add_exchange(
    const std::string& name, std::shared_ptr<arrow::Schema> params_schema,
    std::shared_ptr<arrow::Schema> input_schema, std::shared_ptr<arrow::Schema> output_schema,
    std::function<Stream(const Request&, CallContext&)> factory, const std::string& doc,
    std::shared_ptr<arrow::Schema> header_schema) {
    if (!params_schema) throw std::invalid_argument("params_schema must not be null");
    if (!input_schema) throw std::invalid_argument("input_schema must not be null");
    if (!output_schema) throw std::invalid_argument("output_schema must not be null");
    check_duplicate(name);

    MethodInfo info;
    info.name = name;
    info.method_type = MethodType::STREAM;
    info.params_schema = std::move(params_schema);
    info.result_schema = empty_schema();
    info.input_schema = std::move(input_schema);
    info.output_schema = std::move(output_schema);
    info.header_schema = std::move(header_schema);
    // A stream yields batches; it has no return value.  __describe__ reports
    // has_return=false for every stream method, producer and exchange alike.
    info.has_return = false;
    info.doc = doc;
    info.stream_factory = std::move(factory);
    info.is_exchange = true;
    methods_.push_back(std::move(info));
    return *this;
}

ServerBuilder& ServerBuilder::server_id(std::string id) {
    server_id_ = std::move(id);
    return *this;
}

ServerBuilder& ServerBuilder::enable_describe(const std::string& protocol_name) {
    describe_enabled_ = true;
    protocol_name_ = protocol_name;
    return *this;
}

ServerBuilder& ServerBuilder::protocol_version(std::string version) {
    protocol_version_ = std::move(version);
    return *this;
}

ServerBuilder& ServerBuilder::on_serve_start(std::function<void(TransportKind)> hook) {
    on_serve_start_ = std::move(hook);
    return *this;
}

ServerBuilder& ServerBuilder::enable_transport_options(bool enabled) {
    transport_options_enabled_ = enabled;
    return *this;
}

ServerBuilder& ServerBuilder::access_log(std::string path, int64_t max_record_bytes) {
    access_log_path_ = std::move(path);
    access_log_max_record_bytes_ = max_record_bytes;
    return *this;
}

std::unique_ptr<Server> ServerBuilder::build() {
    if (built_) {
        throw std::logic_error("ServerBuilder::build() has already been called");
    }
    built_ = true;

    auto server_id = server_id_.empty() ? random_hex(12) : std::move(server_id_);

    std::unordered_map<std::string, MethodInfo> method_map;
    for (auto& m : methods_) {
        method_map[m.name] = std::move(m);
    }

    // Compute protocol_hash over the user methods (before __describe__ is added)
    // so it is available to both __describe__ and the access log.
    std::string protocol_hash = compute_protocol_hash(protocol_name_, method_map);

    if (describe_enabled_) {
        register_describe(method_map, protocol_name_, server_id, protocol_version_);
    }

    if (transport_options_enabled_) {
        // Registered after __describe__ so neither synthetic method appears in
        // the other's output — they are framework surface, not service surface.
        register_transport_options(method_map, server_id);
    }

    return std::unique_ptr<Server>(
        new Server(std::move(method_map), std::move(server_id), protocol_name_,
                   std::move(protocol_hash), protocol_version_, access_log_path_,
                   access_log_max_record_bytes_, std::move(on_serve_start_)));
}

// Server

namespace {

// The canonical-semver grammar the wire spec fixes: MAJOR.MINOR.PATCH, each a
// non-negative integer with no leading zeros, and nothing else — no prerelease,
// no build metadata, no sign, no whitespace. Mirrors `SEMVER_REGEX` in the
// reference implementation, which rejects anything looser outright.
std::optional<std::array<int, 3>> parse_semver(const std::string& version) {
    std::array<int, 3> parts{};
    size_t offset = 0;
    for (size_t i = 0; i < parts.size(); ++i) {
        if (i > 0) {
            if (offset >= version.size() || version[offset] != '.') return std::nullopt;
            ++offset;
        }
        const size_t start = offset;
        while (offset < version.size() && version[offset] >= '0' && version[offset] <= '9') {
            ++offset;
        }
        const size_t digits = offset - start;
        // No leading zeros, except the literal `0`.
        if (digits == 0 || (digits > 1 && version[start] == '0')) return std::nullopt;
        // Anything long enough to overflow an int is malformed for our purposes.
        if (digits > 9) return std::nullopt;
        parts[i] = std::stoi(version.substr(start, digits));
    }
    if (offset != version.size()) return std::nullopt;
    return parts;
}

}  // namespace

std::string Server::protocol_version_error(
    const std::shared_ptr<arrow::KeyValueMetadata>& custom_metadata) const {
    if (protocol_version_.empty()) return {};
    // The constructor rejected anything unparseable, so the server's own
    // version is known good by the time a request can reach this.
    const auto expected = *parse_semver(protocol_version_);

    const std::string header = "VGI client/worker protocol_version mismatch.\n  Client: ";
    const auto trailer = "\n  Server: " + protocol_version_ + "\n  Direction: ";

    const auto index = custom_metadata ? custom_metadata->FindKey(keys::PROTOCOL_VERSION) : -1;
    if (index < 0) {
        return header + "<not declared>" + trailer + "the client did not send a " +
               std::string(keys::PROTOCOL_VERSION) +
               " metadata key. This is either a vgi-rpc framework bug or a non-VGI client "
               "connecting to a VGI worker.";
    }

    const auto& declared = custom_metadata->value(index);
    const auto actual = parse_semver(declared);
    if (!actual) {
        return header + declared + trailer +
               "client sent a malformed protocol_version. Expected canonical semver "
               "MAJOR.MINOR.PATCH.";
    }
    // Exact major+minor match; patch is ignored, since the surface does not
    // change within one.
    const std::pair actual_surface{(*actual)[0], (*actual)[1]};
    const std::pair expected_surface{expected[0], expected[1]};
    if (actual_surface == expected_surface) return {};

    return header + declared + trailer +
           (actual_surface < expected_surface
                ? "client is too old; upgrade the VGI extension/client to a version supporting "
                  "protocol_version " +
                      protocol_version_ + "."
                : "server is too old; upgrade the VGI worker to a version supporting "
                  "protocol_version " +
                      declared + ".");
}

Server::Server(std::unordered_map<std::string, MethodInfo> methods, std::string server_id,
               std::string protocol_name, std::string protocol_hash, std::string protocol_version,
               const std::string& access_log_path, int64_t access_log_max_record_bytes,
               std::function<void(TransportKind)> on_serve_start)
    : methods_(std::move(methods)),
      server_id_(std::move(server_id)),
      protocol_name_(std::move(protocol_name)),
      protocol_hash_(std::move(protocol_hash)),
      protocol_version_(std::move(protocol_version)),
      on_serve_start_(std::move(on_serve_start)) {
    // A worker that declares a version it cannot parse would silently enforce
    // nothing, which is worse than not declaring one: the operator believes
    // there is a gate. Refuse to build such a server at all.
    if (!protocol_version_.empty() && !parse_semver(protocol_version_)) {
        throw std::invalid_argument(
            "Invalid protocol version '" + protocol_version_ +
            "': expected canonical semver MAJOR.MINOR.PATCH with non-negative integers "
            "and no leading zeros (no prereleases or build metadata).");
    }
    if (!access_log_path.empty()) {
        access_log_ =
            std::make_unique<AccessLogWriter>(access_log_path, server_id_, protocol_name_,
                                              protocol_hash_, access_log_max_record_bytes);
    }
}

void Server::notify_serve_start(TransportKind transport_kind) {
    // std::call_once commits only when the callable returns.  If the user hook
    // throws, the flag remains unset and a later request retries; concurrent
    // callers serialize behind whichever attempt is currently running.
    std::call_once(serve_start_once_, [this, transport_kind]() {
        if (on_serve_start_) on_serve_start_(transport_kind);
        transport_kind_ = transport_kind;
    });
    if (transport_kind_ != transport_kind) {
        throw std::logic_error("server is already bound to transport '" +
                               std::string(transport_kind_name(*transport_kind_)) + "'");
    }
}

}  // namespace vgi_rpc
