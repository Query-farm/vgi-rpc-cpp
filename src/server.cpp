// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/server.h"
#include "vgi_rpc/metadata.h"
#include "vgi_rpc/describe.h"

#include <optional>
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

ServerBuilder& ServerBuilder::add_unary(
    const std::string& name,
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

ServerBuilder& ServerBuilder::add_void(
    const std::string& name,
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
    const std::string& name,
    std::shared_ptr<arrow::Schema> params_schema,
    std::shared_ptr<arrow::Schema> output_schema,
    std::function<Stream(const Request&, CallContext&)> factory,
    const std::string& doc,
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
    const std::string& name,
    std::shared_ptr<arrow::Schema> params_schema,
    std::shared_ptr<arrow::Schema> input_schema,
    std::shared_ptr<arrow::Schema> output_schema,
    std::function<Stream(const Request&, CallContext&)> factory,
    const std::string& doc,
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

    return std::unique_ptr<Server>(new Server(
        std::move(method_map), std::move(server_id),
        protocol_name_, std::move(protocol_hash), protocol_version_,
        access_log_path_, access_log_max_record_bytes_));
}

// Server

namespace {

// `major.minor` of a canonical semver string, or nothing when it is not one.
std::optional<std::pair<int, int>> semver_prefix(const std::string& version) {
    int major = 0;
    int minor = 0;
    size_t offset = 0;
    for (int* part : {&major, &minor}) {
        size_t consumed = 0;
        try {
            *part = std::stoi(version.substr(offset), &consumed);
        } catch (const std::exception&) {
            return std::nullopt;
        }
        offset += consumed;
        if (part == &major) {
            if (offset >= version.size() || version[offset] != '.') return std::nullopt;
            ++offset;
        }
    }
    // A trailing `.patch` is required but ignored: the surface does not change
    // within a major.minor.
    if (offset >= version.size() || version[offset] != '.') return std::nullopt;
    return std::pair{major, minor};
}

}  // namespace

std::string Server::protocol_version_error(
    const std::shared_ptr<arrow::KeyValueMetadata>& custom_metadata) const {
    if (protocol_version_.empty()) return {};
    const auto expected = semver_prefix(protocol_version_);
    if (!expected) return {};

    const std::string header = "VGI client/worker protocol_version mismatch.\n  Client: ";
    const auto trailer = "\n  Server: " + protocol_version_ + "\n  Direction: ";

    if (!custom_metadata) {
        return header + "<not declared>" + trailer +
               "the client sent no " + std::string(keys::PROTOCOL_VERSION) +
               " metadata key. This is either a framework bug or a non-VGI client.";
    }
    const auto index = custom_metadata->FindKey(keys::PROTOCOL_VERSION);
    if (index < 0) {
        return header + "<not declared>" + trailer +
               "the client sent no " + std::string(keys::PROTOCOL_VERSION) +
               " metadata key. This is either a framework bug or a non-VGI client.";
    }

    const auto& declared = custom_metadata->value(index);
    const auto actual = semver_prefix(declared);
    if (!actual) {
        return header + declared + trailer +
               "client sent a malformed protocol_version; expected canonical semver "
               "MAJOR.MINOR.PATCH.";
    }
    if (*actual == *expected) return {};

    return header + declared + trailer +
           (*actual < *expected
                ? "client is too old; upgrade it to a version supporting protocol_version " +
                      protocol_version_ + "."
                : "server is too old; upgrade the worker to a version supporting "
                  "protocol_version " + declared + ".");
}

Server::Server(std::unordered_map<std::string, MethodInfo> methods,
               std::string server_id,
               std::string protocol_name,
               std::string protocol_hash,
               std::string protocol_version,
               const std::string& access_log_path,
               int64_t access_log_max_record_bytes)
    : methods_(std::move(methods))
    , server_id_(std::move(server_id))
    , protocol_name_(std::move(protocol_name))
    , protocol_hash_(std::move(protocol_hash))
    , protocol_version_(std::move(protocol_version)) {
    if (!access_log_path.empty()) {
        access_log_ = std::make_unique<AccessLogWriter>(
            access_log_path, server_id_, protocol_name_, protocol_hash_,
            access_log_max_record_bytes);
    }
}

}  // namespace vgi_rpc
