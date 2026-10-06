// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/server.h"
#include "vgi_rpc/metadata.h"
#include "vgi_rpc/reflection.h"
#include "vgi_rpc/token_identity.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <set>
#include <optional>
#include <stdexcept>
#include <stdexcept>
#include <utility>

namespace vgi_rpc {

// ProtocolBuilder

ProtocolBuilder::ProtocolBuilder(std::string name, std::string version)
    : name_(std::move(name)), version_(std::move(version)) {}

void ProtocolBuilder::check_duplicate(const std::string& name) const {
    for (const auto& m : methods_) {
        if (m.name == name) {
            throw std::logic_error("Duplicate method name: '" + name + "'");
        }
    }
}

ProtocolBuilder& ProtocolBuilder::add_unary(
    const std::string& name, std::shared_ptr<arrow::Schema> params_schema,
    std::shared_ptr<arrow::Schema> result_schema,
    std::function<Result(const Request&, CallContext&)> handler, const std::string& doc) {
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

ProtocolBuilder& ProtocolBuilder::add_void(
    const std::string& name, std::shared_ptr<arrow::Schema> params_schema,
    std::function<void(const Request&, CallContext&)> handler, const std::string& doc) {
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

ProtocolBuilder& ProtocolBuilder::add_producer(
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
    // A stream yields batches; it has no return value.  Reflection reports
    // has_return=false for every stream method, producer and exchange alike.
    info.has_return = false;
    info.doc = doc;
    info.stream_factory = std::move(factory);
    methods_.push_back(std::move(info));
    return *this;
}

ProtocolBuilder& ProtocolBuilder::add_exchange(
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
    // A stream yields batches; it has no return value.  Reflection reports
    // has_return=false for every stream method, producer and exchange alike.
    info.has_return = false;
    info.doc = doc;
    info.stream_factory = std::move(factory);
    info.is_exchange = true;
    methods_.push_back(std::move(info));
    return *this;
}

// ServerBuilder

void ServerBuilder::check_not_built(const char* what) const {
    if (built_) {
        throw std::logic_error(std::string("ServerBuilder::") + what +
                               " called after build(): the server's protocol set is sealed "
                               "before it serves, and a late registration would be silently "
                               "absent from every transport and from reflection");
    }
}

ServerBuilder& ServerBuilder::add_unary(const std::string& name,
                                        std::shared_ptr<arrow::Schema> params_schema,
                                        std::shared_ptr<arrow::Schema> result_schema,
                                        std::function<Result(const Request&, CallContext&)> handler,
                                        const std::string& doc) {
    check_not_built("add_unary");
    primary_.add_unary(name, std::move(params_schema), std::move(result_schema), std::move(handler),
                       doc);
    return *this;
}

ServerBuilder& ServerBuilder::add_void(const std::string& name,
                                       std::shared_ptr<arrow::Schema> params_schema,
                                       std::function<void(const Request&, CallContext&)> handler,
                                       const std::string& doc) {
    check_not_built("add_void");
    primary_.add_void(name, std::move(params_schema), std::move(handler), doc);
    return *this;
}

ServerBuilder& ServerBuilder::add_producer(
    const std::string& name, std::shared_ptr<arrow::Schema> params_schema,
    std::shared_ptr<arrow::Schema> output_schema,
    std::function<Stream(const Request&, CallContext&)> factory, const std::string& doc,
    std::shared_ptr<arrow::Schema> header_schema) {
    check_not_built("add_producer");
    primary_.add_producer(name, std::move(params_schema), std::move(output_schema),
                          std::move(factory), doc, std::move(header_schema));
    return *this;
}

ServerBuilder& ServerBuilder::add_exchange(
    const std::string& name, std::shared_ptr<arrow::Schema> params_schema,
    std::shared_ptr<arrow::Schema> input_schema, std::shared_ptr<arrow::Schema> output_schema,
    std::function<Stream(const Request&, CallContext&)> factory, const std::string& doc,
    std::shared_ptr<arrow::Schema> header_schema) {
    check_not_built("add_exchange");
    primary_.add_exchange(name, std::move(params_schema), std::move(input_schema),
                          std::move(output_schema), std::move(factory), doc,
                          std::move(header_schema));
    return *this;
}

ServerBuilder& ServerBuilder::server_id(std::string id) {
    server_id_ = std::move(id);
    return *this;
}

ServerBuilder& ServerBuilder::protocol(std::string protocol_name) {
    check_not_built("protocol");
    primary_.name_ = std::move(protocol_name);
    return *this;
}

ServerBuilder& ServerBuilder::add_protocol(ProtocolBuilder protocol) {
    check_not_built("add_protocol");
    extra_protocols_.push_back(std::move(protocol));
    return *this;
}

ServerBuilder& ServerBuilder::include_tracebacks(bool include) {
    check_not_built("include_tracebacks");
    include_tracebacks_ = include;
    return *this;
}

ServerBuilder& ServerBuilder::identity(std::shared_ptr<IdentityImpl> impl) {
    check_not_built("identity");
    identity_ = std::move(impl);
    return *this;
}

ServerBuilder& ServerBuilder::grant_keys(std::optional<GrantKeys> keys) {
    check_not_built("grant_keys");
    if (keys) keys->validate();
    grant_keys_ = std::move(keys);
    grant_keys_explicit_ = true;
    return *this;
}

ServerBuilder& ServerBuilder::protocol_version(std::string version) {
    check_not_built("protocol_version");
    primary_.version_ = std::move(version);
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

namespace {

// Whether `name` matches the protocol grammar every routing key obeys:
// `[A-Za-z_][A-Za-z0-9_.]*`, at most 255 bytes.
bool valid_protocol_name(const std::string& name) {
    if (name.empty() || name.size() > 255) return false;
    const auto lead = static_cast<unsigned char>(name[0]);
    if (!(std::isalpha(lead) || lead == '_')) return false;
    return std::all_of(name.begin() + 1, name.end(),
                       [](unsigned char c) { return std::isalnum(c) || c == '_' || c == '.'; });
}

std::unordered_map<std::string, MethodInfo> method_map(std::vector<MethodInfo> methods) {
    std::unordered_map<std::string, MethodInfo> map;
    for (auto& m : methods) map[m.name] = std::move(m);
    return map;
}

}  // namespace

std::unique_ptr<Server> ServerBuilder::build() {
    if (built_) {
        throw std::logic_error("ServerBuilder::build() has already been called");
    }
    built_ = true;

    auto server_id = server_id_.empty() ? random_hex(12) : std::move(server_id_);

    // The registered set is validated here, once, and is then fixed for the
    // server's life.  The reserved prefix is checked on every name however it
    // was supplied -- the primary's `protocol()` and every `add_protocol` --
    // because a port that checks names declared one way lets the other way
    // shadow `vgi_rpc.Reflection.v1` (WIRE_PROTOCOL.md §3.1).
    std::set<std::string> names;
    auto check_name = [&](const std::string& name, const char* where) {
        if (name.rfind(kReservedProtocolPrefix, 0) == 0) {
            throw std::invalid_argument(std::string(where) + ": protocol name '" + name +
                                        "' uses the reserved '" + kReservedProtocolPrefix +
                                        "' prefix, which belongs to framework protocols");
        }
        if (!valid_protocol_name(name)) {
            throw std::invalid_argument(std::string(where) + ": protocol name '" + name +
                                        "' is not a valid routing key ([A-Za-z_][A-Za-z0-9_.]*)");
        }
        if (!names.insert(name).second) {
            throw std::invalid_argument(std::string(where) + ": protocol '" + name +
                                        "' is registered more than once");
        }
    };
    if (!primary_.name_.empty()) check_name(primary_.name_, "ServerBuilder::protocol");
    if (!extra_protocols_.empty() && primary_.name_.empty()) {
        throw std::invalid_argument(
            "ServerBuilder::add_protocol: hosting more than one application protocol requires the "
            "primary to declare its name with ServerBuilder::protocol(); without one there is no "
            "routing key to tell the protocols apart");
    }
    for (const auto& extra : extra_protocols_) {
        check_name(extra.name_, "ServerBuilder::add_protocol");
    }

    std::vector<HostedProtocol> protocols;
    HostedProtocol primary;
    primary.name = primary_.name_;
    primary.version = primary_.version_;
    primary.methods = method_map(std::move(primary_.methods_));
    if (transport_options_enabled_) {
        // Framework surface, not service surface: `IsApplicationMethod` keeps
        // it out of the protocol hash and out of every description.
        register_transport_options(primary.methods, server_id);
    }
    protocols.push_back(std::move(primary));
    for (auto& extra : extra_protocols_) {
        HostedProtocol hosted;
        hosted.name = std::move(extra.name_);
        hosted.version = std::move(extra.version_);
        hosted.methods = method_map(std::move(extra.methods_));
        protocols.push_back(std::move(hosted));
    }

    // Read at build, so a malformed key refuses to start the worker rather
    // than failing the first mint.
    std::optional<GrantKeys> grant_keys =
        grant_keys_explicit_ ? std::move(grant_keys_) : GrantKeys::from_env();
    if (grant_keys) {
        if (!identity_) {
            // Grants on and no other identity hooks: the framework mints and
            // accepts its own, and hosts issue_grant alone.
            IdentityOptions options;
            options.grant_keys = std::move(grant_keys);
            identity_ = std::make_shared<IdentityImpl>(std::move(options));
        } else if (!identity_->grant_keys()) {
            throw std::invalid_argument(
                "grant keys were configured (ServerBuilder::grant_keys or VGI_RPC_GRANT_KEYS) and "
                "an IdentityImpl was supplied without them. Set IdentityOptions::grant_keys so the "
                "minter and the verifier use the same keys.");
        }
    }

    // Identity, when the deployment configured it, is hosted as its own
    // framework protocol rather than as entries in any application table: it
    // lives under the reserved `vgi_rpc.` prefix, and putting it in an
    // application's method map would move that protocol's hash.
    return std::unique_ptr<Server>(new Server(
        std::move(protocols), std::move(server_id), access_log_path_, access_log_max_record_bytes_,
        std::move(on_serve_start_), std::move(identity_), include_tracebacks_));
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
    return protocol_version_error(protocols_.front(), custom_metadata);
}

std::string Server::protocol_version_error(
    const HostedProtocol& protocol,
    const std::shared_ptr<arrow::KeyValueMetadata>& custom_metadata) const {
    // Gated against the binding the request resolved to, never the primary:
    // a protocol that declared no version enforces nothing, whatever the
    // primary declares (WIRE_PROTOCOL.md §13).
    const std::string& server_version = protocol.version;
    if (server_version.empty()) return {};
    // The constructor rejected anything unparseable, so the server's own
    // version is known good by the time a request can reach this.
    const auto expected = *parse_semver(server_version);

    const std::string header = "VGI client/worker protocol_version mismatch for protocol '" +
                               protocol.name + "'.\n  Client: ";
    const auto trailer = "\n  Server: " + server_version + "\n  Direction: ";

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
                      server_version + "."
                : "server is too old; upgrade the VGI worker to a version supporting "
                  "protocol_version " +
                      declared + ".");
}

ErrorExtras Server::protocol_version_extras(
    const HostedProtocol& protocol,
    const std::shared_ptr<arrow::KeyValueMetadata>& custom_metadata) const {
    std::string declared = "<none>";
    if (custom_metadata) {
        const auto index = custom_metadata->FindKey(keys::PROTOCOL_VERSION);
        if (index >= 0) declared = custom_metadata->value(index);
    }
    // With several bindings, "Server: 2.0.0" alone does not say which server;
    // the violation names the gated protocol.
    PreconditionFailure failure;
    failure.violations.push_back(PreconditionViolation{"protocol_version", protocol.name,
                                                       "client declares " + declared +
                                                           ", server requires " + protocol.version +
                                                           "; major and minor must match"});
    ErrorExtras extras;
    extras.code = code_name(Code::FAILED_PRECONDITION);
    extras.details = nlohmann::json::array({failure.to_json()});
    return extras;
}

const HostedProtocol* Server::find_protocol(const std::string& name) const noexcept {
    for (const auto& protocol : protocols_) {
        if (!protocol.name.empty() && protocol.name == name) return &protocol;
    }
    return nullptr;
}

namespace {

// Fingerprint one binding, or refuse to build a server that cannot state its
// own protocol hash.
//
// Thrown rather than deferred because the alternatives are both worse: an
// unhashable protocol answers `list_protocols` with an error on the first call
// a client makes, and leaves the access log with no digest at all for every
// record before it.  The only cause is an Arrow type with no canonical token,
// which is a property of the registration, known here.
std::string binding_hash_or_throw(const std::string& name,
                                  const std::unordered_map<std::string, MethodInfo>& methods) {
    auto hash = BindingHash(name, methods);
    if (!hash.ok()) {
        throw std::invalid_argument("Cannot compute the protocol hash for '" + name +
                                    "': " + hash.status().ToString());
    }
    return *hash;
}

}  // namespace

Server::Server(std::vector<HostedProtocol> protocols, std::string server_id,
               const std::string& access_log_path, int64_t access_log_max_record_bytes,
               std::function<void(TransportKind)> on_serve_start,
               std::shared_ptr<IdentityImpl> identity, bool include_tracebacks)
    : protocols_(std::move(protocols)),
      server_id_(std::move(server_id)),
      include_tracebacks_(include_tracebacks),
      identity_(std::move(identity)),
      on_serve_start_(std::move(on_serve_start)) {
    if (protocols_.empty()) protocols_.emplace_back();
    // A worker that declares a version it cannot parse would silently enforce
    // nothing, which is worse than not declaring one: the operator believes
    // there is a gate. Refuse to build such a server at all.
    for (const auto& protocol : protocols_) {
        if (!protocol.version.empty() && !parse_semver(protocol.version)) {
            throw std::invalid_argument(
                "Invalid protocol version '" + protocol.version + "' for protocol '" +
                protocol.name +
                "': expected canonical semver MAJOR.MINOR.PATCH with non-negative integers "
                "and no leading zeros (no prereleases or build metadata).");
        }
    }

    // Fingerprint every hosted binding once.  Reflection is fingerprinted over
    // the two methods it answers, exactly like any other protocol: they are
    // framework-owned rather than user-registered, but the table is what
    // `describe` reports and what the hash covers, so an empty one would be a
    // protocol lying about itself.  Identity narrows to the methods whose hooks
    // the deployment configured, so the hash a client compares narrows with
    // them.
    for (auto& protocol : protocols_) {
        protocol.binding = {protocol.name, binding_hash_or_throw(protocol.name, protocol.methods)};
    }
    reflection_binding_ = {kReflectionProtocolName,
                           binding_hash_or_throw(kReflectionProtocolName, ReflectionMethods())};
    if (identity_ != nullptr) {
        identity_methods_ = IdentityMethods(identity_->offered_methods());
    }
    if (!identity_methods_.empty()) {
        identity_binding_ = {kIdentityProtocolName,
                             binding_hash_or_throw(kIdentityProtocolName, identity_methods_)};
    }

    if (!access_log_path.empty()) {
        access_log_ = std::make_unique<AccessLogWriter>(access_log_path, server_id_,
                                                        access_log_max_record_bytes);
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
