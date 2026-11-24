// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#pragma once

// Protocol version negotiation (vgi-rpc PROTOCOL_VERSIONS_V1.md).
//
// A server advertises the major.minor window of every protocol it gates as
// `name=MIN-MAX[, name=MIN-MAX]...`: on every HTTP response
// (`VGI-Protocol-Versions`), and as the `vgi_rpc.protocol_versions` custom
// metadata key on reflection results, `__transport_options__` replies and
// refusals, on every transport.  A client learns the windows per endpoint,
// speaks the highest common version, and advertises its own window as
// `vgi_rpc.protocol_versions_client` (and `VGI-Client-Protocol-Versions` on
// HTTP), which a server only logs and quotes.

#include <map>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "vgi_rpc/errors.h"
#include "vgi_rpc/export.h"

namespace vgi_rpc {

/// HTTP response header carrying the server's windows.
inline constexpr const char* PROTOCOL_VERSIONS_HEADER = "VGI-Protocol-Versions";
/// HTTP request header carrying the client's own windows (informational).
inline constexpr const char* CLIENT_PROTOCOL_VERSIONS_HEADER = "VGI-Client-Protocol-Versions";
/// The error kind of a version refusal.
inline constexpr const char* ERROR_KIND_PROTOCOL_VERSION_UNSUPPORTED =
    "protocol_version_unsupported";

namespace keys {
/// The server's windows: reflection results, transport options, refusals.
inline constexpr const char* PROTOCOL_VERSIONS = "vgi_rpc.protocol_versions";
/// The client's own windows, on every request (informational).
inline constexpr const char* PROTOCOL_VERSIONS_CLIENT = "vgi_rpc.protocol_versions_client";
}  // namespace keys

using MajorMinor = std::pair<int, int>;

/// An inclusive window of major.minor versions.
struct VGI_RPC_EXPORT VersionRange {
    MajorMinor min{0, 0};
    MajorMinor max{0, 0};

    /// `MIN-MAX` (each `MAJOR.MINOR`, a `MAJOR.MINOR.PATCH` end drops its
    /// patch) or a single version meaning `V-V`.  Throws
    /// `std::invalid_argument` on anything else or an inverted window.
    static VersionRange parse(const std::string& text);
    /// The single-version window of a semver's major.minor.
    static VersionRange exactly(const std::string& version);

    bool contains(MajorMinor version) const noexcept { return min <= version && version <= max; }
    /// Wire form, always both ends: `3.4-3.4`.
    std::string to_string() const;

    bool operator==(const VersionRange& other) const noexcept {
        return min == other.min && max == other.max;
    }
};

/// `M.m`.
VGI_RPC_EXPORT std::string format_major_minor(MajorMinor version);

/// Parse `MAJOR.MINOR[.PATCH]` into its major.minor.  Throws
/// `std::invalid_argument` when it is not canonical.
VGI_RPC_EXPORT MajorMinor parse_major_minor(const std::string& text);

/// Render an advertisement in the given order.
VGI_RPC_EXPORT std::string format_versions(
    const std::vector<std::pair<std::string, VersionRange>>& ranges);

/// Parse an advertisement.  Tolerant: entries that do not parse are skipped,
/// and a repeated name keeps its first window.
VGI_RPC_EXPORT std::map<std::string, VersionRange> parse_versions(const std::string& text);

/// The highest major.minor inside both windows, or nullopt when disjoint.
VGI_RPC_EXPORT std::optional<MajorMinor> highest_common(const VersionRange& a,
                                                        const VersionRange& b);

/// What a client bound to one protocol speaks.
struct VGI_RPC_EXPORT ClientVersioning {
    std::string protocol;
    /// The newest version spoken (canonical semver); its patch rides along
    /// with whatever major.minor is negotiated.
    std::string version;
    /// The supported window; its max is `version`'s major.minor.
    VersionRange range;

    /// `version` with an optional `MIN-MAX` window (empty means exactly
    /// `version`).  Throws `std::invalid_argument` when the window is
    /// malformed or does not end at `version`'s major.minor.
    static ClientVersioning make(std::string protocol, std::string version,
                                 const std::string& version_range = "");

    /// `MAJOR.MINOR.PATCH` to send for a negotiated major.minor.
    std::string wire_version(MajorMinor negotiated) const;
    /// The client's `vgi_rpc.protocol_versions_client` value.
    std::string advertised() const;
};

/// The client found no version both sides speak; raised before sending.  Its
/// kind is `protocol_version_unsupported`, the same as a server refusal.
class VGI_RPC_EXPORT NoCommonVersionError : public std::runtime_error {
public:
    NoCommonVersionError(const std::string& protocol, const VersionRange& client_range,
                         const VersionRange& server_range, const std::string& endpoint = "");

    const char* error_kind() const noexcept { return ERROR_KIND_PROTOCOL_VERSION_UNSUPPORTED; }
    const VersionRange& client_range() const noexcept { return client_range_; }
    const VersionRange& server_range() const noexcept { return server_range_; }

private:
    VersionRange client_range_;
    VersionRange server_range_;
};

/// Whether `kind` is a version refusal (this spec's, or the pre-spec one).
VGI_RPC_EXPORT bool is_version_refusal_kind(const std::string& kind) noexcept;

/// Negotiation state for one endpoint (an HTTP base URL, a connection).
/// Thread-safe.
class VGI_RPC_EXPORT EndpointNegotiation {
public:
    explicit EndpointNegotiation(std::string endpoint = "") : endpoint_(std::move(endpoint)) {}

    /// Record an advertisement from a response (header or metadata).  A
    /// present value replaces what was learned.
    void observe(const std::string& value);
    /// Learn from a refusal: its `ErrorInfo` windows, else the single server
    /// version a pre-spec refusal names (`Server: X.Y.Z`).
    void observe_refusal(const std::string& protocol, const std::string& error_kind,
                         const std::string& message, const RemoteStatus& status);
    /// The major.minor to speak.  Throws `NoCommonVersionError`.
    MajorMinor choose(const ClientVersioning& client) const;
    /// `choose` plus the client's patch.
    std::string wire_version(const ClientVersioning& client) const {
        return client.wire_version(choose(client));
    }
    bool has_advertisement() const;
    std::map<std::string, VersionRange> advertised() const;

private:
    std::string endpoint_;
    mutable std::mutex mutex_;
    std::map<std::string, VersionRange> advertised_;
    std::map<std::string, MajorMinor> legacy_;
    bool seen_ = false;
};

}  // namespace vgi_rpc
