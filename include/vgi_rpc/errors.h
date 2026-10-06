// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/// The error model: a canonical code, an open reason, and typed details.
///
/// Every EXCEPTION batch carries three layers, adopted from gRPC's
/// `google.rpc.Status` (WIRE_PROTOCOL.md §8):
///
///  - **Code**, `vgi_rpc.error_code`: one of gRPC's sixteen non-`OK` codes,
///    sent as its *name* (`"UNAVAILABLE"`).  Closed.  What generic handling --
///    retry or not, how to show it -- keys on.
///  - **Reason**, `vgi_rpc.error_kind`: open, unique within the protocol that
///    raised it.  What a client branches on.
///  - **Details**, `vgi_rpc.error_details`: a JSON array of typed objects from
///    a fixed catalog (`vgi_rpc.RetryInfo`, `vgi_rpc.BadRequest`, ...).  At
///    most 4 KiB serialized; over the cap the **whole** array is dropped,
///    because a client cannot tell a trimmed list from a complete one.
///
/// Ordinary handler exceptions map to an error_type by their C++ class
/// (invalid_argument -> ValueError and so on) and to the code `UNKNOWN`.
/// Exceptions whose class is part of the wire contract derive from
/// `KindedError`, which carries all three layers explicitly.
#pragma once

#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include <nlohmann/json.hpp>

#include "vgi_rpc/export.h"
#include "vgi_rpc/metadata.h"

namespace vgi_rpc {

// ---------------------------------------------------------------------------
// Codes
// ---------------------------------------------------------------------------

/// The closed set of canonical error codes: gRPC's sixteen, minus `OK`.
///
/// The wire value is the code's *name*, never its number, so a log line, a
/// proxy rule and a client switch all read the same string.
enum class Code {
    CANCELLED = 1,
    UNKNOWN = 2,
    INVALID_ARGUMENT = 3,
    DEADLINE_EXCEEDED = 4,
    NOT_FOUND = 5,
    ALREADY_EXISTS = 6,
    PERMISSION_DENIED = 7,
    RESOURCE_EXHAUSTED = 8,
    FAILED_PRECONDITION = 9,
    ABORTED = 10,
    OUT_OF_RANGE = 11,
    UNIMPLEMENTED = 12,
    INTERNAL = 13,
    UNAVAILABLE = 14,
    DATA_LOSS = 15,
    UNAUTHENTICATED = 16,
};

/// The wire name of `code` (`"UNAVAILABLE"`).
VGI_RPC_EXPORT const char* code_name(Code code) noexcept;

/// The code a wire value names, or nullopt when it names none of the sixteen.
VGI_RPC_EXPORT std::optional<Code> code_from_name(std::string_view name) noexcept;

/// Read a wire value the way a client must: anything unrecognised -- including
/// an absent code -- is `UNKNOWN`.
VGI_RPC_EXPORT Code parse_code(std::string_view name) noexcept;

/// The code a framework error with this `error_kind` and exception type
/// carries, per the table in WIRE_PROTOCOL.md §8 and §16.  `UNKNOWN` for
/// anything unclassified -- which is honest: the lesson of gRPC is not that
/// `UNKNOWN` is wrong but that services reached for it for errors they *had*
/// classified, so every kind this package defines names its code here.
VGI_RPC_EXPORT Code default_error_code(std::string_view exception_type,
                                       std::string_view error_kind) noexcept;

// ---------------------------------------------------------------------------
// The detail catalog
// ---------------------------------------------------------------------------

/// Extra context for the reason.  Reason and domain are already `error_kind`
/// and the protocol, so, unlike gRPC's, neither is repeated here.
struct VGI_RPC_EXPORT ErrorInfo {
    static constexpr const char* TYPE = "vgi_rpc.ErrorInfo";
    std::map<std::string, std::string> metadata;
    nlohmann::json to_json() const;
    static std::optional<ErrorInfo> from_json(const nlohmann::json& obj);
};

/// How long to wait before retrying.  Finite and non-negative.
struct VGI_RPC_EXPORT RetryInfo {
    static constexpr const char* TYPE = "vgi_rpc.RetryInfo";
    double retry_delay_seconds = 0.0;
    nlohmann::json to_json() const;
    static std::optional<RetryInfo> from_json(const nlohmann::json& obj);
};

struct FieldViolation {
    std::string field;
    std::string description;
};

/// Which inputs were wrong.
struct VGI_RPC_EXPORT BadRequest {
    static constexpr const char* TYPE = "vgi_rpc.BadRequest";
    std::vector<FieldViolation> field_violations;
    nlohmann::json to_json() const;
    static std::optional<BadRequest> from_json(const nlohmann::json& obj);
};

struct PreconditionViolation {
    std::string type;
    std::string subject;
    std::string description;
};

/// What state must change before the call can succeed.
struct VGI_RPC_EXPORT PreconditionFailure {
    static constexpr const char* TYPE = "vgi_rpc.PreconditionFailure";
    std::vector<PreconditionViolation> violations;
    nlohmann::json to_json() const;
    static std::optional<PreconditionFailure> from_json(const nlohmann::json& obj);
};

struct QuotaViolation {
    std::string subject;
    std::string description;
};

/// Which limit was hit.
struct VGI_RPC_EXPORT QuotaFailure {
    static constexpr const char* TYPE = "vgi_rpc.QuotaFailure";
    std::vector<QuotaViolation> violations;
    nlohmann::json to_json() const;
    static std::optional<QuotaFailure> from_json(const nlohmann::json& obj);
};

/// Which object the error concerns.
struct VGI_RPC_EXPORT ResourceInfo {
    static constexpr const char* TYPE = "vgi_rpc.ResourceInfo";
    std::string resource_type;
    std::string resource_name;
    std::string owner;
    std::string description;
    nlohmann::json to_json() const;
    static std::optional<ResourceInfo> from_json(const nlohmann::json& obj);
};

struct HelpLink {
    std::string description;
    std::string url;
};

/// Where to read more.
struct VGI_RPC_EXPORT Help {
    static constexpr const char* TYPE = "vgi_rpc.Help";
    std::vector<HelpLink> links;
    nlohmann::json to_json() const;
    static std::optional<Help> from_json(const nlohmann::json& obj);
};

/// Text that is safe to show an end user.  The error message stays
/// developer-facing English; this is the one place user-facing text belongs.
struct VGI_RPC_EXPORT LocalizedMessage {
    static constexpr const char* TYPE = "vgi_rpc.LocalizedMessage";
    std::string locale;
    std::string message;
    nlohmann::json to_json() const;
    static std::optional<LocalizedMessage> from_json(const nlohmann::json& obj);
};

/// Any member of the fixed catalog.
using ErrorDetail = std::variant<ErrorInfo, RetryInfo, BadRequest, PreconditionFailure,
                                 QuotaFailure, ResourceInfo, Help, LocalizedMessage>;

/// The JSON object form of a catalog detail, `@type` included.
VGI_RPC_EXPORT nlohmann::json detail_to_json(const ErrorDetail& detail);

/// Decode one detail object, or nullopt when it is not an object, names a type
/// outside the catalog, or is a catalog type with a malformed field.  The
/// error is the news and the detail is commentary, so a bad detail is treated
/// as absent rather than failing the error it rides on.
VGI_RPC_EXPORT std::optional<ErrorDetail> parse_error_detail(const nlohmann::json& obj);

/// Cap on the serialized `vgi_rpc.error_details` value, in UTF-8 bytes.
inline constexpr size_t kMaxErrorDetailsBytes = 4096;

/// Whether a detail array obeys the catalog rules: every element an object
/// naming a qualified `@type`, each type at most once, and no invented type in
/// the reserved `vgi_rpc.` space.
VGI_RPC_EXPORT bool error_details_valid(const nlohmann::json& details);

/// Serialize a detail array for `vgi_rpc.error_details`, enforcing the rules.
///
/// nullopt means *omit the key*: an empty array, an array that breaks a rule,
/// and one whose compact serialization exceeds `kMaxErrorDetailsBytes`.  The
/// array is dropped whole, never trimmed.
VGI_RPC_EXPORT std::optional<std::string> encode_error_details(const nlohmann::json& details);

/// Decode a `vgi_rpc.error_details` value.  Tolerant by design: anything that
/// is not a JSON array decodes as empty, and non-object elements are skipped.
/// Unknown `@type`s are kept -- filtering is what typed access does.
VGI_RPC_EXPORT nlohmann::json decode_error_details(std::string_view raw);

/// Whether WIRE_PROTOCOL.md §8 calls an error retryable: `UNAVAILABLE` always,
/// `RESOURCE_EXHAUSTED` only when it carries `RetryInfo`, nothing else.
/// A classification, not a policy: nothing in this library retries an RPC
/// error automatically, because a method may not be idempotent.
VGI_RPC_EXPORT bool is_retryable(std::string_view code, const nlohmann::json& details);

// ---------------------------------------------------------------------------
// Server side: exceptions that carry the model
// ---------------------------------------------------------------------------

/// Base for exceptions whose class is part of the wire contract.
///
/// Carries all three layers.  `code` defaults to the framework table's answer
/// for `kind` (`UNKNOWN` for a kind the table does not know), so a subclass
/// naming a framework kind cannot forget its code.
class VGI_RPC_EXPORT KindedError : public std::runtime_error {
public:
    KindedError(std::string kind, std::string exception_type, const std::string& what)
        : KindedError(kind, exception_type, what, default_error_code(exception_type, kind)) {}

    KindedError(std::string kind, std::string exception_type, const std::string& what, Code code,
                nlohmann::json details = nlohmann::json::array())
        : std::runtime_error(what),
          kind_(std::move(kind)),
          exception_type_(std::move(exception_type)),
          code_(code),
          details_(details.is_array() ? std::move(details) : nlohmann::json::array()) {}

    const std::string& kind() const noexcept { return kind_; }
    const std::string& exception_type() const noexcept { return exception_type_; }
    Code code() const noexcept { return code_; }
    /// The detail objects, as JSON, in order.
    const nlohmann::json& details() const noexcept { return details_; }

protected:
    void set_details(nlohmann::json details) { details_ = std::move(details); }

private:
    std::string kind_;
    std::string exception_type_;
    Code code_;
    nlohmann::json details_;
};

/// An application error carrying the full error model.
///
/// Throw it from a method body to choose the code, the reason and the details
/// a client sees:
///
///     throw StatusError("report is being rebuilt", Code::UNAVAILABLE,
///                       "report_rebuilding", {RetryInfo{30}});
///
/// The details are validated eagerly -- a repeated type, or an invented
/// `vgi_rpc.*` type, throws `std::invalid_argument` here -- so the mistake
/// fails in the code that made it rather than being silently dropped on the
/// way out.  Protocol-defined detail types (named under the protocol's own
/// name) go through the `nlohmann::json` constructor.
class VGI_RPC_EXPORT StatusError : public KindedError {
public:
    StatusError(const std::string& message, Code code, std::string kind = "",
                std::vector<ErrorDetail> details = {});
    StatusError(const std::string& message, Code code, std::string kind, nlohmann::json details);
};

/// An authenticator could not answer.  Not a rejection.
///
/// This port's transport-auth "could not find out" error: the HTTP transport
/// answers it with `503` and `Retry-After`, where any other authentication
/// failure is a `401`.  "The credential is bad" and "I could not find out
/// whether it is bad" are different answers; collapsing them turns a sidecar
/// blip into a fleet-wide re-login.
///
/// Deliberately not a `std::invalid_argument` (which `exception_type_of`
/// maps to `ValueError`, read by callers as "do not retry").
///
/// Thrown from a `vgi_rpc.Identity.v1` hook (`resolve_token` / `mint_grant`)
/// it is translated to `identity_unavailable` with the **same** retry hint
/// (WIRE_PROTOCOL.md §16), on every transport.  Thrown anywhere else it still
/// reaches the wire as `UNAVAILABLE` with `RetryInfo`.
class VGI_RPC_EXPORT AuthUnavailableError : public KindedError {
public:
    explicit AuthUnavailableError(const std::string& detail = "", int retry_after = 5)
        : AuthUnavailableError("AuthUnavailableError", detail, retry_after) {}

    /// Seconds to advertise in `Retry-After` / `RetryInfo`.
    int retry_after() const noexcept { return retry_after_; }
    const std::string& detail() const noexcept { return detail_; }

protected:
    AuthUnavailableError(std::string exception_type, const std::string& detail, int retry_after)
        : KindedError(
              "", std::move(exception_type),
              detail.empty() ? "authentication service unavailable" : detail, Code::UNAVAILABLE,
              nlohmann::json::array({RetryInfo{static_cast<double>(retry_after)}.to_json()})),
          detail_(detail),
          retry_after_(retry_after) {}

private:
    std::string detail_;
    int retry_after_;
};

// Map a handler exception onto the error_type the wire carries.  Ordering
// matters: invalid_argument and out_of_range are both logic_error subclasses,
// so the specific cases must be tested before the general one.
VGI_RPC_EXPORT std::string exception_type_of(const std::exception& e);

// The machine-readable error_kind, or "" for an ordinary exception.
VGI_RPC_EXPORT std::string error_kind_of(const std::exception& e);

// The canonical code an exception declares; `UNKNOWN` for an ordinary one.
VGI_RPC_EXPORT Code error_code_of(const std::exception& e);

// The detail objects an exception declares, or an empty array.
VGI_RPC_EXPORT nlohmann::json error_details_of(const std::exception& e);

// The worker violated or received an invalid wire-level RPC contract.
class VGI_RPC_EXPORT ProtocolError : public KindedError {
public:
    explicit ProtocolError(const std::string& what) : KindedError("", "ProtocolError", what) {}
};

// A sticky session could not be resolved.  Every cause — a token that will not
// decrypt, one sealed for another principal, one minted by another worker, an
// entry that aged out — raises this same error with this same message, so the
// endpoint cannot be used to probe whose sessions exist.
class VGI_RPC_EXPORT SessionLostError : public KindedError {
public:
    explicit SessionLostError(const std::string& what = "session lost")
        : KindedError(ERROR_KIND_SESSION_LOST, "SessionLostError", what) {}
};

// The worker is draining and will not open new sessions.  Distinct from
// session_lost: the client's token is fine, the server is going away.  Carries
// `RetryInfo`: a retry is usually routed to a worker that is not draining.
class VGI_RPC_EXPORT ServerDrainingError : public KindedError {
public:
    explicit ServerDrainingError(const std::string& what = "server draining",
                                 double retry_after = 1.0)
        : KindedError(ERROR_KIND_SERVER_DRAINING, "ServerDrainingError", what, Code::UNAVAILABLE,
                      nlohmann::json::array({RetryInfo{retry_after}.to_json()})) {}
};

// ---------------------------------------------------------------------------
// What an EXCEPTION batch carries beyond its type and message
// ---------------------------------------------------------------------------

/// Optional parts of an error batch.  `code` empty means "the framework
/// table's code for the batch's type and kind".  `traceback` empty means
/// "none from this site": inside a server dispatch whose traceback setting is
/// on (the default), `make_error_metadata` synthesizes one, because every
/// EXCEPTION batch then carries a non-empty traceback (WIRE_PROTOCOL.md §8).
struct ErrorExtras {
    std::string code;
    nlohmann::json details = nlohmann::json::array();
    std::string traceback;
};

/// The extras an exception declares, plus `traceback`.
VGI_RPC_EXPORT ErrorExtras error_extras_of(const std::exception& e, std::string traceback = "");

/// A traceback for `e`, for the transports that send one.
///
/// C++ exceptions do not capture a stack, so this is the closest honest
/// substitute: where the error was raised (`where`, e.g. `Protocol/method`),
/// then `Type: message`, in the shape a Python traceback ends with.  It names
/// no files and no values beyond the message the error already carries.
VGI_RPC_EXPORT std::string cpp_traceback(const std::string& exception_type,
                                         const std::string& message, const std::string& where);

// ---------------------------------------------------------------------------
// Client side: the model as decoded off an EXCEPTION batch
// ---------------------------------------------------------------------------

/// The error model of a remote error, as the client decoded it.
///
/// Mixed into every client exception that represents a remote EXCEPTION batch
/// (`RpcException`, `RpcRemoteError`) and filled on every decode path.
///
///  - `error_code()`: the code's name, or `""` when the server sent none (a
///    server older than the model).  `""` and `"UNKNOWN"` are different
///    answers; `code()` reads both as `Code::UNKNOWN`.
///  - `error_kind()`: the reason, or `""`.
///  - `error_details()`: the array as received -- every element, in order,
///    unknown types included.  The typed accessors skip what they do not know.
///
/// `is_retryable()` classifies; nothing retries.
class VGI_RPC_EXPORT RemoteStatus {
public:
    RemoteStatus() = default;
    RemoteStatus(std::string error_code, std::string error_kind, nlohmann::json error_details,
                 std::string remote_traceback = "");
    virtual ~RemoteStatus() = default;

    const std::string& error_code() const noexcept { return error_code_; }
    const std::string& error_kind() const noexcept { return error_kind_; }
    const nlohmann::json& error_details() const noexcept { return error_details_; }
    const std::string& remote_traceback() const noexcept { return remote_traceback_; }

    Code code() const noexcept { return parse_code(error_code_); }
    bool is_retryable() const { return vgi_rpc::is_retryable(error_code_, error_details_); }

    /// The catalog details this client understands, in wire order.
    std::vector<ErrorDetail> details() const;
    std::optional<ErrorInfo> error_info() const;
    std::optional<RetryInfo> retry_info() const;
    std::optional<BadRequest> bad_request() const;
    std::optional<PreconditionFailure> precondition_failure() const;
    std::optional<QuotaFailure> quota_failure() const;
    std::optional<ResourceInfo> resource_info() const;
    std::optional<Help> help() const;
    std::optional<LocalizedMessage> localized_message() const;

private:
    template <typename T>
    std::optional<T> first() const;

    std::string error_code_;
    std::string error_kind_;
    nlohmann::json error_details_ = nlohmann::json::array();
    std::string remote_traceback_;
};

/// Read the model off an EXCEPTION batch's metadata.  Top-level keys are
/// canonical; the `log_extra` mirror (`extra`, the decoded object) is the
/// fallback for each one that is absent.  `traceback` comes from
/// `log_extra.traceback`.
VGI_RPC_EXPORT RemoteStatus decode_remote_status(const arrow::KeyValueMetadata* metadata,
                                                 const nlohmann::json& extra);

}  // namespace vgi_rpc
