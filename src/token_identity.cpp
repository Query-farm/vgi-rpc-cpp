// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// `vgi_rpc.Identity.v1` -- see include/vgi_rpc/token_identity.h for why the two
// methods are guarded differently, and why the guard *order* in
// `introspect_token` is not a matter of taste.

#include "vgi_rpc/token_identity.h"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <stdexcept>
#include <string_view>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include "vgi_rpc/arrow_utils.h"
#include "vgi_rpc/crypto.h"
#include "vgi_rpc/metadata.h"
#include "vgi_rpc/request.h"
#include "vgi_rpc/result.h"
#include "vgi_rpc/wire.h"

#include "request_contract.h"

namespace vgi_rpc {
namespace {

std::shared_ptr<arrow::Field> Utf8Field(const std::string& name) {
    return arrow::field(name, arrow::utf8(), /*nullable=*/false);
}

/// Serialize one batch as a complete IPC stream.
///
/// The framework's ordinary convention for a structured return: the payload
/// rides as serialized bytes in a single `result` binary column, so the
/// protocol's own hash is the same three fields in every port regardless of
/// what the payload dataclass grows later.
std::string BatchToIpc(const std::shared_ptr<arrow::RecordBatch>& batch) {
    auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto writer = arrow::ipc::MakeStreamWriter(sink, batch->schema()).ValueOrDie();
    VGI_RPC_THROW_NOT_OK(writer->WriteRecordBatch(*batch));
    VGI_RPC_THROW_NOT_OK(writer->Close());
    auto buffer = sink->Finish().ValueOrDie();
    return std::string(reinterpret_cast<const char*>(buffer->data()),
                       static_cast<size_t>(buffer->size()));
}

std::shared_ptr<arrow::Array> OneString(const std::string& value) {
    arrow::StringBuilder builder;
    VGI_RPC_THROW_NOT_OK(builder.Append(value));
    std::shared_ptr<arrow::Array> out;
    VGI_RPC_THROW_NOT_OK(builder.Finish(&out));
    return out;
}

std::shared_ptr<arrow::Array> OneInt64(int64_t value) {
    arrow::Int64Builder builder;
    VGI_RPC_THROW_NOT_OK(builder.Append(value));
    std::shared_ptr<arrow::Array> out;
    VGI_RPC_THROW_NOT_OK(builder.Finish(&out));
    return out;
}

std::shared_ptr<arrow::Array> OneFloat64(double value) {
    arrow::DoubleBuilder builder;
    VGI_RPC_THROW_NOT_OK(builder.Append(value));
    std::shared_ptr<arrow::Array> out;
    VGI_RPC_THROW_NOT_OK(builder.Finish(&out));
    return out;
}

/// The byte width of the whitespace codepoint beginning at `pos`, or 0.
///
/// The floor `IDENTITY_V1_SPEC.md` requires every port to trim:
///
///     U+0009 U+000A U+000B U+000C U+000D U+0020 U+0085 U+00A0
///
/// It is a *floor* and not a set because trimming wider can only add refusals,
/// never remove one -- but trimming narrower is the hole this guard exists to
/// close, one level down: a port that stops at ASCII routes a NBSP-padded JWS
/// onward that a port trimming the full set refuses.  Divergence between ports
/// is the bug, so the set is enumerated rather than delegated to `isspace`,
/// whose answer depends on the active locale.
///
/// The credential is a UTF-8 `std::string`, so two of the eight are two bytes
/// each -- U+0085 is `C2 85` and U+00A0 is `C2 A0`.  Comparing `char`s would
/// silently trim neither.  A lone `85` or `A0` byte is a continuation of some
/// other codepoint and is left alone.
size_t WhitespaceWidth(const std::string& token, size_t pos) {
    const auto byte = static_cast<unsigned char>(token[pos]);
    if (byte == 0x20 || (byte >= 0x09 && byte <= 0x0D)) return 1;
    if (byte == 0xC2 && pos + 1 < token.size()) {
        const auto next = static_cast<unsigned char>(token[pos + 1]);
        if (next == 0x85 || next == 0xA0) return 2;
    }
    return 0;
}

/// The byte width of the whitespace codepoint *ending* at `end`, or 0.
size_t TrailingWhitespaceWidth(const std::string& token, size_t begin, size_t end) {
    if (end - begin >= 2 && WhitespaceWidth(token, end - 2) == 2) return 2;
    const auto byte = static_cast<unsigned char>(token[end - 1]);
    return (byte == 0x20 || (byte >= 0x09 && byte <= 0x0D)) ? 1 : 0;
}

/// Strip leading and trailing whitespace, without copying.
///
/// A credential is not text to be normalised -- this view exists only so the
/// shape test below can be run against it.
std::string_view Trimmed(const std::string& token) {
    size_t begin = 0;
    while (begin < token.size()) {
        const size_t width = WhitespaceWidth(token, begin);
        if (width == 0) break;
        begin += width;
    }
    size_t end = token.size();
    while (end > begin) {
        const size_t width = TrailingWhitespaceWidth(token, begin, end);
        if (width == 0) break;
        end -= width;
    }
    return std::string_view(token).substr(begin, end - begin);
}

/// Whether `candidate` looks like a JWS: three dot-separated base64url
/// segments, the third possibly empty (an unsecured JWT).
///
/// Hand-rolled rather than `std::regex`.  The input is an attacker-supplied
/// credential, and libstdc++/libc++ implement `std::regex` with recursive
/// backtracking that has blown the stack on adversarial input before; a linear
/// scan cannot.  It also keeps the pattern readable next to the contract, which
/// pins it as `^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*$`.
///
/// Strictly anchored, with no special case for a trailing newline.  Anchor
/// semantics are the least portable corner of seven regex dialects -- Python's
/// `$` matches before one trailing newline and not two, Go's `\A..\z` and
/// JavaScript's unflagged `$` match neither -- so the padding question is
/// settled by the caller trimming first rather than by anything spelled here.
bool IsJwsShaped(std::string_view view) {
    auto is_b64url = [](char c) {
        return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
               c == '_' || c == '-';
    };

    size_t offset = 0;
    // Two non-empty segments, each followed by a '.'.
    for (int segment = 0; segment < 2; ++segment) {
        const size_t start = offset;
        while (offset < view.size() && is_b64url(view[offset])) ++offset;
        if (offset == start) return false;
        if (offset >= view.size() || view[offset] != '.') return false;
        ++offset;
    }
    // A possibly-empty third segment, and nothing after it.
    while (offset < view.size() && is_b64url(view[offset])) ++offset;
    return offset == view.size();
}

/// Seconds since the Unix epoch, for the freshness ceiling.
///
/// Wall clock on purpose: `auth_time` is an absolute OIDC claim, so comparing
/// it against a monotonic reading would be comparing two unrelated origins.
double WallSeconds() {
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    return std::chrono::duration<double>(now).count();
}

}  // namespace

std::string token_digest(const std::string& token) {
    const auto digest =
        crypto::sha256(reinterpret_cast<const uint8_t*>(token.data()), token.size());
    return crypto::hex_encode(digest.data(), digest.size());
}

// Guards

std::set<std::string> normalise_principals(const std::vector<std::string>& principals) {
    std::set<std::string> allowed;
    for (const auto& p : principals) {
        if (!p.empty()) allowed.insert(p);
    }
    if (allowed.empty()) {
        throw std::invalid_argument(
            "introspect_principals must name at least one principal. "
            "Introspection is a distinct capability from authentication: "
            "allowing any authenticated caller lets any user resolve any "
            "other user's credential to its owner.");
    }
    return allowed;
}

std::string check_introspector(const AuthContext& auth, const std::set<std::string>& principals) {
    const std::string caller = auth.principal.value_or("");
    if (!auth.authenticated || principals.find(caller) == principals.end()) {
        throw IntrospectionRefusedError("caller is not an introspector");
    }
    return caller;
}

void reject_jws_shaped(const std::string& token) {
    // The shape test runs against the *trimmed* credential; the resolver still
    // receives what the caller actually sent.  Trimming here can only add
    // refusals, never remove one, and it closes a padding bypass that would
    // otherwise be spelled differently in every port: without it "a.b.c\n" is
    // not JWS-shaped to a strict matcher and gets routed onward, which is
    // precisely what this guard exists to stop.
    //
    // A whitespace-only credential is refused for the same reason an empty one
    // is: it is not a credential.
    //
    // The length cap stays on the *original*, because the thing being bounded
    // is what a resolver would be handed, not what is left after trimming.
    const std::string_view candidate = Trimmed(token);
    if (candidate.empty() || token.size() > kMaxTokenBytes || IsJwsShaped(candidate)) {
        throw TokenUnresolvedError("unresolved");
    }
}

double check_freshness(const AuthContext& auth, double max_auth_age, std::optional<double> now) {
    if (!auth.authenticated || !auth.principal.has_value() || auth.principal->empty()) {
        throw StaleAuthError("caller is not authenticated");
    }
    const auto claim = auth.claims.find("auth_time");
    if (claim == auth.claims.end() || claim->is_null()) {
        throw StaleAuthError(
            "credential carries no auth_time; only a recently authenticated user may mint a "
            "grant");
    }

    double auth_time = 0.0;
    if (claim->is_number()) {
        auth_time = claim->get<double>();
    } else if (claim->is_string()) {
        // A JSON-typed claim set is a convenience, not a guarantee: an IdP that
        // stringifies numbers must not be read as "no auth_time at all", which
        // is a different (and differently actionable) refusal.
        const std::string raw = claim->get<std::string>();
        try {
            size_t consumed = 0;
            auth_time = std::stod(raw, &consumed);
            if (consumed != raw.size()) throw std::invalid_argument("trailing characters");
        } catch (const std::exception&) {
            throw StaleAuthError("credential carries an unusable auth_time");
        }
    } else {
        throw StaleAuthError("credential carries an unusable auth_time");
    }

    const double age = (now.has_value() ? *now : WallSeconds()) - auth_time;
    if (age > max_auth_age) {
        char message[256];
        std::snprintf(message, sizeof(message),
                      "last authentication was %.0fs ago, which exceeds the %.0fs ceiling for "
                      "minting a grant; re-authenticate",
                      age, max_auth_age);
        throw StaleAuthError(message);
    }
    return auth_time;
}

// Payloads

const std::shared_ptr<arrow::Schema>& TokenIdentity::arrow_schema() {
    // Declaration order is the schema, and the schema is part of the protocol
    // hash.  Do not sort these for tidiness.
    static const auto schema =
        arrow::schema({Utf8Field("principal"), Utf8Field("token_name"),
                       arrow::field("ttl_seconds", arrow::int64(), /*nullable=*/false)});
    return schema;
}

std::string TokenIdentity::serialize_to_bytes() const {
    auto batch = arrow::RecordBatch::Make(
        arrow_schema(), 1, {OneString(principal), OneString(token_name), OneInt64(ttl_seconds)});
    return BatchToIpc(batch);
}

const std::shared_ptr<arrow::Schema>& IssuedGrant::arrow_schema() {
    static const auto schema = arrow::schema(
        {Utf8Field("token"), arrow::field("expires_at", arrow::float64(), /*nullable=*/false),
         Utf8Field("grant_id")});
    return schema;
}

std::string IssuedGrant::serialize_to_bytes() const {
    auto batch = arrow::RecordBatch::Make(
        arrow_schema(), 1, {OneString(token), OneFloat64(expires_at), OneString(grant_id)});
    return BatchToIpc(batch);
}

// IdentityImpl

IdentityImpl::IdentityImpl(IdentityOptions options)
    : resolve_token_(std::move(options.resolve_token)),
      mint_grant_(std::move(options.mint_grant)),
      max_auth_age_(options.max_auth_age) {
    // Validated at construction, not at first call: a worker that would refuse
    // every introspection should fail to start rather than serve traffic until
    // someone tries.
    if (resolve_token_) principals_ = normalise_principals(options.introspect_principals);
}

std::set<std::string> IdentityImpl::offered_methods() const {
    std::set<std::string> offered;
    if (resolve_token_) offered.insert("introspect_token");
    if (mint_grant_) offered.insert("issue_grant");
    return offered;
}

TokenIdentity IdentityImpl::introspect_token(const std::string& token, const AuthContext& auth) {
    // The belt to method-level narrowing's braces: the method is not hosted
    // when the hook is absent, and refuses for a caller that reached it anyway.
    if (!resolve_token_) {
        throw IntrospectionRefusedError("this worker does not resolve credentials");
    }

    // Authorization first, and only then anything that looks at the subject
    // credential.  An unauthorized caller must learn nothing about it --
    // including how long looking at it took, which is why the cheap syntactic
    // checks do not get hoisted above it for tidiness.  Reordering this is a
    // timing oracle, not a style preference.
    //
    // There is no rate limit.  The allowlist is the control: a per-caller
    // budget is one budget for every user behind the asker, and junk
    // credentials from unauthenticated clients drain it.
    (void)check_introspector(auth, principals_);
    reject_jws_shaped(token);

    auto identity = resolve_token_(token);
    if (!identity.has_value()) {
        // Uniform with malformed and expired: reporting which would confirm
        // that a guessed credential exists.
        throw TokenUnresolvedError("unresolved");
    }
    return *identity;
}

IssuedGrant IdentityImpl::issue_grant(const std::string& purpose,
                                      const std::vector<std::string>& scopes, int64_t ttl_seconds,
                                      const AuthContext& auth) {
    if (!mint_grant_) throw GrantRefusedError("this worker does not mint grants");
    check_freshness(auth, max_auth_age_);
    // The subject is the caller, never a parameter: cross-subject minting is
    // closed by construction rather than by a check that could be forgotten in
    // one of several ports.
    return mint_grant_(auth.principal.value_or(""), purpose, scopes, ttl_seconds);
}

// The protocol surface

std::unordered_map<std::string, MethodInfo> IdentityMethods(const std::set<std::string>& offered) {
    // The single `result: binary` column every structured return in this
    // framework uses.  Shared by both methods, so the payload dataclasses can
    // evolve without moving the protocol hash.
    static const auto result_schema =
        arrow::schema({arrow::field("result", arrow::binary(), /*nullable=*/false)});

    std::unordered_map<std::string, MethodInfo> methods;

    if (offered.count("introspect_token") != 0) {
        MethodInfo info;
        info.name = "introspect_token";
        info.method_type = MethodType::UNARY;
        info.params_schema = arrow::schema({Utf8Field("token")});
        info.result_schema = result_schema;
        info.has_return = true;
        info.doc = "Resolve an opaque bearer credential to the identity it authenticates as.";
        methods[info.name] = std::move(info);
    }

    if (offered.count("issue_grant") != 0) {
        MethodInfo info;
        info.name = "issue_grant";
        info.method_type = MethodType::UNARY;
        // The list item is nullable.  That is part of the type token
        // (`list<item?:utf8>`) and therefore of the hash -- getting it wrong is
        // the single most likely way to end up with a protocol that no other
        // port agrees with.
        info.params_schema = arrow::schema(
            {Utf8Field("purpose"),
             arrow::field("scopes",
                          arrow::list(arrow::field("item", arrow::utf8(), /*nullable=*/true)),
                          /*nullable=*/false),
             arrow::field("ttl_seconds", arrow::int64(), /*nullable=*/false)});
        info.result_schema = result_schema;
        info.has_return = true;
        info.doc = "Mint a standing delegation credential for the calling user.";
        methods[info.name] = std::move(info);
    }

    return methods;
}

/// Serve one call to `vgi_rpc.Identity.v1`.
///
/// Routed by the same protocol key as every other co-hosted protocol -- this is
/// not a reserved method name bolted onto the application's table, which is
/// what the retired `POST {prefix}/__introspect_token__` route was and why it
/// existed on exactly one transport.
///
/// Method-level narrowing is enforced here as well as in the hash: a method
/// whose hook the deployment did not configure is answered as *absent*, not as
/// refused, so what the server hosts describes what it actually does and a
/// client learns it from reflection rather than by calling.
bool Server::serve_identity(const std::shared_ptr<arrow::io::OutputStream>& output,
                            const std::string& method_name,
                            const std::shared_ptr<arrow::RecordBatch>& request_batch,
                            const std::string& request_id, const AuthContext& auth, bool* errored) {
    const auto t0 = std::chrono::steady_clock::now();
    if (errored != nullptr) *errored = false;
    // Identity is a protocol in its own right, so its calls are logged under
    // its own name and digest.  A record filed under the application's protocol
    // would merge credential resolution into application traffic and, worse,
    // carry a digest that decodes against the wrong description.
    //
    // `identity_binding_` is empty exactly when no implementation was
    // configured -- the protocol is then absent rather than hosted -- and that
    // branch is refused below without a record, because there is no binding to
    // name one after.
    auto fail = [&](const std::string& type, const std::string& message,
                    const std::string& kind = "") {
        if (errored != nullptr) *errored = true;
        auto err = Result::error(empty_schema(), type, message, server_id_, request_id, kind);
        write_ipc_stream(output, empty_schema(), {err.annotated_batch()});
        VGI_RPC_THROW_NOT_OK(output->Flush());
        if (!identity_binding_.name.empty()) {
            log_framework_call(identity_binding_, method_name, request_id, request_batch, t0, type,
                               message);
        }
        return true;
    };

    if (identity_ == nullptr) {
        // Not registered at all when neither hook is configured.  Absent beats
        // routed-and-refusing: it is what keeps a dependency upgrade from
        // growing a credential-to-identity oracle on every existing worker.
        return fail("RuntimeError", std::string("This server does not host protocol '") +
                                        kIdentityProtocolName + "'.");
    }

    const auto offered = identity_->offered_methods();
    if (offered.count(method_name) == 0) {
        std::string available;
        for (const auto& name : offered) {
            if (!available.empty()) available += ", ";
            available += "'" + name + "'";
        }
        return fail("AttributeError", std::string("Protocol '") + kIdentityProtocolName +
                                          "' has no method '" + method_name + "'. Available: [" +
                                          available + "]");
    }

    const auto& info = identity_methods_.at(method_name);
    if (request_batch == nullptr) {
        return fail("ProtocolError", "Identity request carried no parameter batch.");
    }
    if (const std::string error = parameter_contract_error(request_batch, info.params_schema);
        !error.empty()) {
        return fail("ProtocolError", error);
    }

    std::string payload;
    try {
        Request request(request_batch, nullptr);
        if (method_name == "introspect_token") {
            payload = identity_->introspect_token(request.get<std::string>("token"), auth)
                          .serialize_to_bytes();
        } else {
            payload = identity_
                          ->issue_grant(request.get<std::string>("purpose"),
                                        request.get<std::vector<std::string>>("scopes"),
                                        request.get<int64_t>("ttl_seconds"), auth)
                          .serialize_to_bytes();
        }
    } catch (const std::exception& e) {
        // `error_kind` is the whole definitive-vs-transient signal now that
        // these are protocol methods rather than HTTP statuses: a caller that
        // negative-caches a transient failure locks out valid users, and one
        // that retries a definitive rejection hammers the worker.
        return fail(exception_type_of(e), e.what(), error_kind_of(e));
    }

    arrow::BinaryBuilder builder;
    VGI_RPC_THROW_NOT_OK(builder.Append(payload));
    std::shared_ptr<arrow::Array> column;
    VGI_RPC_THROW_NOT_OK(builder.Finish(&column));
    auto batch = arrow::RecordBatch::Make(info.result_schema, 1, {column});
    auto result = Result::value(batch);
    write_ipc_stream(output, info.result_schema, {result.annotated_batch()});
    VGI_RPC_THROW_NOT_OK(output->Flush());
    log_framework_call(identity_binding_, method_name, request_id, request_batch, t0, "", "");
    return true;
}

}  // namespace vgi_rpc
