// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/// Per-request context passed to method handlers and stream processors.
/// Provides the server/request IDs and a LogSink for emitting client-visible
/// log messages during request handling.
#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include <nlohmann/json.hpp>

#include "vgi_rpc/export.h"
#include "vgi_rpc/log.h"
#include "vgi_rpc/log_sink.h"
#include "vgi_rpc/errors.h"
#include "vgi_rpc/session.h"
#include "vgi_rpc/identity.h"
#include "vgi_rpc/state_ref.h"

namespace vgi_rpc {

// Coarse identity of the wire transport carrying a call.  Subprocess workers
// use PIPE because they speak the same stdin/stdout framing as an in-process
// pipe transport.
enum class TransportKind {
    PIPE,
    HTTP,
    UNIX,
    TCP,
};

VGI_RPC_EXPORT const char* transport_kind_name(TransportKind kind) noexcept;

// Sticky-session machinery for one request, installed by the HTTP transport
// and absent everywhere else.  The other transports are single-process, so a
// session that binds state to "this worker" would mean nothing there — the
// runtime raises rather than silently no-opping, which is what stops a method
// from appearing to work until it is deployed behind a load balancer.
struct StickySlot {
    std::shared_ptr<SessionState> resolved;  // session this request arrived on
    std::string session_id;
    bool client_accepts = false;  // VGI-Session-Accept: true
    bool draining = false;

    // Registers a state object and returns the token to mint on the response.
    std::function<std::string(std::shared_ptr<SessionState>, std::optional<int>)> open;
    std::function<void()> close;

    // Filled in by open()/close() for the transport to act on afterwards.
    std::string minted_token;
    bool closed = false;
};

class VGI_RPC_EXPORT CallContext {
public:
    CallContext(std::shared_ptr<LogSink> sink, std::string server_id, std::string request_id);
    CallContext(std::shared_ptr<LogSink> sink, std::string server_id, std::string request_id,
                TransportKind transport_kind);

    void client_log(LogLevel level, std::string_view message, const nlohmann::json& extra = {});
    void client_log(const Message& msg);

    const std::string& server_id() const noexcept { return server_id_; }
    const std::string& request_id() const noexcept { return request_id_; }
    TransportKind transport_kind() const noexcept { return transport_kind_; }
    const AuthContext& auth() const noexcept { return auth_; }
    const PeerEvidenceSet& peer_evidence() const noexcept { return peer_evidence_; }
    std::optional<int64_t> response_limit_bytes() const noexcept { return response_limit_bytes_; }
    std::optional<int64_t> preferred_response_bytes() const noexcept {
        return preferred_response_bytes_;
    }
    void set_response_budgets(std::optional<int64_t> limit,
                              std::optional<int64_t> preferred) noexcept {
        response_limit_bytes_ = limit;
        preferred_response_bytes_ = preferred;
    }
    void set_identity(AuthContext auth, PeerEvidenceSet evidence) {
        auth_ = std::move(auth);
        peer_evidence_ = std::move(evidence);
    }

    std::shared_ptr<LogSink> log_sink() const noexcept { return sink_; }

    // --- Sticky sessions (HTTP only) ---

    // Owned by the transport for the life of the request; null elsewhere.
    void set_sticky(StickySlot* slot) noexcept { sticky_ = slot; }

    // The state bound by a previous open_session on this session, or null.
    // Returns the same object identity across the calls of one session.
    std::shared_ptr<SessionState> session() const;
    std::string session_id() const;

    // Bind `state` to this worker for `ttl` seconds (default when unset).
    // Throws if the transport has no sticky machinery, if the client did not
    // send VGI-Session-Accept, or if a session is already active — the last
    // two prevent a server opening a session for a client not tracking it.
    void open_session(std::shared_ptr<SessionState> state,
                      std::optional<int> ttl_seconds = std::nullopt);

    // Close the active session and tell the client to drop its token.
    // Idempotent.
    void close_session();

    // --- StateRef and argument references (protocol 3.4) ---
    //
    // STATE_REF_V1.md.  This port is single-replica in 3.4 (§5.4): a StateRef
    // stays in memory on every transport, so these never touch storage.

    // Wrap an immutable per-stream value.  Valid only on the context handed
    // to a stream method itself; anywhere else -- a unary call, a stream's
    // process/produce/exchange (the first producer turn folded into an HTTP
    // init included), on_cancel -- throws StateRefMisuseError
    // (`state_ref_misuse`, FAILED_PRECONDITION).  Within one init, the same
    // bytes (or the same record instance) return the same ref.  Never call it
    // again on a later turn: copy the ref into the next state.
    StateRef state_ref(std::string value);
    // A record, serialized as an Arrow IPC stream for the byte form; a typed
    // resolve returns this very instance.
    StateRef state_ref(const std::shared_ptr<arrow::RecordBatch>& record);

    // The value's bytes, on any turn of any transport.  Valid while `ref` (or
    // a copy of it) is alive.  Throws InvalidStateRefError for an empty ref.
    const std::string& resolve(const StateRef& ref) const;
    // Typed variant for a record ref: the original instance while in memory,
    // else the record decoded from its bytes.
    std::shared_ptr<arrow::RecordBatch> resolve_record(const StateRef& ref) const;

    // Resolve an argument reference minted by this server's
    // `__upload_url__/init` grant, for this request's principal, with the
    // size and SHA-256 hex it was sent with (STATE_REF_V1.md §10.2).  Goes
    // through the process's object cache.  Errors: invalid_arguments_ref,
    // arguments_ref_integrity, arguments_ref_missing, storage_unavailable.
    // Only the HTTP transport vends grants; elsewhere this is
    // invalid_arguments_ref.
    std::shared_ptr<const std::string> resolve_argument_ref(const std::string& ref, int64_t size,
                                                            const std::string& sha256_hex) const;

    // Transport plumbing.
    void set_state_ref_runtime(std::shared_ptr<const StateRefRuntime> runtime) noexcept {
        state_ref_runtime_ = std::move(runtime);
    }
    // The protocol and method this context serves, for error messages.
    void set_call_identity(std::string protocol, std::string method) {
        protocol_ = std::move(protocol);
        method_ = std::move(method);
    }
    const std::string& protocol_name() const noexcept { return protocol_; }
    const std::string& method_name() const noexcept { return method_; }

    // Brackets the call of a stream method (the factory).  Only inside it is
    // state_ref() allowed; the transport opens the scope immediately before
    // the call and closes it immediately after, whatever the outcome.
    class VGI_RPC_EXPORT StreamInitScope {
    public:
        explicit StreamInitScope(CallContext& ctx);
        ~StreamInitScope();
        StreamInitScope(const StreamInitScope&) = delete;
        StreamInitScope& operator=(const StreamInitScope&) = delete;

    private:
        CallContext& ctx_;
    };

private:
    struct StateRefMemo;
    std::shared_ptr<LogSink> sink_;
    std::string server_id_;
    std::string request_id_;
    TransportKind transport_kind_ = TransportKind::PIPE;
    StickySlot* sticky_ = nullptr;
    AuthContext auth_ = AuthContext::anonymous();
    PeerEvidenceSet peer_evidence_;
    std::optional<int64_t> response_limit_bytes_;
    std::optional<int64_t> preferred_response_bytes_;
    std::shared_ptr<const StateRefRuntime> state_ref_runtime_;
    std::string protocol_;
    std::string method_;
    // Non-null only while a stream method runs: the init turn's memo.
    std::shared_ptr<StateRefMemo> state_ref_memo_;
};

}  // namespace vgi_rpc
