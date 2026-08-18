// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/// Per-request context passed to method handlers and stream processors.
/// Provides the server/request IDs and a LogSink for emitting client-visible
/// log messages during request handling.
#pragma once

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

namespace vgi_rpc {

// Sticky-session machinery for one request, installed by the HTTP transport
// and absent everywhere else.  The other transports are single-process, so a
// session that binds state to "this worker" would mean nothing there — the
// runtime raises rather than silently no-opping, which is what stops a method
// from appearing to work until it is deployed behind a load balancer.
struct StickySlot {
    std::shared_ptr<SessionState> resolved;  // session this request arrived on
    std::string session_id;
    bool client_accepts = false;             // VGI-Session-Accept: true
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
    CallContext(std::shared_ptr<LogSink> sink,
                std::string server_id,
                std::string request_id);

    void client_log(LogLevel level, std::string_view message,
                    const nlohmann::json& extra = {});
    void client_log(const Message& msg);

    const std::string& server_id() const noexcept { return server_id_; }
    const std::string& request_id() const noexcept { return request_id_; }

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

private:
    std::shared_ptr<LogSink> sink_;
    std::string server_id_;
    std::string request_id_;
    StickySlot* sticky_ = nullptr;
};

}  // namespace vgi_rpc
