// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/call_context.h"

namespace vgi_rpc {

CallContext::CallContext(std::shared_ptr<LogSink> sink,
                         std::string server_id,
                         std::string request_id)
    : sink_(std::move(sink))
    , server_id_(std::move(server_id))
    , request_id_(std::move(request_id)) {}

void CallContext::client_log(LogLevel level, std::string_view message,
                             const nlohmann::json& extra) {
    sink_->emit(level, message, extra);
}

void CallContext::client_log(const Message& msg) {
    sink_->emit(msg);
}

std::shared_ptr<SessionState> CallContext::session() const {
    return sticky_ ? sticky_->resolved : nullptr;
}

std::string CallContext::session_id() const {
    return sticky_ ? sticky_->session_id : std::string();
}

void CallContext::open_session(std::shared_ptr<SessionState> state,
                               std::optional<int> ttl_seconds) {
    if (!sticky_ || !sticky_->open) {
        throw std::runtime_error("sticky sessions not available on this transport");
    }
    if (sticky_->draining) {
        // Distinct from session_lost: the client's token is fine, the server
        // is going away.  Existing sessions keep serving until they end.
        throw ServerDrainingError("server is draining; not opening new sessions");
    }
    if (!sticky_->client_accepts) {
        throw std::runtime_error(
            "open_session requires the client to send 'VGI-Session-Accept: true'; "
            "without it the client would not capture the token and the session would leak");
    }
    if (sticky_->resolved || !sticky_->minted_token.empty()) {
        throw std::runtime_error("a session is already active for this request");
    }
    sticky_->minted_token = sticky_->open(std::move(state), ttl_seconds);
}

void CallContext::close_session() {
    if (!sticky_ || !sticky_->close) {
        throw std::runtime_error("sticky sessions not available on this transport");
    }
    if (sticky_->closed) return;  // idempotent
    sticky_->close();
    sticky_->closed = true;
    sticky_->resolved.reset();
}

}  // namespace vgi_rpc
