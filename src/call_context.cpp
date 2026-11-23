// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/call_context.h"

#include "vgi_rpc/arrow_utils.h"
#include "vgi_rpc/object_cache.h"
#include "vgi_rpc/wire.h"
#include "state_ref_internal.h"

#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/record_batch.h>

#include <map>
#include <string_view>
#include <unordered_map>

namespace vgi_rpc {

const char* transport_kind_name(TransportKind kind) noexcept {
    switch (kind) {
        case TransportKind::PIPE: return "pipe";
        case TransportKind::HTTP: return "http";
        case TransportKind::UNIX: return "unix";
        case TransportKind::TCP: return "tcp";
    }
    return "pipe";
}

CallContext::CallContext(std::shared_ptr<LogSink> sink, std::string server_id,
                         std::string request_id)
    : CallContext(std::move(sink), std::move(server_id), std::move(request_id),
                  TransportKind::PIPE) {}

CallContext::CallContext(std::shared_ptr<LogSink> sink, std::string server_id,
                         std::string request_id, TransportKind transport_kind)
    : sink_(std::move(sink)),
      server_id_(std::move(server_id)),
      request_id_(std::move(request_id)),
      transport_kind_(transport_kind) {}

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

// ---------------------------------------------------------------------------
// StateRef (STATE_REF_V1.md; single-replica, in memory)
// ---------------------------------------------------------------------------

// The init turn's memo (§2.2): byte-equal values and the same record instance
// return the same ref, so one init never holds two copies of one value.  Lives
// only for the init turn; it is not a cross-stream cache.
struct CallContext::StateRefMemo {
    std::unordered_map<std::string_view, StateRef> by_bytes;  // views into the refs' own bytes
    std::map<const arrow::RecordBatch*, StateRef> by_record;
};

CallContext::StreamInitScope::StreamInitScope(CallContext& ctx) : ctx_(ctx) {
    ctx_.state_ref_memo_ = std::make_shared<StateRefMemo>();
}

CallContext::StreamInitScope::~StreamInitScope() {
    ctx_.state_ref_memo_.reset();
}

namespace {

[[noreturn]] void throw_misuse(const std::string& protocol, const std::string& method) {
    std::string where = method;
    if (!protocol.empty()) where = protocol + "." + method;
    throw StateRefMisuseError(
        "state_ref() may only be called while handling a stream's init request (" + where + ")");
}

}  // namespace

StateRef CallContext::state_ref(std::string value) {
    if (!state_ref_memo_) throw_misuse(protocol_, method_);
    if (auto found = state_ref_memo_->by_bytes.find(value);
        found != state_ref_memo_->by_bytes.end()) {
        return found->second;
    }
    auto impl = std::make_shared<StateRef::Impl>();
    impl->bytes = std::make_shared<const std::string>(std::move(value));
    StateRef ref(impl);
    state_ref_memo_->by_bytes.emplace(std::string_view(*impl->bytes), ref);
    return ref;
}

StateRef CallContext::state_ref(const std::shared_ptr<arrow::RecordBatch>& record) {
    if (!state_ref_memo_) throw_misuse(protocol_, method_);
    if (!record) throw std::invalid_argument("state_ref() needs a record batch");
    if (auto found = state_ref_memo_->by_record.find(record.get());
        found != state_ref_memo_->by_record.end()) {
        return found->second;
    }
    auto sink = unwrap(arrow::io::BufferOutputStream::Create());
    write_ipc_stream(sink, record->schema(), {AnnotatedBatch::data(record)});
    const auto buffer = unwrap(sink->Finish());
    std::string bytes(reinterpret_cast<const char*>(buffer->data()),
                      static_cast<size_t>(buffer->size()));
    if (auto found = state_ref_memo_->by_bytes.find(bytes);
        found != state_ref_memo_->by_bytes.end() && found->second.impl_->record) {
        state_ref_memo_->by_record.emplace(record.get(), found->second);
        return found->second;
    }
    auto impl = std::make_shared<StateRef::Impl>();
    impl->bytes = std::make_shared<const std::string>(std::move(bytes));
    impl->record = record;
    StateRef ref(impl);
    state_ref_memo_->by_bytes.emplace(std::string_view(*impl->bytes), ref);
    state_ref_memo_->by_record.emplace(record.get(), ref);
    return ref;
}

const std::string& CallContext::resolve(const StateRef& ref) const {
    if (!ref.impl_) throw InvalidStateRefError("cannot resolve an empty StateRef");
    return *ref.impl_->bytes;
}

std::shared_ptr<arrow::RecordBatch> CallContext::resolve_record(const StateRef& ref) const {
    if (!ref.impl_) throw InvalidStateRefError("cannot resolve an empty StateRef");
    if (ref.impl_->record) return ref.impl_->record;
    const auto& bytes = *ref.impl_->bytes;
    // An owning copy: the decoded batch must not point into bytes that die
    // with the last copy of the ref.
    auto reader = std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(bytes));
    std::optional<IpcStreamContents> contents;
    try {
        contents = read_ipc_stream(reader);
    } catch (const std::exception& e) {
        throw InvalidStateRefError(std::string("StateRef value is not a record: ") + e.what());
    }
    if (!contents || contents->batches.size() != 1 || !contents->batches[0].batch) {
        throw InvalidStateRefError("StateRef value is not a single record batch");
    }
    return contents->batches[0].batch;
}

std::shared_ptr<const std::string> CallContext::resolve_argument_ref(
    const std::string& ref, int64_t size, const std::string& sha256_hex) const {
    if (!state_ref_runtime_) {
        throw InvalidArgumentsRefError(
            "argument references can only be resolved by an HTTP server");
    }
    ObjectCache& cache =
        state_ref_runtime_->cache ? *state_ref_runtime_->cache : default_object_cache();
    return vgi_rpc::resolve_argument_ref(ref, size, sha256_hex, state_ref_runtime_->token_key,
                                         auth_, state_ref_runtime_->store,
                                         state_ref_runtime_->max_fetch_bytes, cache);
}

}  // namespace vgi_rpc
