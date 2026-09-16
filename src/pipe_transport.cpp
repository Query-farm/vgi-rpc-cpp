// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/server.h"
#include "vgi_rpc/access_log.h"
#include "vgi_rpc/arrow_utils.h"
#include "vgi_rpc/metadata.h"
#include "vgi_rpc/reflection.h"
#include "vgi_rpc/token_identity.h"
#include <arrow/array/builder_binary.h>
#include "vgi_rpc/wire.h"
#include "vgi_rpc/log_sink.h"
#include "vgi_rpc/output_collector.h"
#include "vgi_rpc/shm.h"
#include "request_contract.h"

#include <arrow/array.h>
#include <arrow/compute/cast.h>
#include <arrow/io/memory.h>
#include <arrow/io/stdio.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#include <stdio.h>
#define STDIN_FILENO 0
#else
#include <unistd.h>
#endif

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

namespace vgi_rpc {

namespace {

// Write an error batch to a mid-stream IPC writer
void write_stream_error(const std::shared_ptr<arrow::ipc::RecordBatchWriter>& writer,
                        const std::shared_ptr<arrow::Schema>& schema,
                        const std::string& exception_type, const std::string& message,
                        const std::string& server_id, const std::string& request_id) {
    auto error_batch = make_empty_batch(schema);
    auto md = make_error_metadata(exception_type, message, server_id, request_id);
    VGI_RPC_THROW_NOT_OK(writer->WriteRecordBatch(*error_batch, md));
}

// Reconcile an inbound exchange batch to the declared input schema.  Strict on
// the field set, tolerant of column order and compatible type coercions (e.g.
// int32->float64).  Mirrors Python's _coerce_input_batch; a mismatch raises
// std::logic_error so the dispatcher surfaces it as a "TypeError".
std::shared_ptr<arrow::RecordBatch> coerce_input_batch(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::shared_ptr<arrow::Schema>& target) {
    if (batch->schema()->Equals(*target)) return batch;

    auto mismatch = [&]() {
        return std::logic_error("Input schema mismatch: expected " + target->ToString() + ", got " +
                                batch->schema()->ToString());
    };

    std::set<std::string> batch_names, target_names;
    for (const auto& f : batch->schema()->fields()) batch_names.insert(f->name());
    for (const auto& f : target->fields()) target_names.insert(f->name());
    if (batch_names != target_names) throw mismatch();

    std::vector<std::shared_ptr<arrow::Array>> cols;
    cols.reserve(static_cast<size_t>(target->num_fields()));
    for (const auto& f : target->fields()) {
        // GetColumnByName returns null for an ambiguous (duplicated) name; the
        // set-equality check above does not catch duplicates, so guard here.
        auto col = batch->GetColumnByName(f->name());
        if (!col) throw mismatch();
        if (!col->type()->Equals(*f->type())) {
            auto cast_res = arrow::compute::Cast(*col, f->type());
            if (!cast_res.ok()) throw mismatch();
            col = cast_res.ValueUnsafe();
        }
        cols.push_back(std::move(col));
    }
    return arrow::RecordBatch::Make(target, batch->num_rows(), std::move(cols));
}

}  // anonymous namespace

// Populate `rec`'s request_data (or its truncation accounting) for `batch`.
//
// The size is measured before anything is serialized, because the alternative
// is not merely wasteful: a 2 GiB request would otherwise be copied into an
// IPC buffer and then expanded ~4/3 again into base64, for a record the cap
// throws away regardless.  That is how the >INT_MAX conformance payload ran
// the worker out of memory.
void fill_request_data(const AccessLogWriter& log, AccessRecord& rec,
                       const std::shared_ptr<arrow::RecordBatch>& batch) {
    const int64_t b64_len = base64_encoded_length(ipc_stream_byte_size(batch));
    if (!log.payload_fits(b64_len)) {
        rec.has_request_data = false;
        rec.original_request_bytes = b64_len;
        return;
    }
    auto out = unwrap(arrow::io::BufferOutputStream::Create());
    write_ipc_stream(out, batch->schema(), {AnnotatedBatch::data(batch)});
    auto buf = unwrap(out->Finish());
    rec.request_data_b64 = base64_encode(buf->data(), static_cast<size_t>(buf->size()));
    rec.has_request_data = true;
}

namespace {

// Milliseconds elapsed since `t0`.
double elapsed_ms_since(std::chrono::steady_clock::time_point t0) {
    auto dt = std::chrono::steady_clock::now() - t0;
    return std::chrono::duration<double, std::milli>(dt).count();
}

}  // anonymous namespace

void Server::log_framework_call(const ProtocolIdentity& owner, const std::string& method,
                                const std::string& request_id,
                                const std::shared_ptr<arrow::RecordBatch>& request_batch,
                                std::chrono::steady_clock::time_point started,
                                const std::string& error_type, const std::string& error_message) {
    if (!access_log_ || !access_log_->enabled()) return;
    // `owner`, not this server's primary.  Reflection and identity are
    // protocols in their own right; filing their calls under the application's
    // name merges two protocols' traffic into one series, and pairing that name
    // with the application's digest sends a consumer to the wrong description
    // entirely.
    AccessRecord rec(owner.name, owner.hash);
    rec.method = method;
    rec.request_id = request_id;
    rec.is_stream = false;
    rec.status = error_type.empty() ? "ok" : "error";
    rec.error_type = error_type;
    rec.error_message = error_message;
    rec.duration_ms = elapsed_ms_since(started);
    // A malformed request can be refused before a batch was ever decoded, and
    // a record with no payload beats no record at all.
    if (request_batch != nullptr) fill_request_data(*access_log_, rec, request_batch);
    access_log_->emit(rec);
}

void Server::run() {
#ifdef _WIN32
    // Windows opens the standard streams in text mode, which rewrites CRLF and
    // — the part that actually bites — treats a 0x1A byte as end of file.  Arrow
    // IPC is binary, so a payload containing 0x1A reads short, the worker calls
    // it a corrupt stream and exits, and the peer sees the pipe die mid-write.
    // It hid for a while because the conformance payload that crosses INT_MAX
    // is one repeated byte that happens to be neither.
    _setmode(_fileno(stdin), _O_BINARY);
    _setmode(_fileno(stdout), _O_BINARY);
#endif
    notify_serve_start(TransportKind::PIPE);
    // Our own fd stream rather than arrow::io::StdinStream: a message body can
    // exceed INT_MAX, and the read side needs the same clamp-and-loop treatment
    // as the write side (see kMaxIoChunk in wire.h).
    auto input = std::make_shared<FdInputStream>(STDIN_FILENO);
    auto output = std::make_shared<StdoutStream>();

    while (true) {
        try {
            if (!serve_one(input, output)) break;
        } catch (const std::exception& e) {
            // Fatal I/O error (broken pipe); cannot recover.  Say so — a silent
            // exit here presents to the client as a truncated response body,
            // which is a much harder thing to diagnose than a line on stderr.
            fprintf(stderr, "vgi_rpc: fatal transport error, closing: %s\n", e.what());
            break;
        }
    }
}

/// Serve one call to `vgi_rpc.Reflection.v1`.
///
/// Two methods, deliberately. `list_protocols` is the cheap question -- what is
/// here, and has it changed -- and the only one a client needs on a warm path,
/// because the hash answers "has it changed" without transferring any schema.
/// `describe` is the expensive one, asked once.
///
/// Self-description is not special-cased: reflection appears in its own output,
/// so a client discovers it the same way it discovers everything else.
bool Server::serve_reflection(const std::shared_ptr<arrow::io::OutputStream>& output,
                              const std::string& method_name,
                              const std::shared_ptr<arrow::RecordBatch>& request_batch,
                              const std::string& request_id, bool* errored) {
    const auto t0 = std::chrono::steady_clock::now();
    if (errored != nullptr) *errored = false;
    auto fail = [&](const std::string& type, const std::string& message) {
        if (errored != nullptr) *errored = true;
        auto err = Result::error(empty_schema(), type, message, server_id_, request_id);
        write_ipc_stream(output, empty_schema(), {err.annotated_batch()});
        VGI_RPC_THROW_NOT_OK(output->Flush());
        log_framework_call(reflection_binding_, method_name, request_id, request_batch, t0, type,
                           message);
        return true;
    };

    // The bindings were fingerprinted once, at construction.  The digest a
    // client reads here is therefore the same string the access log files the
    // call under -- which is what makes `protocol_hash` usable as a registry
    // key rather than a number that happens to be near the right one.
    const bool hosts_identity = !identity_binding_.name.empty();

    arrow::Result<std::string> payload = arrow::Status::Invalid("unreachable");
    if (method_name == "list_protocols") {
        std::vector<ProtocolSummary> summaries{
            ProtocolSummary{protocol_name_, protocol_version_, application_binding_.hash},
            ProtocolSummary{kReflectionProtocolName, "", reflection_binding_.hash}};
        // Identity comes after reflection, so it appears in reflection's output
        // -- which is the whole reason a client can discover that this worker
        // resolves credentials, or mints grants, or does neither, without
        // calling anything and reading an error.
        if (hosts_identity) {
            summaries.push_back(ProtocolSummary{kIdentityProtocolName, "", identity_binding_.hash});
        }
        payload = BuildProtocolList(server_id_, "", REQUEST_VERSION_VALUE, summaries);
    } else if (method_name == "describe") {
        std::string requested;
        if (request_batch != nullptr) {
            auto col = request_batch->GetColumnByName("protocol");
            if (col != nullptr && col->length() > 0 && col->IsValid(0)) {
                if (auto* sa = dynamic_cast<arrow::StringArray*>(col.get())) {
                    requested = sa->GetString(0);
                }
            }
        }
        if (requested == protocol_name_) {
            payload = BuildServiceDescription(protocol_name_, protocol_version_,
                                              application_binding_.hash, methods_);
        } else if (requested == kReflectionProtocolName) {
            // Self-description is not special-cased: reflection reports the two
            // methods it answers, so a client that found it through
            // `list_protocols` can learn to call the protocol it is already
            // calling.
            payload = BuildServiceDescription(kReflectionProtocolName, "", reflection_binding_.hash,
                                              ReflectionMethods());
        } else if (hosts_identity && requested == kIdentityProtocolName) {
            payload = BuildServiceDescription(kIdentityProtocolName, "", identity_binding_.hash,
                                              identity_methods_);
        } else {
            // Named, not silently empty: an empty description reads as "this
            // protocol has no methods".
            std::string hosted = protocol_name_ + ", " + kReflectionProtocolName;
            if (hosts_identity) hosted += std::string(", ") + kIdentityProtocolName;
            return fail("RuntimeError", "This server does not host protocol '" + requested +
                                            "'. Hosted: [" + hosted + "]");
        }
    } else {
        return fail("AttributeError", std::string("Protocol '") + kReflectionProtocolName +
                                          "' has no method '" + method_name +
                                          "'. Available: ['describe', 'list_protocols']");
    }
    if (!payload.ok()) return fail("RuntimeError", payload.status().ToString());

    // The framework's ordinary convention for a structured return: the payload
    // rides as serialized bytes in a single `result` binary column. Reflection
    // is an ordinary protocol, so it is subject to it like everything else.
    auto schema = arrow::schema({arrow::field("result", arrow::binary(), /*nullable=*/false)});
    arrow::BinaryBuilder b;
    VGI_RPC_THROW_NOT_OK(b.Append(*payload));
    std::shared_ptr<arrow::Array> arr;
    VGI_RPC_THROW_NOT_OK(b.Finish(&arr));
    auto out_batch = arrow::RecordBatch::Make(schema, 1, {arr});
    auto result = Result::value(out_batch);
    write_ipc_stream(output, schema, {result.annotated_batch()});
    VGI_RPC_THROW_NOT_OK(output->Flush());
    log_framework_call(reflection_binding_, method_name, request_id, request_batch, t0, "", "");
    return true;
}

bool Server::serve_one(const std::shared_ptr<arrow::io::InputStream>& input,
                       const std::shared_ptr<arrow::io::OutputStream>& output) {
    return serve_one(input, output, TransportKind::PIPE);
}

bool Server::serve_one(const std::shared_ptr<arrow::io::InputStream>& input,
                       const std::shared_ptr<arrow::io::OutputStream>& output,
                       TransportKind transport_kind) {
    return serve_one(input, output, transport_kind, AuthContext::anonymous(), PeerEvidenceSet());
}

bool Server::serve_one(const std::shared_ptr<arrow::io::InputStream>& input,
                       const std::shared_ptr<arrow::io::OutputStream>& output,
                       TransportKind transport_kind, const AuthContext& auth,
                       const PeerEvidenceSet& peer_evidence) {
    return serve_one_with_state(input, output, transport_kind, default_connection_state_, auth,
                                peer_evidence);
}

bool Server::serve_one_with_state(const std::shared_ptr<arrow::io::InputStream>& input,
                                  const std::shared_ptr<arrow::io::OutputStream>& output,
                                  TransportKind transport_kind, ConnectionState& connection,
                                  const AuthContext& auth, const PeerEvidenceSet& peer_evidence,
                                  std::function<void()> first_frame_complete) {
    notify_serve_start(transport_kind);
    // 1. Read request IPC stream
    auto contents_opt = read_ipc_stream(input);
    if (contents_opt && first_frame_complete) first_frame_complete();
    if (!contents_opt || contents_opt->batches.empty()) {
        return false;  // Clean EOF — no more requests
    }
    auto& contents = *contents_opt;

    auto& first_batch = contents.batches[0];
    auto& batch = first_batch.batch;
    auto& custom_metadata = first_batch.custom_metadata;

    auto request_id = random_hex(16);

    // 2. Extract method name
    auto method_name = get_metadata_value(custom_metadata, keys::METHOD);
    if (method_name.empty()) {
        auto error_result = Result::error(
            empty_schema(), "ProtocolError",
            "Missing 'vgi_rpc.method' in request batch custom_metadata.", server_id_, request_id);
        write_ipc_stream(output, empty_schema(), {error_result.annotated_batch()});
        VGI_RPC_THROW_NOT_OK(output->Flush());
        return true;
    }

    // 3. Validate request version
    auto version = get_metadata_value(custom_metadata, keys::REQUEST_VERSION);
    if (version.empty()) {
        auto error_result =
            Result::error(empty_schema(), "VersionError",
                          "Missing 'vgi_rpc.request_version' in request batch custom_metadata. "
                          "Set the 'vgi_rpc.request_version' custom_metadata value to '1'.",
                          server_id_, request_id);
        write_ipc_stream(output, empty_schema(), {error_result.annotated_batch()});
        VGI_RPC_THROW_NOT_OK(output->Flush());
        return true;
    }
    if (version != REQUEST_VERSION_VALUE) {
        auto error_result = Result::error(
            empty_schema(), "VersionError",
            "Unsupported request version '" + version + "', expected '1'.", server_id_, request_id);
        write_ipc_stream(output, empty_schema(), {error_result.annotated_batch()});
        VGI_RPC_THROW_NOT_OK(output->Flush());
        return true;
    }

    // 3a. Reflection is a co-hosted protocol, routed by the same key as
    // everything else and appearing in its own output. Handled before the
    // version gate because it is exempt from it: this is what a
    // version-mismatched client calls to learn what mismatched, and gating it
    // would deny the client the diagnosis it came for.
    if (get_metadata_value(custom_metadata, keys::PROTOCOL) == kReflectionProtocolName) {
        return serve_reflection(output, method_name, batch, request_id);
    }

    // 3b. Identity, likewise co-hosted and likewise routed by protocol key.
    // Ahead of the version gate for a different reason than reflection: that
    // gate compares against the *application* protocol's declared version, and
    // this is a separate protocol that declares none, so gating it would refuse
    // identity calls over a version disagreement that says nothing about them.
    if (get_metadata_value(custom_metadata, keys::PROTOCOL) == kIdentityProtocolName) {
        return serve_identity(output, method_name, batch, request_id, auth);
    }

    // 3c. The routing key, required here.
    //
    // On stdio, unix and named pipes the metadata field is the *only* carrier,
    // so an absent key really is unroutable -- and with reflection and identity
    // co-hosted, letting it through would land the request on whichever
    // protocol the dispatcher reached first rather than telling the caller.
    // That is the mis-routing the rule exists to prevent, and it is why the
    // answer here differs from HTTP's: there the path segment has already
    // resolved the binding, so absent is accepted (see handle_rpc, and spec
    // §5c for what that relaxation gives up).
    //
    // The three answers are distinct because a client depends on the
    // difference: no key at all, a key naming a protocol this server does not
    // host, and a hosted protocol missing the method are three different
    // things to do about it.
    //
    // Reserved `__name__` methods are exempt: they are server-level surface
    // owned by no protocol, so there is nothing for them to name.  So is a
    // server that declared no protocol name -- it has no key to match, and
    // exactly one namespace with no name for it.  `ServerBuilder::protocol()`
    // declares one and turns the check on.
    if (IsApplicationMethod(method_name) && !protocol_name_.empty()) {
        const std::string wire_protocol = get_metadata_value(custom_metadata, keys::PROTOCOL);
        if (wire_protocol.empty()) {
            auto error_result = Result::error(
                empty_schema(), "ProtocolNotSpecifiedError",
                "Request carries no 'vgi_rpc.protocol' routing key. Every request must name "
                "the protocol it addresses. This server hosts: ['" +
                    protocol_name_ + "', '" + kReflectionProtocolName + "']" +
                    (identity_ != nullptr ? std::string(" and '") + kIdentityProtocolName + "'"
                                          : std::string()) +
                    ".",
                server_id_, request_id, ERROR_KIND_PROTOCOL_NOT_SPECIFIED);
            write_ipc_stream(output, empty_schema(), {error_result.annotated_batch()});
            VGI_RPC_THROW_NOT_OK(output->Flush());
            return true;
        }
        if (wire_protocol != protocol_name_) {
            // The request-supplied name is deliberately not echoed: a routing
            // failure must not be a way to get a chosen string into a log.
            auto error_result = Result::error(
                empty_schema(), "ProtocolNotSupportedError",
                "This server does not host the named protocol. It hosts: '" + protocol_name_ + "'.",
                server_id_, request_id, ERROR_KIND_PROTOCOL_NOT_SUPPORTED);
            write_ipc_stream(output, empty_schema(), {error_result.annotated_batch()});
            VGI_RPC_THROW_NOT_OK(output->Flush());
            return true;
        }
    }

    // 4. Application protocol version gate.
    // The synthetic `__`-prefixed methods are exempt: they are framework
    // surface rather than the application's, so a disagreement about the
    // application's version says nothing about them.
    if (method_name.rfind("__", 0) != 0) {
        if (auto reason = protocol_version_error(custom_metadata); !reason.empty()) {
            auto error_result = Result::error(empty_schema(), "ProtocolVersionError", reason,
                                              server_id_, request_id);
            write_ipc_stream(output, empty_schema(), {error_result.annotated_batch()});
            VGI_RPC_THROW_NOT_OK(output->Flush());
            return true;
        }
    }

    // 5. Look up handler
    auto it = methods_.find(method_name);
    if (it == methods_.end() && method_name == RETIRED_DESCRIBE_METHOD) {
        // Named explicitly because `__describe__` is *retired* rather than
        // merely absent, and from the caller's side those look identical while
        // needing opposite fixes -- update the client, or reconfigure the
        // server.  A stale client told only "unknown method" has no way to
        // learn that introspection moved to a protocol; one told where it went
        // is fixable from the error text alone.  Only this name is
        // special-cased: every other reserved name keeps the plain capability
        // answer, which is what a client probing for an optional method needs.
        auto error_result =
            Result::error(empty_schema(), "MethodNotImplementedError", RETIRED_DESCRIBE_MESSAGE,
                          server_id_, request_id, ERROR_KIND_METHOD_NOT_IMPLEMENTED);
        write_ipc_stream(output, empty_schema(), {error_result.annotated_batch()});
        VGI_RPC_THROW_NOT_OK(output->Flush());
        return true;
    }
    if (it == methods_.end()) {
        std::vector<std::string> names;
        for (const auto& [name, _] : methods_) {
            names.push_back(name);
        }
        std::sort(names.begin(), names.end());
        std::string available;
        for (size_t i = 0; i < names.size(); ++i) {
            if (i > 0) available += ", ";
            available += "'" + names[i] + "'";
        }
        auto error_result = Result::error(
            empty_schema(), "AttributeError",
            "Unknown method: '" + method_name + "'. Available methods: [" + available + "]",
            server_id_, request_id);
        write_ipc_stream(output, empty_schema(), {error_result.annotated_batch()});
        VGI_RPC_THROW_NOT_OK(output->Flush());
        return true;
    }

    auto& method_info = it->second;

    // 6. Shared memory.  Attach whatever segment this request advertises, then
    //    resolve a pointer request batch back to its real columns.  The
    //    response is routed through the segment only when the client signalled
    //    SHM for *this* call — either by sending a pointer or by naming the
    //    segment — because a caller that sent an inline request is not reading
    //    the other channel and would see an empty answer.
    refresh_shm(connection, custom_metadata);
    const bool request_used_shm =
        custom_metadata && (custom_metadata->FindKey(keys::SHM_OFFSET) >= 0 ||
                            custom_metadata->FindKey(keys::SHM_SEGMENT_NAME) >= 0);
    connection.call_shm = request_used_shm ? connection.shm : nullptr;

    int64_t shm_free_offset = -1;
    if (is_shm_pointer_batch(batch, custom_metadata)) {
        if (!connection.shm) {
            // A negotiation violation: fail loudly rather than hand the method
            // a zero-row batch the caller never sent.
            auto error_result =
                Result::error(empty_schema(), "ProtocolError",
                              "Request carries a shared-memory pointer but no segment is attached.",
                              server_id_, request_id);
            write_ipc_stream(output, empty_schema(), {error_result.annotated_batch()});
            VGI_RPC_THROW_NOT_OK(output->Flush());
            return true;
        }
        try {
            batch = resolve_shm_batch(batch, &custom_metadata, connection.shm, &shm_free_offset);
        } catch (const std::exception& e) {
            auto error_result =
                Result::error(empty_schema(), "ProtocolError", e.what(), server_id_, request_id);
            write_ipc_stream(output, empty_schema(), {error_result.annotated_batch()});
            VGI_RPC_THROW_NOT_OK(output->Flush());
            return true;
        }
    }

    if (const std::string error = parameter_contract_error(batch, method_info.params_schema);
        !error.empty()) {
        auto error_result =
            Result::error(empty_schema(), "ProtocolError", error, server_id_, request_id);
        write_ipc_stream(output, empty_schema(), {error_result.annotated_batch()});
        VGI_RPC_THROW_NOT_OK(output->Flush());
        return true;
    }

    Request request(batch, custom_metadata);

    if (method_info.method_type == MethodType::UNARY) {
        serve_unary(method_info, request, request_id, output, transport_kind, connection.call_shm,
                    auth, peer_evidence);
    } else {
        serve_stream(method_info, request, request_id, input, output, transport_kind, connection,
                     auth, peer_evidence);
    }

    // The region is dead once the handler has read its columns out.
    if (shm_free_offset >= 0 && connection.shm) connection.shm->free_alloc(shm_free_offset);
    connection.call_shm.reset();
    return true;
}

void Server::refresh_shm(ConnectionState& connection,
                         const std::shared_ptr<arrow::KeyValueMetadata>& custom_metadata) {
    const std::string name = get_metadata_value(custom_metadata, keys::SHM_SEGMENT_NAME);
    if (name.empty() || name == connection.shm_name) return;

    size_t size = 0;
    try {
        size = static_cast<size_t>(
            std::stoull(get_metadata_value(custom_metadata, keys::SHM_SEGMENT_SIZE)));
    } catch (const std::exception&) {
        return;  // an unreadable size means we stay on the pipe
    }
    // A failed attach is not an error: the peer offered a channel we cannot
    // use, and the pipe still carries everything.
    connection.shm = ShmSegment::attach(name, size);
    connection.shm_name = connection.shm ? name : std::string();
}

bool Server::serve_unary_http(const MethodInfo& method_info, const Request& request,
                              const std::string& request_id,
                              const std::shared_ptr<arrow::io::OutputStream>& output,
                              CallContext& ctx) {
    return serve_unary_impl(method_info, request, request_id, output, ctx, nullptr);
}

bool Server::serve_unary_impl(const MethodInfo& method_info, const Request& request,
                              const std::string& request_id,
                              const std::shared_ptr<arrow::io::OutputStream>& output,
                              CallContext& ctx, const std::shared_ptr<ShmSegment>& call_shm) {
    auto t0 = std::chrono::steady_clock::now();
    auto log_sink = ctx.log_sink();

    std::string status = "ok", error_type, error_message;
    Result result = Result::void_result();
    try {
        result = method_info.handler(request, ctx);
    } catch (const std::exception& e) {
        // The exception's class picks the error_type but never the HTTP
        // status: a raising method still answers 200, with the error in the
        // body, because the call reached the method and the method raised.
        status = "error";
        error_type = exception_type_of(e);
        error_message = e.what();
        result = Result::error(method_info.result_schema, error_type, e.what(), server_id_,
                               request_id, error_kind_of(e));
    }

    auto log_batches = log_sink->flush(method_info.result_schema);

    std::vector<AnnotatedBatch> response_batches;
    response_batches.reserve(log_batches.size() + 1);
    for (auto& log_ab : log_batches) {
        response_batches.push_back(std::move(log_ab));
    }
    // Log batches are zero-row and pass through untouched; only a data batch
    // large enough to be worth it becomes a pointer.
    AnnotatedBatch result_ab = result.annotated_batch();
    result_ab.batch = maybe_write_to_shm(result_ab.batch, &result_ab.custom_metadata, call_shm);
    response_batches.push_back(std::move(result_ab));
    write_ipc_stream(output, method_info.result_schema, response_batches);
    VGI_RPC_THROW_NOT_OK(output->Flush());

    if (access_log_ && access_log_->enabled()) {
        // The application binding owns everything in `methods_`.  The one
        // exception is `__transport_options__`, a framework endpoint owned by no
        // protocol -- which the spec says logs the server's primary, so it is
        // right here by prescription rather than by omission.
        AccessRecord rec(application_binding_.name, application_binding_.hash);
        rec.method = method_info.name;
        rec.request_id = request_id;
        rec.is_stream = false;
        rec.status = status;
        rec.error_type = error_type;
        rec.error_message = error_message;
        rec.duration_ms = elapsed_ms_since(t0);
        fill_request_data(*access_log_, rec, request.batch());
        access_log_->emit(rec);
    }
    return status == "error";
}

void Server::serve_unary(const MethodInfo& method_info, const Request& request,
                         const std::string& request_id,
                         const std::shared_ptr<arrow::io::OutputStream>& output,
                         TransportKind transport_kind, const std::shared_ptr<ShmSegment>& call_shm,
                         const AuthContext& auth, const PeerEvidenceSet& peer_evidence) {
    auto log_sink = std::make_shared<LogSink>(server_id_, request_id);
    CallContext ctx(log_sink, server_id_, request_id, transport_kind);
    ctx.set_identity(auth, peer_evidence);
    serve_unary_impl(method_info, request, request_id, output, ctx, call_shm);
}

void Server::serve_stream(const MethodInfo& method_info, const Request& request,
                          const std::string& request_id,
                          const std::shared_ptr<arrow::io::InputStream>& input,
                          const std::shared_ptr<arrow::io::OutputStream>& output,
                          TransportKind transport_kind, ConnectionState& connection,
                          const AuthContext& auth, const PeerEvidenceSet& peer_evidence) {
    auto t0 = std::chrono::steady_clock::now();
    auto log_sink = std::make_shared<LogSink>(server_id_, request_id);
    CallContext ctx(log_sink, server_id_, request_id, transport_kind);
    ctx.set_identity(auth, peer_evidence);

    // Access-log state for the (single) record emitted at the normal end of the
    // stream.  Factory-init failures return early and are not logged.
    std::string status = "ok", error_type, error_message;
    bool cancelled_flag = false;
    std::string stream_id = random_hex(32);

    // Call the stream factory
    Stream stream_result = Stream{};

    auto handle_factory_error = [&](const std::string& error_type, const char* msg) {
        auto error_result = Result::error(empty_schema(), error_type, msg, server_id_, request_id);
        write_ipc_stream(output, empty_schema(), {error_result.annotated_batch()});
        VGI_RPC_THROW_NOT_OK(output->Flush());

        // Drain the client's tick/data IPC stream that will follow.
        // The client sends an IPC stream (ticks for producer, data for exchange)
        // even after reading the error. We must consume it so the pipe stays clean
        // for the next request.
        try {
            read_ipc_stream(input);
        } catch (const std::exception& e) {
            fprintf(stderr, "vgi_rpc: warning: error draining input after factory error: %s\n",
                    e.what());
        } catch (...) {
        }
    };

    try {
        stream_result = method_info.stream_factory(request, ctx);
    } catch (const std::invalid_argument& e) {
        handle_factory_error("ValueError", e.what());
        return;
    } catch (const std::out_of_range& e) {
        handle_factory_error("IndexError", e.what());
        return;
    } catch (const std::logic_error& e) {
        handle_factory_error("TypeError", e.what());
        return;
    } catch (const std::exception& e) {
        handle_factory_error("RuntimeError", e.what());
        return;
    }

    auto& output_schema = stream_result.output_schema;
    auto& input_schema = stream_result.input_schema;
    auto& state = stream_result.state;

    if (!output_schema) {
        handle_factory_error("RuntimeError", "Stream factory returned null output_schema");
        return;
    }
    if (!input_schema) {
        handle_factory_error("RuntimeError", "Stream factory returned null input_schema");
        return;
    }
    if (!state) {
        handle_factory_error("RuntimeError", "Stream factory returned null state");
        return;
    }

    // Write header if present
    if (stream_result.header) {
        auto header_schema = stream_result.header->schema();
        auto init_logs = log_sink->flush(header_schema);

        std::vector<AnnotatedBatch> header_batches;
        for (auto& log_ab : init_logs) {
            header_batches.push_back(std::move(log_ab));
        }
        AnnotatedBatch header_ab;
        header_ab.batch = stream_result.header;
        header_ab.custom_metadata = nullptr;
        header_batches.push_back(std::move(header_ab));

        write_ipc_stream(output, header_schema, header_batches);
        VGI_RPC_THROW_NOT_OK(output->Flush());
    }

    // Open input reader (client sends ticks for producer, data for exchange)
    auto input_reader_result = arrow::ipc::RecordBatchStreamReader::Open(input);
    if (!input_reader_result.ok()) {
        return;  // Client disconnected
    }
    auto input_reader = std::move(input_reader_result).ValueUnsafe();

    // Open output writer
    auto output_writer_result = arrow::ipc::MakeStreamWriter(output, output_schema);
    if (!output_writer_result.ok()) {
        return;
    }
    auto output_writer = std::move(output_writer_result).ValueUnsafe();

    // Flush any init log batches
    auto init_logs = log_sink->flush(output_schema);
    for (auto& log_ab : init_logs) {
        if (log_ab.custom_metadata) {
            VGI_RPC_THROW_NOT_OK(
                output_writer->WriteRecordBatch(*log_ab.batch, log_ab.custom_metadata));
        } else {
            VGI_RPC_THROW_NOT_OK(output_writer->WriteRecordBatch(*log_ab.batch));
        }
    }

    bool is_producer = (input_schema->num_fields() == 0);

    // Stream loop
    try {
        while (true) {
            // Read input batch (with per-batch custom metadata so we can
            // detect client cancellation).
            auto read_result = input_reader->ReadNext();
            if (!read_result.ok()) break;  // I/O error / disconnect
            auto batch_with_md = std::move(read_result).ValueUnsafe();
            if (!batch_with_md.batch) break;  // EOS

            // Cancellation: the client sends a batch carrying vgi_rpc.cancel.
            // Run the state's on_cancel hook (best-effort) and stop without
            // emitting an output batch for this turn.
            if (batch_with_md.custom_metadata &&
                batch_with_md.custom_metadata->FindKey(keys::CANCEL) >= 0) {
                cancelled_flag = true;
                CallContext cancel_ctx(log_sink, server_id_, request_id, transport_kind);
                cancel_ctx.set_identity(auth, peer_evidence);
                try {
                    state->on_cancel(cancel_ctx);
                } catch (const std::exception& e) {
                    fprintf(stderr, "vgi_rpc: warning: on_cancel hook failed: %s\n", e.what());
                } catch (...) {
                }
                break;
            }

            AnnotatedBatch input_ab;
            input_ab.custom_metadata = batch_with_md.custom_metadata
                                           ? std::static_pointer_cast<arrow::KeyValueMetadata>(
                                                 batch_with_md.custom_metadata->Copy())
                                           : nullptr;

            // A large exchange input may arrive as a pointer into the peer's
            // segment; resolve it before coercion so the schema check sees the
            // real columns rather than a zero-row placeholder.
            auto raw_input = batch_with_md.batch;
            int64_t input_free_offset = -1;
            if (is_shm_pointer_batch(raw_input, input_ab.custom_metadata)) {
                if (!connection.shm) {
                    throw std::runtime_error(
                        "Stream input carries a shared-memory pointer but no segment is attached.");
                }
                raw_input = resolve_shm_batch(raw_input, &input_ab.custom_metadata, connection.shm,
                                              &input_free_offset);
            }

            // Exchange streams coerce the inbound batch to the declared input
            // schema (reorder + compatible casts); producer ticks are empty.
            input_ab.batch = is_producer ? raw_input : coerce_input_batch(raw_input, input_schema);

            OutputCollector out(output_schema, is_producer, server_id_, request_id);
            CallContext stream_ctx(log_sink, server_id_, request_id, transport_kind);
            stream_ctx.set_identity(auth, peer_evidence);

            state->process(input_ab, out, stream_ctx);

            // Flush log_sink and out batches to writer
            auto stream_logs = log_sink->flush(output_schema);
            for (auto& log_ab : stream_logs) {
                if (log_ab.custom_metadata) {
                    VGI_RPC_THROW_NOT_OK(
                        output_writer->WriteRecordBatch(*log_ab.batch, log_ab.custom_metadata));
                } else {
                    VGI_RPC_THROW_NOT_OK(output_writer->WriteRecordBatch(*log_ab.batch));
                }
            }

            for (auto ab : out.batches()) {
                ab.batch = maybe_write_to_shm(ab.batch, &ab.custom_metadata, connection.call_shm);
                if (ab.custom_metadata) {
                    VGI_RPC_THROW_NOT_OK(
                        output_writer->WriteRecordBatch(*ab.batch, ab.custom_metadata));
                } else {
                    VGI_RPC_THROW_NOT_OK(output_writer->WriteRecordBatch(*ab.batch));
                }
            }

            VGI_RPC_THROW_NOT_OK(output->Flush());

            // The handler has read the input by now, so its region is dead.
            if (input_free_offset >= 0 && connection.shm) {
                connection.shm->free_alloc(input_free_offset);
            }

            if (out.is_finished()) break;
        }
    } catch (const std::invalid_argument& e) {
        status = "error";
        error_type = "ValueError";
        error_message = e.what();
        write_stream_error(output_writer, output_schema, "ValueError", e.what(), server_id_,
                           request_id);
    } catch (const std::out_of_range& e) {
        status = "error";
        error_type = "IndexError";
        error_message = e.what();
        write_stream_error(output_writer, output_schema, "IndexError", e.what(), server_id_,
                           request_id);
    } catch (const std::logic_error& e) {
        status = "error";
        error_type = "TypeError";
        error_message = e.what();
        write_stream_error(output_writer, output_schema, "TypeError", e.what(), server_id_,
                           request_id);
    } catch (const std::exception& e) {
        status = "error";
        error_type = "RuntimeError";
        error_message = e.what();
        write_stream_error(output_writer, output_schema, "RuntimeError", e.what(), server_id_,
                           request_id);
    }

    // Close output writer (writes EOS) — suppress errors if pipe is broken
    try {
        VGI_RPC_THROW_NOT_OK(output_writer->Close());
        VGI_RPC_THROW_NOT_OK(output->Flush());
    } catch (const std::exception& e) {
        fprintf(stderr, "vgi_rpc: warning: error closing stream writer: %s\n", e.what());
    } catch (...) {
    }

    // Drain remaining input
    drain_reader(input_reader);

    if (access_log_ && access_log_->enabled()) {
        // Streams are application surface only: neither framework protocol
        // hosts one.
        AccessRecord rec(application_binding_.name, application_binding_.hash);
        rec.method = method_info.name;
        rec.request_id = request_id;
        rec.is_stream = true;
        rec.status = status;
        rec.error_type = error_type;
        rec.error_message = error_message;
        rec.duration_ms = elapsed_ms_since(t0);
        rec.stream_id = stream_id;
        rec.cancelled = cancelled_flag;
        fill_request_data(*access_log_, rec, request.batch());
        access_log_->emit(rec);
    }
}

}  // namespace vgi_rpc
