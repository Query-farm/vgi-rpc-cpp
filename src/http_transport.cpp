// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// HTTP transport for the vgi-rpc server (cpp-httplib).  Maps the pipe-based
// wire protocol onto stateless HTTP request/response pairs per
// docs/WIRE_PROTOCOL.md section 10.  Streaming state is held server-side in an
// opaque-token session map (the client treats the token as opaque), which keeps
// the C++ stream-state objects — which are not serializable — alive across the
// separate HTTP requests of a stream.  Requests are serialized under one mutex
// and a single worker thread to preserve the framework's single-threaded model.

#include "vgi_rpc/server.h"
#include "vgi_rpc/arrow_utils.h"
#include "vgi_rpc/log_sink.h"
#include "vgi_rpc/metadata.h"
#include "vgi_rpc/output_collector.h"
#include "vgi_rpc/wire.h"

#include <arrow/array.h>
#include <arrow/compute/cast.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include <httplib.h>

#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

namespace vgi_rpc {

namespace {

constexpr const char* ARROW_CONTENT_TYPE = "application/vnd.apache.arrow.stream";
constexpr const char* STREAM_STATE_B64 = "vgi_rpc.stream_state#b64";
constexpr const char* REQUEST_ID_HEADER = "X-Request-ID";

// Live stream held across the separate HTTP requests of one stream call.
struct HttpStreamSession {
    std::shared_ptr<StreamState> state;
    std::shared_ptr<arrow::Schema> output_schema;
    std::shared_ptr<arrow::Schema> input_schema;
    bool is_exchange = false;
    std::string method_name;
};

std::string buffer_to_string(const std::shared_ptr<arrow::Buffer>& buf) {
    return std::string(reinterpret_cast<const char*>(buf->data()),
                       static_cast<size_t>(buf->size()));
}

// Serialize one or more IPC streams written by `fn` into a byte string.
template <typename Fn>
std::string build_body(Fn&& fn) {
    auto out = unwrap(arrow::io::BufferOutputStream::Create());
    fn(out);
    return buffer_to_string(unwrap(out->Finish()));
}

std::string error_body(const std::shared_ptr<arrow::Schema>& schema,
                       const std::string& exception_type,
                       const std::string& message,
                       const std::string& server_id,
                       const std::string& request_id) {
    return build_body([&](const std::shared_ptr<arrow::io::OutputStream>& out) {
        auto err = Result::error(schema, exception_type, message, server_id, request_id);
        write_ipc_stream(out, schema, {err.annotated_batch()});
    });
}

// Same name-set-strict / order+cast-tolerant coercion the pipe transport uses.
std::shared_ptr<arrow::RecordBatch> coerce_input(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::shared_ptr<arrow::Schema>& target) {
    if (batch->schema()->Equals(*target)) return batch;
    auto mismatch = [&]() {
        return std::logic_error("Input schema mismatch: expected " + target->ToString() +
                                ", got " + batch->schema()->ToString());
    };
    std::set<std::string> bn, tn;
    for (const auto& f : batch->schema()->fields()) bn.insert(f->name());
    for (const auto& f : target->fields()) tn.insert(f->name());
    if (bn != tn) throw mismatch();
    std::vector<std::shared_ptr<arrow::Array>> cols;
    for (const auto& f : target->fields()) {
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

std::shared_ptr<arrow::KeyValueMetadata> token_metadata(const std::string& token) {
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(STREAM_STATE_B64, token);
    return md;
}

std::string error_type_of(const std::exception& e) {
    if (dynamic_cast<const std::invalid_argument*>(&e)) return "ValueError";
    if (dynamic_cast<const std::out_of_range*>(&e)) return "IndexError";
    if (dynamic_cast<const std::logic_error*>(&e)) return "TypeError";
    return "RuntimeError";
}

// Drive a producer into `writer` for one HTTP turn.  With no wire cap
// (max_bytes < 0) this emits exactly one produce cycle then asks for a
// continuation token, matching the Python reference's incremental default;
// with a cap it buffers cycles until the body fills the cap.  Returns true
// when the stream is terminal (finished or errored — no token needed), false
// when the caller should append a continuation token.  A produce-time error is
// written as an in-stream error batch and reported as terminal.
bool run_producer_turns(const std::shared_ptr<arrow::ipc::RecordBatchWriter>& writer,
                        const std::shared_ptr<arrow::io::OutputStream>& out_stream,
                        const std::shared_ptr<StreamState>& state,
                        const std::shared_ptr<arrow::Schema>& schema,
                        CallContext& ctx, const std::string& server_id,
                        const std::string& request_id, int64_t max_bytes) {
    while (true) {
        OutputCollector oc(schema, /*producer=*/true, server_id, request_id);
        try {
            state->process(AnnotatedBatch::data(make_empty_batch(empty_schema())), oc, ctx);
        } catch (const std::exception& e) {
            auto md = make_error_metadata(error_type_of(e), e.what(), server_id, request_id);
            VGI_RPC_THROW_NOT_OK(writer->WriteRecordBatch(*make_empty_batch(schema), md));
            return true;
        }
        for (const auto& ab : oc.batches()) {
            VGI_RPC_THROW_NOT_OK(ab.custom_metadata
                ? writer->WriteRecordBatch(*ab.batch, ab.custom_metadata)
                : writer->WriteRecordBatch(*ab.batch));
        }
        if (oc.is_finished()) return true;
        bool should_continue = (max_bytes >= 0) && (unwrap(out_stream->Tell()) < max_bytes);
        if (!should_continue) return false;
    }
}

}  // namespace

void Server::serve_http(const std::string& host, int port, int64_t max_response_bytes) {
    httplib::Server svr;
    // A global mutex serializes request handling to preserve the framework's
    // single-threaded model.  We keep httplib's default (multi-thread) pool so
    // that concurrent keep-alive connections each get a worker — a single-thread
    // pool would let one idle keep-alive connection starve all others.
    auto mtx = std::make_shared<std::mutex>();
    auto sessions = std::make_shared<std::unordered_map<std::string, HttpStreamSession>>();

    // OPTIONS {prefix}/health — capability discovery.
    svr.Options(R"(/health)",
                [max_response_bytes](const httplib::Request&, httplib::Response& res) {
        if (max_response_bytes >= 0) {
            res.set_header("VGI-Max-Response-Bytes", std::to_string(max_response_bytes));
        }
        res.set_header("VGI-Externalization-Enabled", "false");
        res.status = 200;
    });

    svr.Post(R"(/(.+))", [this, mtx, sessions, max_response_bytes](
                             const httplib::Request& req, httplib::Response& res) {
        std::lock_guard<std::mutex> lock(*mtx);

        std::string request_id = req.get_header_value(REQUEST_ID_HEADER);
        if (request_id.empty()) request_id = random_hex(16);
        res.set_header(REQUEST_ID_HEADER, request_id);
        res.set_header("Content-Type", ARROW_CONTENT_TYPE);

        // Content-Type enforcement (415).
        std::string ctype = req.get_header_value("Content-Type");
        if (ctype.rfind(ARROW_CONTENT_TYPE, 0) != 0) {
            res.status = 415;
            res.set_content("Unsupported Media Type", "text/plain");
            return;
        }

        // Route: /{method}, /{method}/init, /{method}/exchange.
        std::string path = req.matches.size() > 1 ? std::string(req.matches[1]) : std::string();
        bool is_init = false, is_exchange_ep = false;
        std::string method_name = path;
        if (path.size() > 5 && path.substr(path.size() - 5) == "/init") {
            is_init = true;
            method_name = path.substr(0, path.size() - 5);
        } else if (path.size() > 9 && path.substr(path.size() - 9) == "/exchange") {
            is_exchange_ep = true;
            method_name = path.substr(0, path.size() - 9);
        }

        // Parse the request IPC stream from the body.
        auto body_buf = arrow::Buffer::Wrap(req.body.data(), req.body.size());
        auto reader = std::make_shared<arrow::io::BufferReader>(body_buf);
        std::optional<IpcStreamContents> contents;
        try {
            contents = read_ipc_stream(reader);
        } catch (const std::exception& e) {
            res.status = 400;
            res.set_content(error_body(empty_schema(), "ProtocolError",
                                       std::string("Invalid request IPC: ") + e.what(),
                                       server_id_, request_id),
                            ARROW_CONTENT_TYPE);
            return;
        }
        if (!contents || contents->batches.empty()) {
            res.status = 400;
            res.set_content(error_body(empty_schema(), "ProtocolError",
                                       "Empty request body", server_id_, request_id),
                            ARROW_CONTENT_TYPE);
            return;
        }
        auto& first = contents->batches[0];
        auto& batch = first.batch;
        auto& custom_metadata = first.custom_metadata;

        // Version check.  Stream /exchange continuations carry the state token
        // instead of the request_version metadata, so only enforce it on the
        // initial request (unary call or stream /init).
        if (!is_exchange_ep) {
            auto version = get_metadata_value(custom_metadata, keys::REQUEST_VERSION);
            if (version != REQUEST_VERSION_VALUE) {
                res.status = 400;
                res.set_content(error_body(empty_schema(), "VersionError",
                                           "Unsupported or missing request version, expected '1'.",
                                           server_id_, request_id),
                                ARROW_CONTENT_TYPE);
                return;
            }
        }

        // Method lookup.
        auto it = methods_.find(method_name);
        if (it == methods_.end()) {
            res.status = 404;
            res.set_content(error_body(empty_schema(), "AttributeError",
                                       "Unknown method: '" + method_name + "'",
                                       server_id_, request_id),
                            ARROW_CONTENT_TYPE);
            return;
        }
        const auto& method_info = it->second;
        Request request(batch, custom_metadata);

        // ---- Unary ----
        if (!is_init && !is_exchange_ep) {
            auto buf_out = unwrap(arrow::io::BufferOutputStream::Create());
            serve_unary(method_info, request, request_id, buf_out);
            auto rbuf = unwrap(buf_out->Finish());
            if (max_response_bytes >= 0 && rbuf->size() > max_response_bytes) {
                res.status = 200;
                res.set_content(
                    error_body(method_info.result_schema, "RpcError",
                               "HTTP body exceeds max_response_bytes (" +
                                   std::to_string(rbuf->size()) + " > " +
                                   std::to_string(max_response_bytes) + ") for method '" +
                                   method_name + "'",
                               server_id_, request_id),
                    ARROW_CONTENT_TYPE);
                return;
            }
            res.status = 200;
            res.set_content(buffer_to_string(rbuf), ARROW_CONTENT_TYPE);
            return;
        }

        auto log_sink = std::make_shared<LogSink>(server_id_, request_id);
        CallContext ctx(log_sink, server_id_, request_id);

        // ---- Stream init ----
        if (is_init) {
            Stream stream;
            try {
                stream = method_info.stream_factory(request, ctx);
            } catch (const std::invalid_argument& e) {
                res.status = 200;
                res.set_content(error_body(empty_schema(), "ValueError", e.what(),
                                           server_id_, request_id), ARROW_CONTENT_TYPE);
                return;
            } catch (const std::logic_error& e) {
                res.status = 200;
                res.set_content(error_body(empty_schema(), "TypeError", e.what(),
                                           server_id_, request_id), ARROW_CONTENT_TYPE);
                return;
            } catch (const std::exception& e) {
                res.status = 200;
                res.set_content(error_body(empty_schema(), "RuntimeError", e.what(),
                                           server_id_, request_id), ARROW_CONTENT_TYPE);
                return;
            }

            std::string token = random_hex(32);
            auto output_schema = stream.output_schema;
            auto input_schema = stream.input_schema;
            bool is_exchange = method_info.is_exchange;

            std::string body = build_body([&](const std::shared_ptr<arrow::io::OutputStream>& out) {
                // Header stream (if any) carries the init logs before the header row.
                if (stream.header) {
                    auto header_schema = stream.header->schema();
                    std::vector<AnnotatedBatch> hb = log_sink->flush(header_schema);
                    hb.push_back(AnnotatedBatch::data(stream.header));
                    write_ipc_stream(out, header_schema, hb);
                }

                if (is_exchange) {
                    // Exchange init: a single zero-row batch carrying the token.
                    std::vector<AnnotatedBatch> ob;
                    if (!stream.header) ob = log_sink->flush(output_schema);
                    ob.push_back(AnnotatedBatch::with_metadata(
                        make_empty_batch(output_schema), token_metadata(token)));
                    write_ipc_stream(out, output_schema, ob);
                    (*sessions)[token] = {stream.state, output_schema, input_schema, true,
                                          method_name};
                } else {
                    // Producer init: produce one or more cycles (per the cap),
                    // appending a continuation token if not yet finished.
                    auto writer = unwrap(arrow::ipc::MakeStreamWriter(out, output_schema));
                    if (!stream.header) {
                        for (auto& lb : log_sink->flush(output_schema)) {
                            VGI_RPC_THROW_NOT_OK(lb.custom_metadata
                                ? writer->WriteRecordBatch(*lb.batch, lb.custom_metadata)
                                : writer->WriteRecordBatch(*lb.batch));
                        }
                    }
                    bool finished = run_producer_turns(writer, out, stream.state, output_schema,
                                                       ctx, server_id_, request_id,
                                                       max_response_bytes);
                    if (!finished) {
                        VGI_RPC_THROW_NOT_OK(writer->WriteRecordBatch(
                            *make_empty_batch(output_schema), token_metadata(token)));
                        (*sessions)[token] = {stream.state, output_schema, input_schema, false,
                                              method_name};
                    }
                    VGI_RPC_THROW_NOT_OK(writer->Close());
                }
            });
            res.status = 200;
            res.set_content(body, ARROW_CONTENT_TYPE);
            return;
        }

        // ---- Stream exchange / producer continuation ----
        auto token = get_metadata_value(custom_metadata, STREAM_STATE_B64);
        auto sit = sessions->find(token);
        if (token.empty() || sit == sessions->end()) {
            res.status = 400;
            res.set_content(error_body(empty_schema(), "ProtocolError",
                                       "Unknown or missing stream state token",
                                       server_id_, request_id), ARROW_CONTENT_TYPE);
            return;
        }
        HttpStreamSession& sess = sit->second;
        auto output_schema = sess.output_schema;

        // Cancellation: on_cancel, drop the session, return an empty output stream.
        if (custom_metadata && custom_metadata->FindKey(keys::CANCEL) >= 0) {
            try { sess.state->on_cancel(ctx); } catch (...) {}
            sessions->erase(sit);
            res.status = 200;
            res.set_content(build_body([&](const std::shared_ptr<arrow::io::OutputStream>& out) {
                write_ipc_stream(out, output_schema, {});
            }), ARROW_CONTENT_TYPE);
            return;
        }

        try {
            if (sess.is_exchange) {
                auto coerced = coerce_input(batch, sess.input_schema);
                OutputCollector oc(output_schema, /*producer=*/false, server_id_, request_id);
                sess.state->process(AnnotatedBatch::data(coerced), oc, ctx);
                std::string body = build_body([&](const std::shared_ptr<arrow::io::OutputStream>& out) {
                    std::vector<AnnotatedBatch> batches = oc.batches();
                    // Attach the (reused) token to the single data batch.
                    for (auto& ab : batches) {
                        if (ab.batch && ab.batch->num_rows() > 0) {
                            ab.custom_metadata = token_metadata(token);
                        }
                    }
                    if (batches.empty() || batches.back().batch->num_rows() == 0) {
                        // Zero-column / zero-row exchange output: still carry the token.
                        AnnotatedBatch tb = AnnotatedBatch::with_metadata(
                            make_empty_batch(output_schema), token_metadata(token));
                        batches.push_back(tb);
                    }
                    write_ipc_stream(out, output_schema, batches);
                });
                if (max_response_bytes >= 0 &&
                    static_cast<int64_t>(body.size()) > max_response_bytes) {
                    res.status = 200;
                    res.set_content(
                        error_body(output_schema, "RpcError",
                                   "HTTP body exceeds max_response_bytes (" +
                                       std::to_string(body.size()) + " > " +
                                       std::to_string(max_response_bytes) + ") for method '" +
                                       sess.method_name + "'",
                                   server_id_, request_id),
                        ARROW_CONTENT_TYPE);
                    return;
                }
                res.status = 200;
                res.set_content(body, ARROW_CONTENT_TYPE);
            } else {
                // Producer continuation: resume producing for one HTTP turn.
                bool finished = false;
                std::string body = build_body([&](const std::shared_ptr<arrow::io::OutputStream>& out) {
                    auto writer = unwrap(arrow::ipc::MakeStreamWriter(out, output_schema));
                    finished = run_producer_turns(writer, out, sess.state, output_schema, ctx,
                                                  server_id_, request_id, max_response_bytes);
                    if (!finished) {
                        VGI_RPC_THROW_NOT_OK(writer->WriteRecordBatch(
                            *make_empty_batch(output_schema), token_metadata(token)));
                    }
                    VGI_RPC_THROW_NOT_OK(writer->Close());
                });
                if (finished) sessions->erase(token);
                res.status = 200;
                res.set_content(body, ARROW_CONTENT_TYPE);
            }
        } catch (const std::invalid_argument& e) {
            sessions->erase(token);
            res.status = 200;
            res.set_content(error_body(output_schema, "ValueError", e.what(),
                                       server_id_, request_id), ARROW_CONTENT_TYPE);
        } catch (const std::logic_error& e) {
            sessions->erase(token);
            res.status = 200;
            res.set_content(error_body(output_schema, "TypeError", e.what(),
                                       server_id_, request_id), ARROW_CONTENT_TYPE);
        } catch (const std::exception& e) {
            sessions->erase(token);
            res.status = 200;
            res.set_content(error_body(output_schema, "RuntimeError", e.what(),
                                       server_id_, request_id), ARROW_CONTENT_TYPE);
        }
    });

    int bound;
    if (port == 0) {
        bound = svr.bind_to_any_port(host);  // returns the ephemeral port
    } else {
        bound = svr.bind_to_port(host, port) ? port : -1;
    }
    if (bound < 0) {
        std::cerr << "vgi_rpc: failed to bind HTTP server to " << host << ":" << port << "\n";
        return;
    }
    std::cout << "PORT:" << bound << std::endl;
    svr.listen_after_bind();
}

}  // namespace vgi_rpc
