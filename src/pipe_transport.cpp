// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/server.h"
#include "vgi_rpc/access_log.h"
#include "vgi_rpc/arrow_utils.h"
#include "vgi_rpc/metadata.h"
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

bool Server::serve_one(const std::shared_ptr<arrow::io::InputStream>& input,
                       const std::shared_ptr<arrow::io::OutputStream>& output) {
    // 1. Read request IPC stream
    auto contents_opt = read_ipc_stream(input);
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

    // 4. Application protocol version gate.
    // The synthetic `__`-prefixed methods are exempt: they are framework
    // surface, and `__describe__` in particular is how a mismatched client
    // finds out what this server speaks.
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
    refresh_shm(custom_metadata);
    const bool request_used_shm =
        custom_metadata && (custom_metadata->FindKey(keys::SHM_OFFSET) >= 0 ||
                            custom_metadata->FindKey(keys::SHM_SEGMENT_NAME) >= 0);
    call_shm_ = request_used_shm ? shm_ : nullptr;

    int64_t shm_free_offset = -1;
    if (is_shm_pointer_batch(batch, custom_metadata)) {
        if (!shm_) {
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
            batch = resolve_shm_batch(batch, &custom_metadata, shm_, &shm_free_offset);
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
        serve_unary(method_info, request, request_id, output);
    } else {
        serve_stream(method_info, request, request_id, input, output);
    }

    // The region is dead once the handler has read its columns out.
    if (shm_free_offset >= 0 && shm_) shm_->free_alloc(shm_free_offset);
    call_shm_.reset();
    return true;
}

void Server::refresh_shm(const std::shared_ptr<arrow::KeyValueMetadata>& custom_metadata) {
    const std::string name = get_metadata_value(custom_metadata, keys::SHM_SEGMENT_NAME);
    if (name.empty() || name == shm_name_) return;

    size_t size = 0;
    try {
        size = static_cast<size_t>(
            std::stoull(get_metadata_value(custom_metadata, keys::SHM_SEGMENT_SIZE)));
    } catch (const std::exception&) {
        return;  // an unreadable size means we stay on the pipe
    }
    // A failed attach is not an error: the peer offered a channel we cannot
    // use, and the pipe still carries everything.
    shm_ = ShmSegment::attach(name, size);
    shm_name_ = shm_ ? name : std::string();
}

bool Server::serve_unary_http(const MethodInfo& method_info, const Request& request,
                              const std::string& request_id,
                              const std::shared_ptr<arrow::io::OutputStream>& output,
                              CallContext& ctx) {
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
    result_ab.batch = maybe_write_to_shm(result_ab.batch, &result_ab.custom_metadata, call_shm_);
    response_batches.push_back(std::move(result_ab));
    write_ipc_stream(output, method_info.result_schema, response_batches);
    VGI_RPC_THROW_NOT_OK(output->Flush());

    if (access_log_ && access_log_->enabled()) {
        AccessRecord rec;
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
                         const std::shared_ptr<arrow::io::OutputStream>& output) {
    auto log_sink = std::make_shared<LogSink>(server_id_, request_id);
    CallContext ctx(log_sink, server_id_, request_id);
    serve_unary_http(method_info, request, request_id, output, ctx);
}

void Server::serve_stream(const MethodInfo& method_info, const Request& request,
                          const std::string& request_id,
                          const std::shared_ptr<arrow::io::InputStream>& input,
                          const std::shared_ptr<arrow::io::OutputStream>& output) {
    auto t0 = std::chrono::steady_clock::now();
    auto log_sink = std::make_shared<LogSink>(server_id_, request_id);
    CallContext ctx(log_sink, server_id_, request_id);

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
                CallContext cancel_ctx(log_sink, server_id_, request_id);
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
                if (!shm_) {
                    throw std::runtime_error(
                        "Stream input carries a shared-memory pointer but no segment is attached.");
                }
                raw_input = resolve_shm_batch(raw_input, &input_ab.custom_metadata, shm_,
                                              &input_free_offset);
            }

            // Exchange streams coerce the inbound batch to the declared input
            // schema (reorder + compatible casts); producer ticks are empty.
            input_ab.batch = is_producer ? raw_input : coerce_input_batch(raw_input, input_schema);

            OutputCollector out(output_schema, is_producer, server_id_, request_id);
            CallContext stream_ctx(log_sink, server_id_, request_id);

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
                ab.batch = maybe_write_to_shm(ab.batch, &ab.custom_metadata, call_shm_);
                if (ab.custom_metadata) {
                    VGI_RPC_THROW_NOT_OK(
                        output_writer->WriteRecordBatch(*ab.batch, ab.custom_metadata));
                } else {
                    VGI_RPC_THROW_NOT_OK(output_writer->WriteRecordBatch(*ab.batch));
                }
            }

            VGI_RPC_THROW_NOT_OK(output->Flush());

            // The handler has read the input by now, so its region is dead.
            if (input_free_offset >= 0 && shm_) shm_->free_alloc(input_free_offset);

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
        AccessRecord rec;
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
