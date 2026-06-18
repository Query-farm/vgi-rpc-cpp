// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/server.h"
#include "vgi_rpc/access_log.h"
#include "vgi_rpc/arrow_utils.h"
#include "vgi_rpc/metadata.h"
#include "vgi_rpc/wire.h"
#include "vgi_rpc/log_sink.h"
#include "vgi_rpc/output_collector.h"

#include <arrow/array.h>
#include <arrow/compute/cast.h>
#include <arrow/io/memory.h>
#include <arrow/io/stdio.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

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
void write_stream_error(
    const std::shared_ptr<arrow::ipc::RecordBatchWriter>& writer,
    const std::shared_ptr<arrow::Schema>& schema,
    const std::string& exception_type,
    const std::string& message,
    const std::string& server_id,
    const std::string& request_id) {
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
        return std::logic_error("Input schema mismatch: expected " + target->ToString() +
                                ", got " + batch->schema()->ToString());
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

// Serialize a request batch to a self-contained Arrow IPC stream and return it
// base64-encoded, for the access log's request_data field.
std::string serialize_request_b64(const std::shared_ptr<arrow::RecordBatch>& batch) {
    auto out = unwrap(arrow::io::BufferOutputStream::Create());
    write_ipc_stream(out, batch->schema(), {AnnotatedBatch::data(batch)});
    auto buf = unwrap(out->Finish());
    return base64_encode(buf->data(), static_cast<size_t>(buf->size()));
}

// Milliseconds elapsed since `t0`.
double elapsed_ms_since(std::chrono::steady_clock::time_point t0) {
    auto dt = std::chrono::steady_clock::now() - t0;
    return std::chrono::duration<double, std::milli>(dt).count();
}

}  // anonymous namespace

void Server::run() {
    auto input = std::make_shared<arrow::io::StdinStream>();
    auto output = std::make_shared<StdoutStream>();

    while (true) {
        try {
            if (!serve_one(input, output)) break;
        } catch (const std::exception&) {
            break;  // Fatal I/O error (broken pipe); cannot recover
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
            "Missing 'vgi_rpc.method' in request batch custom_metadata.",
            server_id_, request_id);
        write_ipc_stream(output, empty_schema(), {error_result.annotated_batch()});
        VGI_RPC_THROW_NOT_OK(output->Flush());
        return true;
    }

    // 3. Validate request version
    auto version = get_metadata_value(custom_metadata, keys::REQUEST_VERSION);
    if (version.empty()) {
        auto error_result = Result::error(
            empty_schema(), "VersionError",
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
            "Unsupported request version '" + version + "', expected '1'.",
            server_id_, request_id);
        write_ipc_stream(output, empty_schema(), {error_result.annotated_batch()});
        VGI_RPC_THROW_NOT_OK(output->Flush());
        return true;
    }

    // 4. Look up handler
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
    Request request(batch, custom_metadata);

    if (method_info.method_type == MethodType::UNARY) {
        serve_unary(method_info, request, request_id, output);
    } else {
        serve_stream(method_info, request, request_id, input, output);
    }
    return true;
}

void Server::serve_unary(const MethodInfo& method_info,
                         const Request& request,
                         const std::string& request_id,
                         const std::shared_ptr<arrow::io::OutputStream>& output) {
    auto t0 = std::chrono::steady_clock::now();
    auto log_sink = std::make_shared<LogSink>(server_id_, request_id);
    CallContext ctx(log_sink, server_id_, request_id);

    std::string status = "ok", error_type, error_message;
    Result result = Result::void_result();
    try {
        result = method_info.handler(request, ctx);
    } catch (const std::invalid_argument& e) {
        status = "error"; error_type = "ValueError"; error_message = e.what();
        result = Result::error(
            method_info.result_schema, "ValueError", e.what(),
            server_id_, request_id);
    } catch (const std::out_of_range& e) {
        status = "error"; error_type = "IndexError"; error_message = e.what();
        result = Result::error(
            method_info.result_schema, "IndexError", e.what(),
            server_id_, request_id);
    } catch (const std::logic_error& e) {
        status = "error"; error_type = "TypeError"; error_message = e.what();
        result = Result::error(
            method_info.result_schema, "TypeError", e.what(),
            server_id_, request_id);
    } catch (const std::exception& e) {
        status = "error"; error_type = "RuntimeError"; error_message = e.what();
        result = Result::error(
            method_info.result_schema, "RuntimeError", e.what(),
            server_id_, request_id);
    }

    auto log_batches = log_sink->flush(method_info.result_schema);

    std::vector<AnnotatedBatch> response_batches;
    response_batches.reserve(log_batches.size() + 1);

    for (auto& log_ab : log_batches) {
        response_batches.push_back(std::move(log_ab));
    }

    response_batches.push_back(result.annotated_batch());
    write_ipc_stream(output, method_info.result_schema, response_batches);
    VGI_RPC_THROW_NOT_OK(output->Flush());

    if (access_log_ && access_log_->enabled()) {
        AccessRecord rec;
        rec.method = method_info.name;
        rec.is_stream = false;
        rec.status = status;
        rec.error_type = error_type;
        rec.error_message = error_message;
        rec.duration_ms = elapsed_ms_since(t0);
        rec.request_data_b64 = serialize_request_b64(request.batch());
        rec.has_request_data = true;
        access_log_->emit(rec);
    }
}

void Server::serve_stream(const MethodInfo& method_info,
                          const Request& request,
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
        auto error_result = Result::error(
            empty_schema(), error_type, msg, server_id_, request_id);
        write_ipc_stream(output, empty_schema(), {error_result.annotated_batch()});
        VGI_RPC_THROW_NOT_OK(output->Flush());

        // Drain the client's tick/data IPC stream that will follow.
        // The client sends an IPC stream (ticks for producer, data for exchange)
        // even after reading the error. We must consume it so the pipe stays clean
        // for the next request.
        try {
            read_ipc_stream(input);
        } catch (const std::exception& e) {
            fprintf(stderr, "vgi_rpc: warning: error draining input after factory error: %s\n", e.what());
        } catch (...) {}
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
            VGI_RPC_THROW_NOT_OK(output_writer->WriteRecordBatch(*log_ab.batch, log_ab.custom_metadata));
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
                } catch (...) {}
                break;
            }

            AnnotatedBatch input_ab;
            // Exchange streams coerce the inbound batch to the declared input
            // schema (reorder + compatible casts); producer ticks are empty.
            input_ab.batch = is_producer
                ? batch_with_md.batch
                : coerce_input_batch(batch_with_md.batch, input_schema);
            input_ab.custom_metadata =
                batch_with_md.custom_metadata
                    ? std::static_pointer_cast<arrow::KeyValueMetadata>(
                          batch_with_md.custom_metadata->Copy())
                    : nullptr;

            OutputCollector out(output_schema, is_producer, server_id_, request_id);
            CallContext stream_ctx(log_sink, server_id_, request_id);

            state->process(input_ab, out, stream_ctx);

            // Flush log_sink and out batches to writer
            auto stream_logs = log_sink->flush(output_schema);
            for (auto& log_ab : stream_logs) {
                if (log_ab.custom_metadata) {
                    VGI_RPC_THROW_NOT_OK(output_writer->WriteRecordBatch(*log_ab.batch, log_ab.custom_metadata));
                } else {
                    VGI_RPC_THROW_NOT_OK(output_writer->WriteRecordBatch(*log_ab.batch));
                }
            }

            for (const auto& ab : out.batches()) {
                if (ab.custom_metadata) {
                    VGI_RPC_THROW_NOT_OK(output_writer->WriteRecordBatch(*ab.batch, ab.custom_metadata));
                } else {
                    VGI_RPC_THROW_NOT_OK(output_writer->WriteRecordBatch(*ab.batch));
                }
            }

            VGI_RPC_THROW_NOT_OK(output->Flush());

            if (out.is_finished()) break;
        }
    } catch (const std::invalid_argument& e) {
        status = "error"; error_type = "ValueError"; error_message = e.what();
        write_stream_error(output_writer, output_schema, "ValueError",
                           e.what(), server_id_, request_id);
    } catch (const std::out_of_range& e) {
        status = "error"; error_type = "IndexError"; error_message = e.what();
        write_stream_error(output_writer, output_schema, "IndexError",
                           e.what(), server_id_, request_id);
    } catch (const std::logic_error& e) {
        status = "error"; error_type = "TypeError"; error_message = e.what();
        write_stream_error(output_writer, output_schema, "TypeError",
                           e.what(), server_id_, request_id);
    } catch (const std::exception& e) {
        status = "error"; error_type = "RuntimeError"; error_message = e.what();
        write_stream_error(output_writer, output_schema, "RuntimeError",
                           e.what(), server_id_, request_id);
    }

    // Close output writer (writes EOS) — suppress errors if pipe is broken
    try {
        VGI_RPC_THROW_NOT_OK(output_writer->Close());
        VGI_RPC_THROW_NOT_OK(output->Flush());
    } catch (const std::exception& e) {
        fprintf(stderr, "vgi_rpc: warning: error closing stream writer: %s\n", e.what());
    } catch (...) {}

    // Drain remaining input
    drain_reader(input_reader);

    if (access_log_ && access_log_->enabled()) {
        AccessRecord rec;
        rec.method = method_info.name;
        rec.is_stream = true;
        rec.status = status;
        rec.error_type = error_type;
        rec.error_message = error_message;
        rec.duration_ms = elapsed_ms_since(t0);
        rec.stream_id = stream_id;
        rec.cancelled = cancelled_flag;
        rec.request_data_b64 = serialize_request_b64(request.batch());
        rec.has_request_data = true;
        access_log_->emit(rec);
    }
}

}  // namespace vgi_rpc
