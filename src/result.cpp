// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/result.h"
#include "vgi_rpc/metadata.h"
#include "vgi_rpc/log.h"
#include "vgi_rpc/errors.h"
#include "traceback_scope.h"

#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>
#include <nlohmann/json.hpp>

#include <stdexcept>
#include <vector>

namespace vgi_rpc {

namespace {

// Arrow custom metadata is byte-oriented, but every vgi-rpc peer decodes log
// fields as UTF-8 and LOG_EXTRA is JSON. Protocol errors can quote untrusted
// metadata bytes, so normalize malformed sequences before putting them on the
// wire instead of letting JSON serialization tear down a reusable connection.
std::string wire_safe_text(const std::string& text) {
    const auto encoded =
        nlohmann::json(text).dump(-1, ' ', false, nlohmann::json::error_handler_t::replace);
    return nlohmann::json::parse(encoded).get<std::string>();
}

}  // anonymous namespace

namespace detail {

namespace {
thread_local TracebackScope* g_traceback_scope = nullptr;
}  // namespace

TracebackScope* current_traceback_scope() noexcept {
    return g_traceback_scope;
}

ScopedTracebackPolicy::ScopedTracebackPolicy(bool include)
    : scope_{include, {}}, previous_(g_traceback_scope) {
    g_traceback_scope = &scope_;
}

ScopedTracebackPolicy::~ScopedTracebackPolicy() {
    g_traceback_scope = previous_;
}

}  // namespace detail

Result Result::value(std::shared_ptr<arrow::RecordBatch> batch) {
    AnnotatedBatch ab;
    ab.batch = std::move(batch);
    ab.custom_metadata = nullptr;
    return Result(std::move(ab));
}

Result Result::from_annotated_batch(AnnotatedBatch ab) {
    return Result(std::move(ab));
}

Result Result::value(std::shared_ptr<arrow::Schema> schema,
                     std::vector<std::shared_ptr<arrow::Array>> arrays) {
    int64_t num_rows = arrays.empty() ? 0 : arrays[0]->length();
    auto batch = arrow::RecordBatch::Make(std::move(schema), num_rows, std::move(arrays));
    return Result::value(std::move(batch));
}

Result Result::void_result() {
    AnnotatedBatch ab;
    ab.batch = make_empty_batch(empty_schema());
    ab.custom_metadata = nullptr;
    return Result(std::move(ab));
}

Result Result::from_external_ref(ExternalRef ref) {
    Result result = Result::void_result();
    result.external_ref_ = std::move(ref);
    return result;
}

std::shared_ptr<arrow::KeyValueMetadata> make_error_metadata(
    const std::string& exception_type, const std::string& message, const std::string& server_id,
    const std::string& request_id, const std::string& error_kind, const ErrorExtras& extras) {
    const std::string safe_exception_type = wire_safe_text(exception_type);
    const std::string safe_message = wire_safe_text(message);
    const std::string safe_kind = wire_safe_text(error_kind);
    // The code is required on every EXCEPTION batch, so it is settled first.
    // An explicit code outside the closed set is a programming error that
    // must not reach the wire as an unknown string; it becomes UNKNOWN.
    const std::string code = extras.code.empty()
                                 ? code_name(default_error_code(exception_type, error_kind))
                                 : code_name(parse_code(extras.code));
    // Same encoder for the top-level key and the decision to mirror, so the
    // bytes on the wire are the bytes the cap was measured against.
    const std::optional<std::string> details = encode_error_details(extras.details);

    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(keys::LOG_LEVEL, log_level_to_string(LogLevel::EXCEPTION));
    md->Append(keys::LOG_MESSAGE, safe_message);

    nlohmann::json extra;
    extra["exception_type"] = safe_exception_type;
    extra["exception_message"] = safe_message;
    extra["error_code"] = code;
    if (!safe_kind.empty()) extra["error_kind"] = safe_kind;
    if (details) extra["error_details"] = extras.details;
    // A server whose traceback setting is on sends a non-empty traceback on
    // every EXCEPTION batch (WIRE_PROTOCOL.md §8); a site that wrote none gets
    // a synthesized one.  An explicit traceback is only ever passed by a site
    // that consulted the same setting.
    std::string traceback = extras.traceback;
    if (traceback.empty()) {
        if (const auto* scope = detail::current_traceback_scope();
            scope != nullptr && scope->include) {
            traceback = cpp_traceback(exception_type, message, scope->where);
        }
    }
    if (!traceback.empty()) extra["traceback"] = wire_safe_text(traceback);
    md->Append(keys::LOG_EXTRA, extra.dump());

    if (!server_id.empty()) {
        md->Append(keys::SERVER_ID, server_id);
    }
    if (!request_id.empty()) {
        md->Append(keys::REQUEST_ID, request_id);
    }
    md->Append(keys::ERROR_CODE, code);
    if (!safe_kind.empty()) {
        md->Append(keys::ERROR_KIND, safe_kind);
    }
    if (details) {
        md->Append(keys::ERROR_DETAILS, *details);
    }
    return md;
}

std::shared_ptr<arrow::KeyValueMetadata> make_exception_metadata(const std::exception& e,
                                                                 const std::string& server_id,
                                                                 const std::string& request_id,
                                                                 const std::string& traceback) {
    return make_error_metadata(exception_type_of(e), e.what(), server_id, request_id,
                               error_kind_of(e), error_extras_of(e, traceback));
}

Result Result::error(std::shared_ptr<arrow::Schema> schema, const std::string& exception_type,
                     const std::string& message, const std::string& server_id,
                     const std::string& request_id, const std::string& error_kind,
                     const ErrorExtras& extras) {
    AnnotatedBatch ab;
    ab.batch = make_empty_batch(schema);
    ab.custom_metadata =
        make_error_metadata(exception_type, message, server_id, request_id, error_kind, extras);
    return Result(std::move(ab));
}

Result Result::error(std::shared_ptr<arrow::Schema> schema, const std::exception& e,
                     const std::string& server_id, const std::string& request_id,
                     const std::string& traceback) {
    AnnotatedBatch ab;
    ab.batch = make_empty_batch(schema);
    ab.custom_metadata = make_exception_metadata(e, server_id, request_id, traceback);
    return Result(std::move(ab));
}

const std::shared_ptr<arrow::Schema>& Result::schema() const {
    return batch_.batch->schema();
}

}  // namespace vgi_rpc
