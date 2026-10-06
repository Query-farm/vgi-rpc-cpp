// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/// Constructs RPC response batches returned from unary method handlers.
/// Use Result::value() to return data, Result::void_result() for void methods,
/// Result::from_external_ref() to answer with a pre-published object, and
/// Result::error() to signal an exception to the client.
#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include "vgi_rpc/annotated_batch.h"
#include "vgi_rpc/errors.h"
#include "vgi_rpc/export.h"
#include "vgi_rpc/external.h"

namespace vgi_rpc {

// Build the standard error metadata used for both unary error results
// and mid-stream error batches.
// `error_kind`, when non-empty, is the machine-readable class a client
// branches on (e.g. "session_lost"); `exception_type` stays the human-facing
// name.  See docs/sticky-sessions-spec.md §6.
//
// Every batch it builds carries the error model (WIRE_PROTOCOL.md §8):
// `vgi_rpc.error_code` always -- `extras.code`, or the framework table's code
// for the type and kind when that is empty, `UNKNOWN` when unclassified --
// and `vgi_rpc.error_details` when `extras.details` is non-empty and obeys the
// catalog rules and the 4 KiB cap (dropped whole otherwise).  All three are
// mirrored into `log_extra`.  `extras.traceback`, when non-empty, rides as
// `log_extra.traceback`; callers decide per transport whether to pass one.
VGI_RPC_EXPORT std::shared_ptr<arrow::KeyValueMetadata> make_error_metadata(
    const std::string& exception_type, const std::string& message,
    const std::string& server_id = "", const std::string& request_id = "",
    const std::string& error_kind = "", const ErrorExtras& extras = ErrorExtras{});

// The error batch metadata for a caught exception: its type, message, kind,
// code and details, plus `traceback` when non-empty.
VGI_RPC_EXPORT std::shared_ptr<arrow::KeyValueMetadata> make_exception_metadata(
    const std::exception& e, const std::string& server_id, const std::string& request_id,
    const std::string& traceback = "");

class VGI_RPC_EXPORT Result {
public:
    // Create a result with a pre-built batch (1 row for values, 0 rows for void/error)
    static Result value(std::shared_ptr<arrow::RecordBatch> batch);

    // Create a result from an AnnotatedBatch (batch + custom metadata)
    static Result from_annotated_batch(AnnotatedBatch ab);

    // Create a result from schema + arrays (builds a 1-row batch)
    static Result value(std::shared_ptr<arrow::Schema> schema,
                        std::vector<std::shared_ptr<arrow::Array>> arrays);

    // Void result (0-row batch on empty schema)
    static Result void_result();

    // Answer with a pre-published object (see ExternalRef / publish_external).
    // The server writes the external-location pointer batch for `ref` on the
    // method's result schema instead of building a value: no serialization,
    // no upload, never inline or through shared memory, on every transport.
    static Result from_external_ref(ExternalRef ref);

    // Error result (0-row batch with EXCEPTION metadata)
    static Result error(std::shared_ptr<arrow::Schema> schema, const std::string& exception_type,
                        const std::string& message, const std::string& server_id = "",
                        const std::string& request_id = "", const std::string& error_kind = "",
                        const ErrorExtras& extras = ErrorExtras{});

    // Error result for a caught exception, carrying its whole error model.
    static Result error(std::shared_ptr<arrow::Schema> schema, const std::exception& e,
                        const std::string& server_id, const std::string& request_id,
                        const std::string& traceback = "");

    const AnnotatedBatch& annotated_batch() const noexcept { return batch_; }
    const std::shared_ptr<arrow::Schema>& schema() const;

    // The pre-published reference this result answers with, if any.  When
    // set, annotated_batch() is a placeholder the dispatcher does not send.
    const std::optional<ExternalRef>& external_ref() const noexcept { return external_ref_; }

private:
    explicit Result(AnnotatedBatch batch) : batch_(std::move(batch)) {}
    AnnotatedBatch batch_;
    std::optional<ExternalRef> external_ref_;
};

}  // namespace vgi_rpc
