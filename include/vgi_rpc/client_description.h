// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/// Client-side decoding of the version-4 __describe__ response.
#pragma once

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

#include <arrow/type_fwd.h>

#include "vgi_rpc/annotated_batch.h"
#include "vgi_rpc/export.h"

namespace vgi_rpc {

struct MethodDescription {
    std::string name;
    std::string method_type;
    bool has_return = false;
    std::shared_ptr<arrow::Schema> params_schema;
    std::shared_ptr<arrow::Schema> result_schema;
    bool has_header = false;
    std::shared_ptr<arrow::Schema> header_schema;
    std::optional<bool> is_exchange;
};

struct ServiceDescription {
    std::string protocol_name;
    std::string request_version;
    std::string describe_version;
    std::string protocol_hash;
    std::string server_id;
    std::string protocol_version;
    std::unordered_map<std::string, MethodDescription> methods;

    const MethodDescription* method(const std::string& name) const noexcept;
};

/// Parse and validate one version-4 __describe__ data batch. Throws
/// std::runtime_error for an invalid response rather than returning a partial
/// description that a generated or dynamic client could misinterpret.
VGI_RPC_EXPORT ServiceDescription parse_service_description(const AnnotatedBatch& response);

}  // namespace vgi_rpc
