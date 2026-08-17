// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Conformance worker implementing all 81 methods from ConformanceService.
// Wire-compatible with the Python vgi_rpc conformance test suite.

#include "vgi_rpc/server.h"
#include "vgi_rpc/stream.h"
#include "vgi_rpc/metadata.h"
#include "vgi_rpc/wire.h"
#include "vgi_rpc/arrow_utils.h"

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <format>
#include <iostream>
#include <limits>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

using namespace vgi_rpc;

// =========================================================================
// Shared schemas
// =========================================================================

// Reused counter schema for producer streams
static std::shared_ptr<arrow::Schema> counter_schema() {
    static auto s = arrow::schema({
        arrow::field("index", arrow::int64()),
        arrow::field("value", arrow::int64()),
    });
    return s;
}

static std::shared_ptr<arrow::Schema> scale_input_schema() {
    static auto s = arrow::schema({arrow::field("value", arrow::float64())});
    return s;
}

static std::shared_ptr<arrow::Schema> scale_output_schema() {
    static auto s = arrow::schema({arrow::field("value", arrow::float64())});
    return s;
}

static std::shared_ptr<arrow::Schema> accum_input_schema() {
    static auto s = arrow::schema({arrow::field("value", arrow::float64())});
    return s;
}

static std::shared_ptr<arrow::Schema> accum_output_schema() {
    static auto s = arrow::schema({
        arrow::field("running_sum", arrow::float64()),
        arrow::field("exchange_count", arrow::int64()),
    });
    return s;
}

static std::shared_ptr<arrow::Schema> conformance_header_schema() {
    static auto s = arrow::schema({
        arrow::field("total_expected", arrow::int64()),
        arrow::field("description", arrow::utf8()),
    });
    return s;
}

// value: int64 — session-counter producer output / sticky output
static std::shared_ptr<arrow::Schema> session_value_schema() {
    static auto s = arrow::schema({arrow::field("value", arrow::int64())});
    return s;
}

// by: int64 — session-counter exchange input
static std::shared_ptr<arrow::Schema> session_by_schema() {
    static auto s = arrow::schema({arrow::field("by", arrow::int64())});
    return s;
}

// =========================================================================
// Result schemas
// =========================================================================

static std::shared_ptr<arrow::Schema> str_result_schema() {
    static auto s = arrow::schema({arrow::field("result", arrow::utf8())});
    return s;
}
static std::shared_ptr<arrow::Schema> bytes_result_schema() {
    static auto s = arrow::schema({arrow::field("result", arrow::binary())});
    return s;
}
static std::shared_ptr<arrow::Schema> int_result_schema() {
    static auto s = arrow::schema({arrow::field("result", arrow::int64())});
    return s;
}
static std::shared_ptr<arrow::Schema> float_result_schema() {
    static auto s = arrow::schema({arrow::field("result", arrow::float64())});
    return s;
}
static std::shared_ptr<arrow::Schema> bool_result_schema() {
    static auto s = arrow::schema({arrow::field("result", arrow::boolean())});
    return s;
}
static std::shared_ptr<arrow::Schema> binary_result_schema() {
    static auto s = arrow::schema({arrow::field("result", arrow::binary())});
    return s;
}
static std::shared_ptr<arrow::Schema> enum_result_schema() {
    static auto s = arrow::schema({
        arrow::field("result", arrow::dictionary(arrow::int16(), arrow::utf8()))});
    return s;
}
static std::shared_ptr<arrow::Schema> list_str_result_schema() {
    static auto s = arrow::schema({arrow::field("result", arrow::list(arrow::utf8()))});
    return s;
}
static std::shared_ptr<arrow::Schema> dict_str_int_result_schema() {
    static auto s = arrow::schema({
        arrow::field("result", arrow::map(arrow::utf8(), arrow::int64()))});
    return s;
}
static std::shared_ptr<arrow::Schema> nested_list_result_schema() {
    static auto s = arrow::schema({
        arrow::field("result", arrow::list(arrow::list(arrow::int64())))});
    return s;
}
static std::shared_ptr<arrow::Schema> optional_str_result_schema() {
    static auto s = arrow::schema({arrow::field("result", arrow::utf8(), true)});
    return s;
}
static std::shared_ptr<arrow::Schema> optional_int_result_schema() {
    static auto s = arrow::schema({arrow::field("result", arrow::int64(), true)});
    return s;
}
static std::shared_ptr<arrow::Schema> int32_result_schema() {
    static auto s = arrow::schema({arrow::field("result", arrow::int32())});
    return s;
}
static std::shared_ptr<arrow::Schema> float32_result_schema() {
    static auto s = arrow::schema({arrow::field("result", arrow::float32())});
    return s;
}
// list[int] return (cancel_probe_counters)
static std::shared_ptr<arrow::Schema> list_int_result_schema() {
    static auto s = arrow::schema({arrow::field("result", arrow::list(arrow::int64()))});
    return s;
}

// Wide-type result schemas (echo passthrough)
static std::shared_ptr<arrow::Schema> result_schema_of(std::shared_ptr<arrow::DataType> t,
                                                        bool nullable = false) {
    return arrow::schema({arrow::field("result", std::move(t), nullable)});
}

// =========================================================================
// Helpers
// =========================================================================

static Result echo_column(const Request& req, std::string_view param_name,
                          const std::shared_ptr<arrow::Schema>& result_schema) {
    auto col = req.batch()->GetColumnByName(std::string(param_name));
    return Result::value(result_schema, {col});
}

template <typename T>
static std::shared_ptr<T> checked_cast_column(
    const std::shared_ptr<arrow::RecordBatch>& batch, const std::string& name) {
    auto col = batch->GetColumnByName(name);
    if (!col) throw std::runtime_error("Column not found: " + name);
    auto typed = std::dynamic_pointer_cast<T>(col);
    if (!typed) throw std::runtime_error("Type mismatch for column: " + name);
    return typed;
}

static std::shared_ptr<arrow::RecordBatch> deserialize_dataclass(
    const Request& req, std::string_view param_name) {
    auto col = req.batch()->GetColumnByName(std::string(param_name));
    std::string_view view;
    if (auto bin = std::dynamic_pointer_cast<arrow::BinaryArray>(col)) {
        if (bin->length() == 0) throw std::runtime_error("Empty binary column for dataclass");
        view = bin->GetView(0);
    } else if (auto lbin = std::dynamic_pointer_cast<arrow::LargeBinaryArray>(col)) {
        if (lbin->length() == 0) throw std::runtime_error("Empty binary column for dataclass");
        view = lbin->GetView(0);
    } else {
        throw std::runtime_error("Expected binary column for dataclass");
    }
    auto buffer = arrow::Buffer::Wrap(view.data(), view.size());
    auto buf_reader = std::make_shared<arrow::io::BufferReader>(buffer);
    auto reader = unwrap(arrow::ipc::RecordBatchStreamReader::Open(buf_reader));
    std::shared_ptr<arrow::RecordBatch> batch;
    VGI_RPC_THROW_NOT_OK(reader->ReadNext(&batch));
    return batch;
}

static std::shared_ptr<arrow::RecordBatch> make_header_batch(
    int64_t total_expected, const std::string& description) {
    arrow::Int64Builder int_builder;
    arrow::StringBuilder str_builder;
    VGI_RPC_THROW_NOT_OK(int_builder.Append(total_expected));
    VGI_RPC_THROW_NOT_OK(str_builder.Append(description));
    return arrow::RecordBatch::Make(
        conformance_header_schema(), 1,
        {unwrap(int_builder.Finish()), unwrap(str_builder.Finish())});
}

// Format a double the way Python's repr does (e.g. 2.0 -> "2.0", 1.5 -> "1.5").
static std::string fmt_py_double(double v) {
    auto s = std::to_string(v);
    auto dot = s.find('.');
    if (dot != std::string::npos) {
        auto last_nonzero = s.find_last_not_of('0');
        if (last_nonzero == dot) last_nonzero++;  // keep "x.0"
        s.erase(last_nonzero + 1);
    }
    return s;
}

// =========================================================================
// RichHeader: schema + batch builder (matches Python ArrowSerializableDataclass)
// =========================================================================

static std::shared_ptr<arrow::DataType> point_struct_type() {
    static auto t = arrow::struct_({
        arrow::field("x", arrow::float64(), false),
        arrow::field("y", arrow::float64(), false)});
    return t;
}

static std::shared_ptr<arrow::Schema> rich_header_schema() {
    static auto s = arrow::schema({
        arrow::field("str_field", arrow::utf8(), false),
        arrow::field("bytes_field", arrow::binary(), false),
        arrow::field("int_field", arrow::int64(), false),
        arrow::field("float_field", arrow::float64(), false),
        arrow::field("bool_field", arrow::boolean(), false),
        arrow::field("list_of_int", arrow::list(arrow::int64()), false),
        arrow::field("list_of_str", arrow::list(arrow::utf8()), false),
        arrow::field("dict_field", arrow::map(arrow::utf8(), arrow::int64()), false),
        arrow::field("enum_field", arrow::dictionary(arrow::int16(), arrow::utf8()), false),
        arrow::field("nested_point", point_struct_type(), false),
        arrow::field("optional_str", arrow::utf8(), true),
        arrow::field("optional_int", arrow::int64(), true),
        arrow::field("optional_nested", point_struct_type(), true),
        arrow::field("list_of_nested", arrow::list(point_struct_type()), false),
        arrow::field("nested_list", arrow::list(arrow::list(arrow::int64())), false),
        arrow::field("annotated_int32", arrow::int32(), false),
        arrow::field("annotated_float32", arrow::float32(), false),
        arrow::field("dict_str_str", arrow::map(arrow::utf8(), arrow::utf8()), false),
    });
    return s;
}

// Build a single-row RichHeader batch deterministically from `seed`.
// Mirrors vgi_rpc.conformance._types.build_rich_header.
static std::shared_ptr<arrow::RecordBatch> make_rich_header_batch(int64_t seed) {
    auto* pool = arrow::default_memory_pool();

    // str_field
    arrow::StringBuilder str_b;
    VGI_RPC_THROW_NOT_OK(str_b.Append("seed-" + std::to_string(seed)));

    // bytes_field: bytes([seed%256, (seed+1)%256, (seed+2)%256])
    arrow::BinaryBuilder bytes_b;
    {
        uint8_t bs[3] = {static_cast<uint8_t>(((seed % 256) + 256) % 256),
                         static_cast<uint8_t>((((seed + 1) % 256) + 256) % 256),
                         static_cast<uint8_t>((((seed + 2) % 256) + 256) % 256)};
        VGI_RPC_THROW_NOT_OK(bytes_b.Append(bs, 3));
    }

    arrow::Int64Builder int_b;
    VGI_RPC_THROW_NOT_OK(int_b.Append(seed * 7));

    arrow::DoubleBuilder float_b;
    VGI_RPC_THROW_NOT_OK(float_b.Append(static_cast<double>(seed) * 1.5));

    arrow::BooleanBuilder bool_b;
    VGI_RPC_THROW_NOT_OK(bool_b.Append(seed % 2 == 0));

    // list_of_int: [seed, seed+1, seed+2]
    arrow::ListBuilder loi_b(pool, std::make_shared<arrow::Int64Builder>(pool));
    {
        auto* vb = static_cast<arrow::Int64Builder*>(loi_b.value_builder());
        VGI_RPC_THROW_NOT_OK(loi_b.Append());
        VGI_RPC_THROW_NOT_OK(vb->Append(seed));
        VGI_RPC_THROW_NOT_OK(vb->Append(seed + 1));
        VGI_RPC_THROW_NOT_OK(vb->Append(seed + 2));
    }

    // list_of_str: ["item-seed", "item-seed+1"]
    arrow::ListBuilder los_b(pool, std::make_shared<arrow::StringBuilder>(pool));
    {
        auto* vb = static_cast<arrow::StringBuilder*>(los_b.value_builder());
        VGI_RPC_THROW_NOT_OK(los_b.Append());
        VGI_RPC_THROW_NOT_OK(vb->Append("item-" + std::to_string(seed)));
        VGI_RPC_THROW_NOT_OK(vb->Append("item-" + std::to_string(seed + 1)));
    }

    // dict_field: {"a": seed, "b": seed+1}
    auto df_key = std::make_shared<arrow::StringBuilder>(pool);
    auto df_item = std::make_shared<arrow::Int64Builder>(pool);
    arrow::MapBuilder df_b(pool, df_key, df_item);
    {
        VGI_RPC_THROW_NOT_OK(df_b.Append());
        VGI_RPC_THROW_NOT_OK(df_key->Append("a"));
        VGI_RPC_THROW_NOT_OK(df_item->Append(seed));
        VGI_RPC_THROW_NOT_OK(df_key->Append("b"));
        VGI_RPC_THROW_NOT_OK(df_item->Append(seed + 1));
    }

    // enum_field: STATUS_CYCLE[seed%3] -> name
    const char* status_names[3] = {"PENDING", "ACTIVE", "CLOSED"};
    std::shared_ptr<arrow::Array> enum_arr;
    {
        int64_t idx = ((seed % 3) + 3) % 3;
        arrow::StringBuilder dv;
        VGI_RPC_THROW_NOT_OK(dv.Append(status_names[idx]));
        arrow::Int16Builder iv;
        VGI_RPC_THROW_NOT_OK(iv.Append(0));
        enum_arr = unwrap(arrow::DictionaryArray::FromArrays(
            arrow::dictionary(arrow::int16(), arrow::utf8()),
            unwrap(iv.Finish()), unwrap(dv.Finish())));
    }

    // nested_point: Point(seed, seed*2)
    auto np_x = std::make_shared<arrow::DoubleBuilder>(pool);
    auto np_y = std::make_shared<arrow::DoubleBuilder>(pool);
    arrow::StructBuilder np_b(point_struct_type(), pool,
        std::vector<std::shared_ptr<arrow::ArrayBuilder>>{np_x, np_y});
    {
        VGI_RPC_THROW_NOT_OK(np_b.Append());
        VGI_RPC_THROW_NOT_OK(np_x->Append(static_cast<double>(seed)));
        VGI_RPC_THROW_NOT_OK(np_y->Append(static_cast<double>(seed * 2)));
    }

    // optional_str: "opt-seed" if seed%2==0 else None
    arrow::StringBuilder os_b;
    if (seed % 2 == 0) {
        VGI_RPC_THROW_NOT_OK(os_b.Append("opt-" + std::to_string(seed)));
    } else {
        VGI_RPC_THROW_NOT_OK(os_b.AppendNull());
    }

    // optional_int: seed*3 if seed%2==1 else None
    arrow::Int64Builder oi_b;
    if (seed % 2 == 1) {
        VGI_RPC_THROW_NOT_OK(oi_b.Append(seed * 3));
    } else {
        VGI_RPC_THROW_NOT_OK(oi_b.AppendNull());
    }

    // optional_nested: Point(seed, 0.0) if seed%3==0 else None
    auto on_x = std::make_shared<arrow::DoubleBuilder>(pool);
    auto on_y = std::make_shared<arrow::DoubleBuilder>(pool);
    arrow::StructBuilder on_b(point_struct_type(), pool,
        std::vector<std::shared_ptr<arrow::ArrayBuilder>>{on_x, on_y});
    if (seed % 3 == 0) {
        VGI_RPC_THROW_NOT_OK(on_b.Append());
        VGI_RPC_THROW_NOT_OK(on_x->Append(static_cast<double>(seed)));
        VGI_RPC_THROW_NOT_OK(on_y->Append(0.0));
    } else {
        VGI_RPC_THROW_NOT_OK(on_b.AppendNull());
    }

    // list_of_nested: [Point(seed, seed+1)]
    auto ln_x = std::make_shared<arrow::DoubleBuilder>(pool);
    auto ln_y = std::make_shared<arrow::DoubleBuilder>(pool);
    auto ln_struct = std::make_shared<arrow::StructBuilder>(point_struct_type(), pool,
        std::vector<std::shared_ptr<arrow::ArrayBuilder>>{ln_x, ln_y});
    arrow::ListBuilder ln_b(pool, ln_struct);
    {
        VGI_RPC_THROW_NOT_OK(ln_b.Append());
        VGI_RPC_THROW_NOT_OK(ln_struct->Append());
        VGI_RPC_THROW_NOT_OK(ln_x->Append(static_cast<double>(seed)));
        VGI_RPC_THROW_NOT_OK(ln_y->Append(static_cast<double>(seed + 1)));
    }

    // nested_list: [[seed, seed+1], [seed+2]]
    auto nl_inner_vb = std::make_shared<arrow::Int64Builder>(pool);
    auto nl_inner = std::make_shared<arrow::ListBuilder>(pool, nl_inner_vb);
    arrow::ListBuilder nl_b(pool, nl_inner);
    {
        VGI_RPC_THROW_NOT_OK(nl_b.Append());
        VGI_RPC_THROW_NOT_OK(nl_inner->Append());
        VGI_RPC_THROW_NOT_OK(nl_inner_vb->Append(seed));
        VGI_RPC_THROW_NOT_OK(nl_inner_vb->Append(seed + 1));
        VGI_RPC_THROW_NOT_OK(nl_inner->Append());
        VGI_RPC_THROW_NOT_OK(nl_inner_vb->Append(seed + 2));
    }

    arrow::Int32Builder ai_b;
    VGI_RPC_THROW_NOT_OK(ai_b.Append(static_cast<int32_t>(((seed % 1000) + 1000) % 1000)));

    arrow::FloatBuilder af_b;
    VGI_RPC_THROW_NOT_OK(af_b.Append(static_cast<float>(static_cast<double>(seed) / 3.0)));

    // dict_str_str: {"key": "val-seed"}
    auto dss_key = std::make_shared<arrow::StringBuilder>(pool);
    auto dss_item = std::make_shared<arrow::StringBuilder>(pool);
    arrow::MapBuilder dss_b(pool, dss_key, dss_item);
    {
        VGI_RPC_THROW_NOT_OK(dss_b.Append());
        VGI_RPC_THROW_NOT_OK(dss_key->Append("key"));
        VGI_RPC_THROW_NOT_OK(dss_item->Append("val-" + std::to_string(seed)));
    }

    std::vector<std::shared_ptr<arrow::Array>> cols = {
        unwrap(str_b.Finish()), unwrap(bytes_b.Finish()), unwrap(int_b.Finish()),
        unwrap(float_b.Finish()), unwrap(bool_b.Finish()), unwrap(loi_b.Finish()),
        unwrap(los_b.Finish()), unwrap(df_b.Finish()), enum_arr,
        unwrap(np_b.Finish()), unwrap(os_b.Finish()), unwrap(oi_b.Finish()),
        unwrap(on_b.Finish()), unwrap(ln_b.Finish()), unwrap(nl_b.Finish()),
        unwrap(ai_b.Finish()), unwrap(af_b.Finish()), unwrap(dss_b.Finish())};

    return arrow::RecordBatch::Make(rich_header_schema(), 1, std::move(cols));
}

// =========================================================================
// Cancellation probe (process-global counters)
// =========================================================================

struct CancelProbe {
    int64_t produce_calls = 0;
    int64_t exchange_calls = 0;
    int64_t on_cancel_calls = 0;
};
static CancelProbe g_cancel_probe;

// =========================================================================
// Unary handlers: scalar echo
// =========================================================================

static Result echo_string_handler(const Request& req, CallContext&) {
    return echo_column(req, "value", str_result_schema());
}
static Result echo_bytes_handler(const Request& req, CallContext&) {
    return echo_column(req, "data", bytes_result_schema());
}
static Result echo_int_handler(const Request& req, CallContext&) {
    return echo_column(req, "value", int_result_schema());
}
static Result echo_float_handler(const Request& req, CallContext&) {
    return echo_column(req, "value", float_result_schema());
}
static Result echo_bool_handler(const Request& req, CallContext&) {
    return echo_column(req, "value", bool_result_schema());
}

static Result oversized_unary_handler(const Request& req, CallContext&) {
    auto n = req.get<int64_t>("target_bytes");
    if (n < 0) throw std::invalid_argument("target_bytes must be non-negative");
    // A single binary() value uses int32 offsets and cannot exceed 2 GiB; reject
    // rather than silently truncating via the int32 length cast below.
    if (n > std::numeric_limits<int32_t>::max()) {
        throw std::invalid_argument("target_bytes exceeds binary array capacity");
    }
    std::string zeros(static_cast<size_t>(n), '\0');
    arrow::BinaryBuilder b;
    VGI_RPC_THROW_NOT_OK(b.Append(zeros.data(), static_cast<int32_t>(n)));
    return Result::value(bytes_result_schema(), {unwrap(b.Finish())});
}

// =========================================================================
// Unary handlers: void
// =========================================================================

static void void_noop_handler(const Request&, CallContext&) {}
static void void_with_param_handler(const Request&, CallContext&) {}

// =========================================================================
// Unary handlers: complex type echo
// =========================================================================

static Result echo_enum_handler(const Request& req, CallContext&) {
    return echo_column(req, "status", enum_result_schema());
}
static Result echo_list_handler(const Request& req, CallContext&) {
    return echo_column(req, "values", list_str_result_schema());
}
static Result echo_dict_handler(const Request& req, CallContext&) {
    return echo_column(req, "mapping", dict_str_int_result_schema());
}
static Result echo_nested_list_handler(const Request& req, CallContext&) {
    return echo_column(req, "matrix", nested_list_result_schema());
}

// =========================================================================
// Unary handlers: optional/nullable
// =========================================================================

static Result echo_optional_string_handler(const Request& req, CallContext&) {
    return echo_column(req, "value", optional_str_result_schema());
}
static Result echo_optional_int_handler(const Request& req, CallContext&) {
    return echo_column(req, "value", optional_int_result_schema());
}

// =========================================================================
// Unary handlers: dataclass round-trip (binary passthrough)
// =========================================================================

static Result echo_point_handler(const Request& req, CallContext&) {
    return echo_column(req, "point", binary_result_schema());
}
static Result echo_all_types_handler(const Request& req, CallContext&) {
    return echo_column(req, "data", binary_result_schema());
}
static Result echo_bounding_box_handler(const Request& req, CallContext&) {
    return echo_column(req, "box", binary_result_schema());
}
static Result echo_wide_types_handler(const Request& req, CallContext&) {
    return echo_column(req, "data", binary_result_schema());
}
static Result echo_container_wide_types_handler(const Request& req, CallContext&) {
    return echo_column(req, "data", binary_result_schema());
}
static Result echo_embedded_arrow_handler(const Request& req, CallContext&) {
    return echo_column(req, "data", binary_result_schema());
}
static Result echo_deep_nested_handler(const Request& req, CallContext&) {
    return echo_column(req, "data", binary_result_schema());
}

// =========================================================================
// Unary handlers: dataclass as parameter
// =========================================================================

static Result inspect_point_handler(const Request& req, CallContext&) {
    auto batch = deserialize_dataclass(req, "point");
    auto x_col = checked_cast_column<arrow::DoubleArray>(batch, "x");
    auto y_col = checked_cast_column<arrow::DoubleArray>(batch, "y");
    auto result_str = std::format("Point({}, {})",
        fmt_py_double(x_col->Value(0)), fmt_py_double(y_col->Value(0)));
    arrow::StringBuilder builder;
    VGI_RPC_THROW_NOT_OK(builder.Append(result_str));
    return Result::value(str_result_schema(), {unwrap(builder.Finish())});
}

// =========================================================================
// Unary handlers: annotated / wide Arrow type echo (passthrough)
// =========================================================================

static Result echo_int32_handler(const Request& req, CallContext&) {
    return echo_column(req, "value", int32_result_schema());
}
static Result echo_float32_handler(const Request& req, CallContext&) {
    return echo_column(req, "value", float32_result_schema());
}
static Result echo_int8_handler(const Request& req, CallContext&) {
    return echo_column(req, "value", result_schema_of(arrow::int8()));
}
static Result echo_int16_handler(const Request& req, CallContext&) {
    return echo_column(req, "value", result_schema_of(arrow::int16()));
}
static Result echo_uint8_handler(const Request& req, CallContext&) {
    return echo_column(req, "value", result_schema_of(arrow::uint8()));
}
static Result echo_uint16_handler(const Request& req, CallContext&) {
    return echo_column(req, "value", result_schema_of(arrow::uint16()));
}
static Result echo_uint32_handler(const Request& req, CallContext&) {
    return echo_column(req, "value", result_schema_of(arrow::uint32()));
}
static Result echo_uint64_handler(const Request& req, CallContext&) {
    return echo_column(req, "value", result_schema_of(arrow::uint64()));
}
static Result echo_date_handler(const Request& req, CallContext&) {
    return echo_column(req, "value", result_schema_of(arrow::date32()));
}
static Result echo_timestamp_handler(const Request& req, CallContext&) {
    return echo_column(req, "value", result_schema_of(arrow::timestamp(arrow::TimeUnit::MICRO)));
}
static Result echo_timestamp_utc_handler(const Request& req, CallContext&) {
    return echo_column(req, "value",
        result_schema_of(arrow::timestamp(arrow::TimeUnit::MICRO, "UTC")));
}
static Result echo_time_handler(const Request& req, CallContext&) {
    return echo_column(req, "value", result_schema_of(arrow::time64(arrow::TimeUnit::MICRO)));
}
static Result echo_duration_handler(const Request& req, CallContext&) {
    return echo_column(req, "value", result_schema_of(arrow::duration(arrow::TimeUnit::MICRO)));
}
static Result echo_decimal_handler(const Request& req, CallContext&) {
    return echo_column(req, "value", result_schema_of(arrow::decimal128(20, 4)));
}
static Result echo_large_string_handler(const Request& req, CallContext&) {
    return echo_column(req, "value", result_schema_of(arrow::large_utf8()));
}
static Result echo_large_binary_handler(const Request& req, CallContext&) {
    return echo_column(req, "value", result_schema_of(arrow::large_binary()));
}
static Result echo_fixed_binary_handler(const Request& req, CallContext&) {
    return echo_column(req, "value", result_schema_of(arrow::fixed_size_binary(8)));
}
static Result echo_dict_encoded_string_handler(const Request& req, CallContext&) {
    return echo_column(req, "value",
        result_schema_of(arrow::dictionary(arrow::int16(), arrow::utf8())));
}

// =========================================================================
// Unary handlers: multi-param & defaults
// =========================================================================

static Result add_floats_handler(const Request& req, CallContext&) {
    auto a = req.get<double>("a");
    auto b = req.get<double>("b");
    arrow::DoubleBuilder builder;
    VGI_RPC_THROW_NOT_OK(builder.Append(a + b));
    return Result::value(float_result_schema(), {unwrap(builder.Finish())});
}

static Result concatenate_handler(const Request& req, CallContext&) {
    auto prefix = req.get<std::string>("prefix");
    auto suffix = req.get<std::string>("suffix");
    auto separator = req.get<std::string>("separator");
    arrow::StringBuilder builder;
    VGI_RPC_THROW_NOT_OK(builder.Append(prefix + separator + suffix));
    return Result::value(str_result_schema(), {unwrap(builder.Finish())});
}

static Result with_defaults_handler(const Request& req, CallContext&) {
    auto required = req.get<int64_t>("required");
    auto optional_str = req.get<std::string>("optional_str");
    auto optional_int = req.get<int64_t>("optional_int");
    auto result = std::format("required={}, optional_str={}, optional_int={}",
                              required, optional_str, optional_int);
    arrow::StringBuilder builder;
    VGI_RPC_THROW_NOT_OK(builder.Append(result));
    return Result::value(str_result_schema(), {unwrap(builder.Finish())});
}

// =========================================================================
// Unary handlers: error propagation
// =========================================================================

static Result raise_value_error_handler(const Request& req, CallContext&) {
    throw std::invalid_argument(req.get<std::string>("message"));
}
static Result raise_runtime_error_handler(const Request& req, CallContext&) {
    throw std::runtime_error(req.get<std::string>("message"));
}
static Result raise_type_error_handler(const Request& req, CallContext&) {
    throw std::logic_error(req.get<std::string>("message"));
}

// =========================================================================
// Unary handlers: client-directed logging
// =========================================================================

static Result echo_with_info_log_handler(const Request& req, CallContext& ctx) {
    auto value = req.get<std::string>("value");
    ctx.client_log(LogLevel::INFO, "info: " + value);
    arrow::StringBuilder builder;
    VGI_RPC_THROW_NOT_OK(builder.Append(value));
    return Result::value(str_result_schema(), {unwrap(builder.Finish())});
}

static Result echo_with_multi_logs_handler(const Request& req, CallContext& ctx) {
    auto value = req.get<std::string>("value");
    ctx.client_log(LogLevel::DEBUG, "debug: " + value);
    ctx.client_log(LogLevel::INFO, "info: " + value);
    ctx.client_log(LogLevel::WARN, "warn: " + value);
    arrow::StringBuilder builder;
    VGI_RPC_THROW_NOT_OK(builder.Append(value));
    return Result::value(str_result_schema(), {unwrap(builder.Finish())});
}

static Result echo_with_log_extras_handler(const Request& req, CallContext& ctx) {
    auto value = req.get<std::string>("value");
    nlohmann::json extras;
    extras["source"] = "conformance";
    extras["detail"] = value;
    ctx.client_log(LogLevel::INFO, "info: " + value, extras);
    arrow::StringBuilder builder;
    VGI_RPC_THROW_NOT_OK(builder.Append(value));
    return Result::value(str_result_schema(), {unwrap(builder.Finish())});
}

static Result echo_with_all_log_levels_handler(const Request& req, CallContext& ctx) {
    auto value = req.get<std::string>("value");
    ctx.client_log(LogLevel::TRACE, "trace: " + value);
    ctx.client_log(LogLevel::DEBUG, "debug: " + value);
    ctx.client_log(LogLevel::INFO, "info: " + value);
    ctx.client_log(LogLevel::WARN, "warn: " + value);
    ctx.client_log(LogLevel::ERROR, "error: " + value);
    arrow::StringBuilder builder;
    VGI_RPC_THROW_NOT_OK(builder.Append(value));
    return Result::value(str_result_schema(), {unwrap(builder.Finish())});
}

// =========================================================================
// Unary handlers: cancellation probe
// =========================================================================

static Result cancel_probe_counters_handler(const Request&, CallContext&) {
    auto* pool = arrow::default_memory_pool();
    arrow::ListBuilder lb(pool, std::make_shared<arrow::Int64Builder>(pool));
    auto* vb = static_cast<arrow::Int64Builder*>(lb.value_builder());
    VGI_RPC_THROW_NOT_OK(lb.Append());
    VGI_RPC_THROW_NOT_OK(vb->Append(g_cancel_probe.produce_calls));
    VGI_RPC_THROW_NOT_OK(vb->Append(g_cancel_probe.exchange_calls));
    VGI_RPC_THROW_NOT_OK(vb->Append(g_cancel_probe.on_cancel_calls));
    return Result::value(list_int_result_schema(), {unwrap(lb.Finish())});
}

static void reset_cancel_probe_handler(const Request&, CallContext&) {
    g_cancel_probe = CancelProbe{};
}

// =========================================================================
// Sticky-session unary handlers (HTTP-only; not exercised over pipe).
// Implemented with a single process-global counter for robustness.
// =========================================================================

// The handle-bearing object a sticky session binds.  Trivial on purpose: the
// feature under test is that *this* object comes back on the next request to
// the same worker, not what it holds.
class StickyCounter : public SessionState {
public:
    explicit StickyCounter(int64_t value) : value(value) {}
    int64_t value;
};

// Resolve the counter bound to this request, or say plainly that none is.
static StickyCounter& require_counter(CallContext& ctx) {
    auto* counter = dynamic_cast<StickyCounter*>(ctx.session().get());
    if (!counter) throw std::runtime_error("no sticky counter bound to this request");
    return *counter;
}

static Result open_counter_handler(const Request& req, CallContext& ctx) {
    auto initial = req.get<int64_t>("initial");
    ctx.open_session(std::make_shared<StickyCounter>(initial));
    arrow::Int64Builder b;
    VGI_RPC_THROW_NOT_OK(b.Append(initial));
    return Result::value(int_result_schema(), {unwrap(b.Finish())});
}
static Result increment_counter_handler(const Request& req, CallContext& ctx) {
    auto& counter = require_counter(ctx);
    counter.value += req.get<int64_t>("by");
    arrow::Int64Builder b;
    VGI_RPC_THROW_NOT_OK(b.Append(counter.value));
    return Result::value(int_result_schema(), {unwrap(b.Finish())});
}
static Result close_counter_handler(const Request&, CallContext& ctx) {
    const int64_t final_value = require_counter(ctx).value;
    ctx.close_session();
    arrow::Int64Builder b;
    VGI_RPC_THROW_NOT_OK(b.Append(final_value));
    return Result::value(int_result_schema(), {unwrap(b.Finish())});
}

// =========================================================================
// Producer stream states
// =========================================================================

class CounterState : public ProducerState {
public:
    CounterState(int64_t count) : count_(count) {}
    void produce(OutputCollector& out, CallContext&) override {
        if (current_ >= count_) { out.finish(); return; }
        arrow::Int64Builder idx, val;
        VGI_RPC_THROW_NOT_OK(idx.Append(current_));
        VGI_RPC_THROW_NOT_OK(val.Append(current_ * 10));
        out.emit_arrays({unwrap(idx.Finish()), unwrap(val.Finish())});
        ++current_;
    }
private:
    int64_t count_, current_ = 0;
};

class EmptyProducerState : public ProducerState {
public:
    void produce(OutputCollector& out, CallContext&) override { out.finish(); }
};

class SingleProducerState : public ProducerState {
public:
    void produce(OutputCollector& out, CallContext&) override {
        if (emitted_) { out.finish(); return; }
        emitted_ = true;
        arrow::Int64Builder idx, val;
        VGI_RPC_THROW_NOT_OK(idx.Append(0));
        VGI_RPC_THROW_NOT_OK(val.Append(0));
        out.emit_arrays({unwrap(idx.Finish()), unwrap(val.Finish())});
    }
private:
    bool emitted_ = false;
};

class LargeProducerState : public ProducerState {
public:
    LargeProducerState(int64_t rows_per_batch, int64_t batch_count)
        : rows_per_batch_(rows_per_batch), batch_count_(batch_count) {}
    void produce(OutputCollector& out, CallContext&) override {
        if (current_ >= batch_count_) { out.finish(); return; }
        int64_t offset = current_ * rows_per_batch_;
        arrow::Int64Builder idx, val;
        for (int64_t i = 0; i < rows_per_batch_; ++i) {
            VGI_RPC_THROW_NOT_OK(idx.Append(offset + i));
            VGI_RPC_THROW_NOT_OK(val.Append((offset + i) * 10));
        }
        out.emit_arrays({unwrap(idx.Finish()), unwrap(val.Finish())});
        ++current_;
    }
private:
    int64_t rows_per_batch_, batch_count_, current_ = 0;
};

// One oversized batch of rows_per_batch {index, value} rows, then finish.
class OversizedBatchState : public ProducerState {
public:
    OversizedBatchState(int64_t rows_per_batch) : rows_per_batch_(rows_per_batch) {}
    void produce(OutputCollector& out, CallContext&) override {
        if (emitted_) { out.finish(); return; }
        emitted_ = true;
        arrow::Int64Builder idx, val;
        for (int64_t i = 0; i < rows_per_batch_; ++i) {
            VGI_RPC_THROW_NOT_OK(idx.Append(i));
            VGI_RPC_THROW_NOT_OK(val.Append(i * 10));
        }
        out.emit_arrays({unwrap(idx.Finish()), unwrap(val.Finish())});
    }
private:
    int64_t rows_per_batch_;
    bool emitted_ = false;
};

class LoggingProducerState : public ProducerState {
public:
    LoggingProducerState(int64_t count) : count_(count) {}
    void produce(OutputCollector& out, CallContext&) override {
        if (current_ >= count_) { out.finish(); return; }
        out.client_log(LogLevel::INFO, std::format("producing batch {}", current_));
        arrow::Int64Builder idx, val;
        VGI_RPC_THROW_NOT_OK(idx.Append(current_));
        VGI_RPC_THROW_NOT_OK(val.Append(current_ * 10));
        out.emit_arrays({unwrap(idx.Finish()), unwrap(val.Finish())});
        ++current_;
    }
private:
    int64_t count_, current_ = 0;
};

class ErrorAfterNState : public ProducerState {
public:
    ErrorAfterNState(int64_t n) : n_(n) {}
    void produce(OutputCollector& out, CallContext&) override {
        if (current_ >= n_) {
            throw std::runtime_error(std::format("intentional error after {} batches", n_));
        }
        arrow::Int64Builder idx, val;
        VGI_RPC_THROW_NOT_OK(idx.Append(current_));
        VGI_RPC_THROW_NOT_OK(val.Append(current_ * 10));
        out.emit_arrays({unwrap(idx.Finish()), unwrap(val.Finish())});
        ++current_;
    }
private:
    int64_t n_, current_ = 0;
};

// Producer used with stream headers — same emission as CounterState.
class HeaderProducerState : public ProducerState {
public:
    HeaderProducerState(int64_t count) : count_(count) {}
    void produce(OutputCollector& out, CallContext&) override {
        if (current_ >= count_) { out.finish(); return; }
        arrow::Int64Builder idx, val;
        VGI_RPC_THROW_NOT_OK(idx.Append(current_));
        VGI_RPC_THROW_NOT_OK(val.Append(current_ * 10));
        out.emit_arrays({unwrap(idx.Finish()), unwrap(val.Finish())});
        ++current_;
    }
private:
    int64_t count_, current_ = 0;
};

// Dynamic-schema producer: output columns depend on flags.
class DynamicProducerState : public ProducerState {
public:
    DynamicProducerState(int64_t count, bool include_strings, bool include_floats,
                         std::shared_ptr<arrow::Schema> schema)
        : count_(count), include_strings_(include_strings),
          include_floats_(include_floats), schema_(std::move(schema)) {}
    void produce(OutputCollector& out, CallContext&) override {
        if (current_ >= count_) { out.finish(); return; }
        std::vector<std::shared_ptr<arrow::Array>> cols;
        arrow::Int64Builder idx;
        VGI_RPC_THROW_NOT_OK(idx.Append(current_));
        cols.push_back(unwrap(idx.Finish()));
        if (include_strings_) {
            arrow::StringBuilder lbl;
            VGI_RPC_THROW_NOT_OK(lbl.Append("row-" + std::to_string(current_)));
            cols.push_back(unwrap(lbl.Finish()));
        }
        if (include_floats_) {
            arrow::DoubleBuilder score;
            VGI_RPC_THROW_NOT_OK(score.Append(static_cast<double>(current_) * 1.5));
            cols.push_back(unwrap(score.Finish()));
        }
        out.emit_batch(arrow::RecordBatch::Make(schema_, 1, std::move(cols)));
        ++current_;
    }
private:
    int64_t count_, current_ = 0;
    bool include_strings_, include_floats_;
    std::shared_ptr<arrow::Schema> schema_;
};

// Infinite producer used by cancel() conformance tests.
class CancellableProducerState : public ProducerState {
public:
    void produce(OutputCollector& out, CallContext&) override {
        ++g_cancel_probe.produce_calls;
        arrow::Int64Builder idx, val;
        VGI_RPC_THROW_NOT_OK(idx.Append(current_));
        VGI_RPC_THROW_NOT_OK(val.Append(current_ * 10));
        out.emit_arrays({unwrap(idx.Finish()), unwrap(val.Finish())});
        ++current_;
    }
    void on_cancel(CallContext&) override { ++g_cancel_probe.on_cancel_calls; }
private:
    int64_t current_ = 0;
};

// Sticky-session producer (HTTP-only; not exercised over pipe).
class SessionCounterProducerState : public ProducerState {
public:
    SessionCounterProducerState(int64_t count) : count_(count) {}
    void produce(OutputCollector& out, CallContext& ctx) override {
        if (current_ >= count_) { out.finish(); return; }
        // Resolved per turn, not captured at init: across HTTP turns the
        // sticky middleware rebinds the session on every request, which is
        // exactly the property this exercises.
        auto& counter = require_counter(ctx);
        ++counter.value;
        arrow::Int64Builder val;
        VGI_RPC_THROW_NOT_OK(val.Append(counter.value));
        out.emit_arrays({unwrap(val.Finish())});
        ++current_;
    }
private:
    int64_t count_, current_ = 0;
};

// =========================================================================
// Exchange stream states
// =========================================================================

class ScaleExchangeState : public ExchangeState {
public:
    ScaleExchangeState(double factor) : factor_(factor) {}
    void exchange(const AnnotatedBatch& input, OutputCollector& out, CallContext&) override {
        auto col = checked_cast_column<arrow::DoubleArray>(input.batch, "value");
        arrow::DoubleBuilder builder;
        for (int64_t i = 0; i < col->length(); ++i) {
            VGI_RPC_THROW_NOT_OK(builder.Append(col->Value(i) * factor_));
        }
        out.emit_arrays({unwrap(builder.Finish())});
    }
private:
    double factor_;
};

class AccumulatingExchangeState : public ExchangeState {
public:
    void exchange(const AnnotatedBatch& input, OutputCollector& out, CallContext&) override {
        auto col = checked_cast_column<arrow::DoubleArray>(input.batch, "value");
        for (int64_t i = 0; i < col->length(); ++i) running_sum_ += col->Value(i);
        ++exchange_count_;
        arrow::DoubleBuilder sum_builder;
        arrow::Int64Builder count_builder;
        VGI_RPC_THROW_NOT_OK(sum_builder.Append(running_sum_));
        VGI_RPC_THROW_NOT_OK(count_builder.Append(exchange_count_));
        out.emit_arrays({unwrap(sum_builder.Finish()), unwrap(count_builder.Finish())});
    }
private:
    double running_sum_ = 0.0;
    int64_t exchange_count_ = 0;
};

class LoggingExchangeState : public ExchangeState {
public:
    void exchange(const AnnotatedBatch& input, OutputCollector& out, CallContext&) override {
        out.client_log(LogLevel::INFO, "exchange processing");
        out.client_log(LogLevel::DEBUG, "exchange debug");
        out.emit_batch(input.batch);
    }
};

class ZeroColumnExchangeState : public ExchangeState {
public:
    void exchange(const AnnotatedBatch&, OutputCollector& out, CallContext&) override {
        out.emit_batch(arrow::RecordBatch::Make(empty_schema(), 0,
            std::vector<std::shared_ptr<arrow::Array>>{}));
    }
};

class FailOnExchangeNState : public ExchangeState {
public:
    FailOnExchangeNState(int64_t fail_on) : fail_on_(fail_on) {}
    void exchange(const AnnotatedBatch& input, OutputCollector& out, CallContext&) override {
        ++exchange_count_;
        if (exchange_count_ >= fail_on_) {
            throw std::runtime_error(
                std::format("intentional error on exchange {}", exchange_count_));
        }
        out.emit_batch(input.batch);
    }
private:
    int64_t fail_on_;
    int64_t exchange_count_ = 0;
};

// Emits an oversized {index, value} batch for any input (HTTP-only test).
class OversizedExchangeState : public ExchangeState {
public:
    OversizedExchangeState(int64_t rows_per_batch) : rows_per_batch_(rows_per_batch) {}
    void exchange(const AnnotatedBatch&, OutputCollector& out, CallContext&) override {
        arrow::Int64Builder idx, val;
        for (int64_t i = 0; i < rows_per_batch_; ++i) {
            VGI_RPC_THROW_NOT_OK(idx.Append(i));
            VGI_RPC_THROW_NOT_OK(val.Append(i * 10));
        }
        out.emit_arrays({unwrap(idx.Finish()), unwrap(val.Finish())});
    }
private:
    int64_t rows_per_batch_;
};

// Echo exchange used by cancel() conformance tests.
class CancellableExchangeState : public ExchangeState {
public:
    void exchange(const AnnotatedBatch& input, OutputCollector& out, CallContext&) override {
        ++g_cancel_probe.exchange_calls;
        out.emit_batch(input.batch);
    }
    void on_cancel(CallContext&) override { ++g_cancel_probe.on_cancel_calls; }
};

// Sticky-session exchange (HTTP-only; not exercised over pipe).
class SessionCounterExchangeState : public ExchangeState {
public:
    void exchange(const AnnotatedBatch& input, OutputCollector& out, CallContext& ctx) override {
        auto& counter = require_counter(ctx);
        auto col = checked_cast_column<arrow::Int64Array>(input.batch, "by");
        for (int64_t i = 0; i < col->length(); ++i) counter.value += col->Value(i);
        arrow::Int64Builder val;
        VGI_RPC_THROW_NOT_OK(val.Append(counter.value));
        out.emit_arrays({unwrap(val.Finish())});
    }
};

// =========================================================================
// Producer stream factories
// =========================================================================

static Stream make_produce_n(const Request& req, CallContext&) {
    return {counter_schema(), empty_schema(),
            std::make_shared<CounterState>(req.get<int64_t>("count")), nullptr};
}
static Stream make_produce_empty(const Request&, CallContext&) {
    return {counter_schema(), empty_schema(),
            std::make_shared<EmptyProducerState>(), nullptr};
}
static Stream make_produce_single(const Request&, CallContext&) {
    return {counter_schema(), empty_schema(),
            std::make_shared<SingleProducerState>(), nullptr};
}
static Stream make_produce_large(const Request& req, CallContext&) {
    return {counter_schema(), empty_schema(),
            std::make_shared<LargeProducerState>(
                req.get<int64_t>("rows_per_batch"), req.get<int64_t>("batch_count")),
            nullptr};
}
static Stream make_produce_with_logs(const Request& req, CallContext&) {
    return {counter_schema(), empty_schema(),
            std::make_shared<LoggingProducerState>(req.get<int64_t>("count")), nullptr};
}
static Stream make_produce_error_mid(const Request& req, CallContext&) {
    return {counter_schema(), empty_schema(),
            std::make_shared<ErrorAfterNState>(req.get<int64_t>("emit_before_error")),
            nullptr};
}
static Stream make_produce_error_init(const Request&, CallContext&) {
    throw std::runtime_error("intentional init error");
}
static Stream make_produce_oversized_batch(const Request& req, CallContext&) {
    return {counter_schema(), empty_schema(),
            std::make_shared<OversizedBatchState>(req.get<int64_t>("rows_per_batch")),
            nullptr};
}

static Stream make_produce_with_header(const Request& req, CallContext&) {
    auto count = req.get<int64_t>("count");
    auto header = make_header_batch(count, std::format("producing {} batches", count));
    return {counter_schema(), empty_schema(),
            std::make_shared<HeaderProducerState>(count), header};
}
static Stream make_produce_with_header_and_logs(const Request& req, CallContext& ctx) {
    auto count = req.get<int64_t>("count");
    ctx.client_log(LogLevel::INFO, "stream init log");
    auto header = make_header_batch(count, std::format("producing {} with logs", count));
    return {counter_schema(), empty_schema(),
            std::make_shared<HeaderProducerState>(count), header};
}

static Stream make_produce_with_rich_header(const Request& req, CallContext&) {
    auto seed = req.get<int64_t>("seed");
    auto count = req.get<int64_t>("count");
    return {counter_schema(), empty_schema(),
            std::make_shared<HeaderProducerState>(count), make_rich_header_batch(seed)};
}

static Stream make_produce_dynamic_schema(const Request& req, CallContext&) {
    auto seed = req.get<int64_t>("seed");
    auto count = req.get<int64_t>("count");
    auto include_strings = req.get<bool>("include_strings");
    auto include_floats = req.get<bool>("include_floats");
    std::vector<std::shared_ptr<arrow::Field>> fields = {arrow::field("index", arrow::int64())};
    if (include_strings) fields.push_back(arrow::field("label", arrow::utf8()));
    if (include_floats) fields.push_back(arrow::field("score", arrow::float64()));
    auto out_schema = arrow::schema(fields);
    return {out_schema, empty_schema(),
            std::make_shared<DynamicProducerState>(count, include_strings, include_floats, out_schema),
            make_rich_header_batch(seed)};
}

static Stream make_cancellable_producer(const Request&, CallContext&) {
    return {counter_schema(), empty_schema(),
            std::make_shared<CancellableProducerState>(), nullptr};
}

static Stream make_stream_session_counter(const Request& req, CallContext&) {
    return {session_value_schema(), empty_schema(),
            std::make_shared<SessionCounterProducerState>(req.get<int64_t>("count")), nullptr};
}

// =========================================================================
// Exchange stream factories
// =========================================================================

static Stream make_exchange_scale(const Request& req, CallContext&) {
    return {scale_output_schema(), scale_input_schema(),
            std::make_shared<ScaleExchangeState>(req.get<double>("factor")), nullptr};
}
static Stream make_exchange_accumulate(const Request&, CallContext&) {
    return {accum_output_schema(), accum_input_schema(),
            std::make_shared<AccumulatingExchangeState>(), nullptr};
}
static Stream make_exchange_with_logs(const Request&, CallContext&) {
    return {scale_output_schema(), scale_input_schema(),
            std::make_shared<LoggingExchangeState>(), nullptr};
}
static Stream make_exchange_zero_columns(const Request&, CallContext&) {
    return {empty_schema(), empty_schema(),
            std::make_shared<ZeroColumnExchangeState>(), nullptr};
}
static Stream make_exchange_error_on_nth(const Request& req, CallContext&) {
    return {scale_output_schema(), scale_input_schema(),
            std::make_shared<FailOnExchangeNState>(req.get<int64_t>("fail_on")), nullptr};
}
static Stream make_exchange_cast_compatible(const Request&, CallContext&) {
    return {scale_output_schema(), scale_input_schema(),
            std::make_shared<ScaleExchangeState>(1.0), nullptr};
}
static Stream make_exchange_error_on_init(const Request&, CallContext&) {
    throw std::runtime_error("intentional exchange init error");
}
static Stream make_exchange_oversized(const Request& req, CallContext&) {
    return {counter_schema(), scale_input_schema(),
            std::make_shared<OversizedExchangeState>(req.get<int64_t>("rows_per_batch")),
            nullptr};
}
static Stream make_exchange_with_header(const Request& req, CallContext&) {
    auto factor = req.get<double>("factor");
    auto header = make_header_batch(0, "scale by " + fmt_py_double(factor));
    return {scale_output_schema(), scale_input_schema(),
            std::make_shared<ScaleExchangeState>(factor), header};
}
static Stream make_exchange_with_rich_header(const Request& req, CallContext&) {
    auto seed = req.get<int64_t>("seed");
    auto factor = req.get<double>("factor");
    return {scale_output_schema(), scale_input_schema(),
            std::make_shared<ScaleExchangeState>(factor), make_rich_header_batch(seed)};
}
static Stream make_cancellable_exchange(const Request&, CallContext&) {
    return {scale_output_schema(), scale_input_schema(),
            std::make_shared<CancellableExchangeState>(), nullptr};
}
static Stream make_exchange_session_counter(const Request&, CallContext&) {
    return {session_value_schema(), session_by_schema(),
            std::make_shared<SessionCounterExchangeState>(), nullptr};
}

// =========================================================================
// Main
// =========================================================================

static std::shared_ptr<arrow::Schema> params(
    std::vector<std::shared_ptr<arrow::Field>> fields) {
    return arrow::schema(std::move(fields));
}

int main(int argc, char** argv) {
    // Parse the conformance CLI surface.  --access-log and the HTTP flags are
    // acted on; other access-log tuning flags are accepted (and ignored) so the
    // worker stays launchable by the cross-language test harness.
    std::string access_log_path;
    bool http = false;
    bool unix_mode = false;
    std::string unix_path;
    bool tcp_mode = false;
    std::string server_id;
    vgi_rpc::HttpConfig http_cfg;
    // Sticky is on by default here, matching the reference conformance worker,
    // so the TestSticky group runs rather than skipping.  It stays off by
    // default in HttpConfig itself, where the library's posture belongs.
    http_cfg.sticky = true;
    http_cfg.test_drain_endpoint = true;
    bool sticky_echo = true;
    int64_t access_log_max_record_bytes = vgi_rpc::kDefaultMaxRecordBytes;
    if (const char* env = std::getenv("VGI_RPC_ACCESS_LOG_MAX_RECORD_BYTES")) {
        try {
            access_log_max_record_bytes = std::stoll(env);
        } catch (const std::exception&) {
            // Keep the default rather than refusing to start over a bad env var.
        }
    }
    auto take_value = [&](int& i) -> std::string {
        return (i + 1 < argc) ? std::string(argv[++i]) : std::string();
    };
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--access-log" && i + 1 < argc) {
            access_log_path = argv[++i];
        } else if (arg.rfind("--access-log=", 0) == 0) {
            access_log_path = arg.substr(std::string("--access-log=").size());
        } else if (arg == "--http") {
            http = true;
        } else if (arg == "--unix") {
            unix_mode = true;
            unix_path = take_value(i);
        } else if (arg == "--tcp") {
            // "[HOST:]PORT" — the host defaults to loopback, because TCP here
            // carries no auth or TLS and must not bind the world by accident.
            tcp_mode = true;
            const std::string spec = take_value(i);
            const size_t colon = spec.rfind(':');
            if (colon == std::string::npos) {
                http_cfg.port = std::stoi(spec);
            } else {
                http_cfg.host = spec.substr(0, colon);
                http_cfg.port = std::stoi(spec.substr(colon + 1));
            }
        } else if (arg == "--host") {
            http_cfg.host = take_value(i);
        } else if (arg == "--port") {
            http_cfg.port = std::stoi(take_value(i));
        } else if (arg == "--prefix") {
            http_cfg.prefix = take_value(i);
        } else if (arg == "--server-id") {
            server_id = take_value(i);
        } else if (arg == "--max-response-bytes") {
            http_cfg.max_response_bytes = std::stoll(take_value(i));
        } else if (arg == "--max-externalized-response-bytes") {
            http_cfg.max_externalized_response_bytes = std::stoll(take_value(i));
        } else if (arg == "--externalize-threshold") {
            http_cfg.externalize_threshold = std::stoll(take_value(i));
        } else if (arg == "--fake-storage") {
            http_cfg.external_storage_url = take_value(i);
        } else if (arg == "--externalize-compression") {
            http_cfg.externalize_compression = take_value(i);
        } else if (arg == "--no-compression") {
            http_cfg.compression = false;
        } else if (arg == "--cors-origin") {
            http_cfg.cors_origin = take_value(i);
        } else if (arg == "--introspect") {
            http_cfg.token_introspection = true;
        } else if (arg == "--sticky") {
            http_cfg.sticky = true;
        } else if (arg == "--no-sticky") {
            http_cfg.sticky = false;
        } else if (arg == "--no-sticky-echo") {
            sticky_echo = false;
        } else if (arg == "--sticky-ttl") {
            http_cfg.sticky = true;
            http_cfg.sticky_default_ttl = std::stoi(take_value(i));
        } else if (arg == "--sticky-auth") {
            http_cfg.sticky = true;
            http_cfg.sticky_header_auth = true;
        } else if (arg == "--token-key") {
            auto raw = vgi_rpc::crypto::hex_decode(take_value(i));
            if (!raw || raw->size() != vgi_rpc::crypto::kAeadKeyBytes) {
                std::cerr << "vgi_rpc: --token-key must be 64 hex characters\n";
                return 2;
            }
            std::copy(raw->begin(), raw->end(), http_cfg.token_key.begin());
        } else if (arg == "--no-call-state-cache") {
            http_cfg.call_state_cache = false;
        } else if (arg == "--auth-reject-all") {
            // Reject every RPC request; health stays reachable.  Also honours
            // X-Conformance-Auth-Reason so the discrimination tests can drive
            // each reason code — a fixture affordance, never production shape.
            http_cfg.reject_all = vgi_rpc::AuthReason::UNAUTHORIZED;
            http_cfg.honour_requested_auth_reason = true;
        } else if (arg == "--proof-mode") {
            const std::string mode = take_value(i);
            if (mode == "require") {
                http_cfg.proof_mode = vgi_rpc::ProofMode::REQUIRE;
            } else if (mode == "allow") {
                http_cfg.proof_mode = vgi_rpc::ProofMode::ALLOW;
            } else {
                http_cfg.proof_mode = vgi_rpc::ProofMode::OFF;
            }
        } else if (arg == "--proof-origin-id") {
            http_cfg.proof_origin_id = take_value(i);
        } else if (arg == "--proof-secrets") {
            http_cfg.proof_secrets = take_value(i);
        } else if (arg == "--proof-skew") {
            http_cfg.proof_skew_seconds = std::stoi(take_value(i));
        } else if (arg == "--proof-no-replay-cache") {
            http_cfg.proof_replay_cache = false;
        } else if (arg == "--access-log-max-record-bytes" && i + 1 < argc) {
            access_log_max_record_bytes = std::stoll(take_value(i));
        } else if ((arg == "--access-log-max-bytes" || arg == "--access-log-when" ||
                    arg == "--access-log-backup-count") && i + 1 < argc) {
            ++i;  // rotation knobs: accepted so the harness can launch us
        }
    }

    auto builder = ServerBuilder();

    // --- Scalar Echo ---
    builder
        .add_unary("echo_string", params({arrow::field("value", arrow::utf8())}),
            str_result_schema(), echo_string_handler, "Echo a string value.")
        .add_unary("echo_bytes", params({arrow::field("data", arrow::binary())}),
            bytes_result_schema(), echo_bytes_handler, "Echo a bytes value.")
        .add_unary("oversized_unary", params({arrow::field("target_bytes", arrow::int64())}),
            bytes_result_schema(), oversized_unary_handler,
            "Return a bytes payload of approximately target_bytes bytes.")
        .add_unary("echo_int", params({arrow::field("value", arrow::int64())}),
            int_result_schema(), echo_int_handler, "Echo an integer value.")
        .add_unary("echo_float", params({arrow::field("value", arrow::float64())}),
            float_result_schema(), echo_float_handler, "Echo a float value.")
        .add_unary("echo_bool", params({arrow::field("value", arrow::boolean())}),
            bool_result_schema(), echo_bool_handler, "Echo a boolean value.");

    // --- Void ---
    builder
        .add_void("void_noop", empty_schema(), void_noop_handler, "No-op returning void.")
        .add_void("void_with_param", params({arrow::field("value", arrow::int64())}),
            void_with_param_handler, "Accept a parameter, return void.");

    // --- Complex Type Echo ---
    builder
        .add_unary("echo_enum",
            params({arrow::field("status", arrow::dictionary(arrow::int16(), arrow::utf8()))}),
            enum_result_schema(), echo_enum_handler, "Echo an enum value.")
        .add_unary("echo_list", params({arrow::field("values", arrow::list(arrow::utf8()))}),
            list_str_result_schema(), echo_list_handler, "Echo a list of strings.")
        .add_unary("echo_dict",
            params({arrow::field("mapping", arrow::map(arrow::utf8(), arrow::int64()))}),
            dict_str_int_result_schema(), echo_dict_handler, "Echo a dict mapping.")
        .add_unary("echo_nested_list",
            params({arrow::field("matrix", arrow::list(arrow::list(arrow::int64())))}),
            nested_list_result_schema(), echo_nested_list_handler, "Echo a nested list.");

    // --- Optional/Nullable ---
    builder
        .add_unary("echo_optional_string",
            params({arrow::field("value", arrow::utf8(), true)}),
            optional_str_result_schema(), echo_optional_string_handler,
            "Echo an optional string (may be None).")
        .add_unary("echo_optional_int",
            params({arrow::field("value", arrow::int64(), true)}),
            optional_int_result_schema(), echo_optional_int_handler,
            "Echo an optional int (may be None).");

    // --- Dataclass Round-trip ---
    builder
        .add_unary("echo_point", params({arrow::field("point", arrow::binary())}),
            binary_result_schema(), echo_point_handler, "Echo a Point dataclass.")
        .add_unary("echo_all_types", params({arrow::field("data", arrow::binary())}),
            binary_result_schema(), echo_all_types_handler,
            "Echo an AllTypes dataclass exercising every type mapping.")
        .add_unary("echo_bounding_box", params({arrow::field("box", arrow::binary())}),
            binary_result_schema(), echo_bounding_box_handler,
            "Echo a BoundingBox with nested Points.");

    // --- Dataclass as Parameter ---
    builder
        .add_unary("inspect_point", params({arrow::field("point", arrow::binary())}),
            str_result_schema(), inspect_point_handler,
            "Accept a Point param (pa.binary() on wire), return formatted string.");

    // --- Annotated Types ---
    builder
        .add_unary("echo_int32", params({arrow::field("value", arrow::int32())}),
            int32_result_schema(), echo_int32_handler, "Echo an int32 value.")
        .add_unary("echo_float32", params({arrow::field("value", arrow::float32())}),
            float32_result_schema(), echo_float32_handler, "Echo a float32 value.");

    // --- Wide Arrow Types ---
    builder
        .add_unary("echo_int8", params({arrow::field("value", arrow::int8())}),
            result_schema_of(arrow::int8()), echo_int8_handler, "Echo an int8 value.")
        .add_unary("echo_int16", params({arrow::field("value", arrow::int16())}),
            result_schema_of(arrow::int16()), echo_int16_handler, "Echo an int16 value.")
        .add_unary("echo_uint8", params({arrow::field("value", arrow::uint8())}),
            result_schema_of(arrow::uint8()), echo_uint8_handler, "Echo a uint8 value.")
        .add_unary("echo_uint16", params({arrow::field("value", arrow::uint16())}),
            result_schema_of(arrow::uint16()), echo_uint16_handler, "Echo a uint16 value.")
        .add_unary("echo_uint32", params({arrow::field("value", arrow::uint32())}),
            result_schema_of(arrow::uint32()), echo_uint32_handler, "Echo a uint32 value.")
        .add_unary("echo_uint64", params({arrow::field("value", arrow::uint64())}),
            result_schema_of(arrow::uint64()), echo_uint64_handler, "Echo a uint64 value.")
        .add_unary("echo_date", params({arrow::field("value", arrow::date32())}),
            result_schema_of(arrow::date32()), echo_date_handler, "Echo a date32 value.")
        .add_unary("echo_timestamp",
            params({arrow::field("value", arrow::timestamp(arrow::TimeUnit::MICRO))}),
            result_schema_of(arrow::timestamp(arrow::TimeUnit::MICRO)),
            echo_timestamp_handler, "Echo a naive microsecond timestamp.")
        .add_unary("echo_timestamp_utc",
            params({arrow::field("value", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"))}),
            result_schema_of(arrow::timestamp(arrow::TimeUnit::MICRO, "UTC")),
            echo_timestamp_utc_handler, "Echo a UTC-tagged microsecond timestamp.")
        .add_unary("echo_time",
            params({arrow::field("value", arrow::time64(arrow::TimeUnit::MICRO))}),
            result_schema_of(arrow::time64(arrow::TimeUnit::MICRO)),
            echo_time_handler, "Echo a microsecond time-of-day value.")
        .add_unary("echo_duration",
            params({arrow::field("value", arrow::duration(arrow::TimeUnit::MICRO))}),
            result_schema_of(arrow::duration(arrow::TimeUnit::MICRO)),
            echo_duration_handler, "Echo a microsecond duration.")
        .add_unary("echo_decimal",
            params({arrow::field("value", arrow::decimal128(20, 4))}),
            result_schema_of(arrow::decimal128(20, 4)), echo_decimal_handler,
            "Echo a decimal128(20, 4) value.")
        .add_unary("echo_large_string",
            params({arrow::field("value", arrow::large_utf8())}),
            result_schema_of(arrow::large_utf8()), echo_large_string_handler,
            "Echo a large_string value.")
        .add_unary("echo_large_binary",
            params({arrow::field("value", arrow::large_binary())}),
            result_schema_of(arrow::large_binary()), echo_large_binary_handler,
            "Echo a large_binary value.")
        .add_unary("echo_fixed_binary",
            params({arrow::field("value", arrow::fixed_size_binary(8))}),
            result_schema_of(arrow::fixed_size_binary(8)), echo_fixed_binary_handler,
            "Echo a fixed_size_binary(8) value.")
        .add_unary("echo_wide_types", params({arrow::field("data", arrow::binary())}),
            binary_result_schema(), echo_wide_types_handler,
            "Round-trip every Arrow primitive width via a single dataclass.")
        .add_unary("echo_container_wide_types", params({arrow::field("data", arrow::binary())}),
            binary_result_schema(), echo_container_wide_types_handler,
            "Round-trip wide Arrow types nested inside list/dict/optional.")
        .add_unary("echo_embedded_arrow", params({arrow::field("data", arrow::binary())}),
            binary_result_schema(), echo_embedded_arrow_handler,
            "Round-trip a RecordBatch and Schema carried as nested IPC.")
        .add_unary("echo_deep_nested", params({arrow::field("data", arrow::binary())}),
            binary_result_schema(), echo_deep_nested_handler,
            "Round-trip multi-level nested containers and dictionary-encoded strings.")
        .add_unary("echo_dict_encoded_string",
            params({arrow::field("value", arrow::dictionary(arrow::int16(), arrow::utf8()))}),
            result_schema_of(arrow::dictionary(arrow::int16(), arrow::utf8())),
            echo_dict_encoded_string_handler,
            "Echo a string carried as a dictionary-encoded Arrow column.");

    // --- Multi-Param & Defaults ---
    builder
        .add_unary("add_floats",
            params({arrow::field("a", arrow::float64()), arrow::field("b", arrow::float64())}),
            float_result_schema(), add_floats_handler, "Add two floats.")
        .add_unary("concatenate",
            params({arrow::field("prefix", arrow::utf8()), arrow::field("suffix", arrow::utf8()),
                    arrow::field("separator", arrow::utf8())}),
            str_result_schema(), concatenate_handler,
            "Concatenate prefix + separator + suffix.")
        .add_unary("with_defaults",
            params({arrow::field("required", arrow::int64()),
                    arrow::field("optional_str", arrow::utf8()),
                    arrow::field("optional_int", arrow::int64())}),
            str_result_schema(), with_defaults_handler,
            "Return a formatted string showing all param values.");

    // --- Error Propagation ---
    builder
        .add_unary("raise_value_error", params({arrow::field("message", arrow::utf8())}),
            str_result_schema(), raise_value_error_handler,
            "Raise a ValueError with the given message.")
        .add_unary("raise_runtime_error", params({arrow::field("message", arrow::utf8())}),
            str_result_schema(), raise_runtime_error_handler,
            "Raise a RuntimeError with the given message.")
        .add_unary("raise_type_error", params({arrow::field("message", arrow::utf8())}),
            str_result_schema(), raise_type_error_handler,
            "Raise a TypeError with the given message.");

    // --- Client-Directed Logging ---
    builder
        .add_unary("echo_with_info_log", params({arrow::field("value", arrow::utf8())}),
            str_result_schema(), echo_with_info_log_handler,
            "Echo value, emitting one INFO log.")
        .add_unary("echo_with_multi_logs", params({arrow::field("value", arrow::utf8())}),
            str_result_schema(), echo_with_multi_logs_handler,
            "Echo value, emitting DEBUG + INFO + WARN logs.")
        .add_unary("echo_with_log_extras", params({arrow::field("value", arrow::utf8())}),
            str_result_schema(), echo_with_log_extras_handler,
            "Echo value, emitting an INFO log with extra key-value pairs.")
        .add_unary("echo_with_all_log_levels", params({arrow::field("value", arrow::utf8())}),
            str_result_schema(), echo_with_all_log_levels_handler,
            "Echo value, emitting one log at each of TRACE/DEBUG/INFO/WARN/ERROR.");

    // --- Cancellation probe (unary) ---
    builder
        .add_unary("cancel_probe_counters", empty_schema(),
            list_int_result_schema(), cancel_probe_counters_handler,
            "Return [produce_calls, exchange_calls, on_cancel_calls].")
        .add_void("reset_cancel_probe", empty_schema(), reset_cancel_probe_handler,
            "Reset all cancel-probe counters to zero on the server.");

    // --- Sticky session (unary; HTTP-only at runtime) ---
    builder
        .add_unary("open_counter", params({arrow::field("initial", arrow::int64())}),
            int_result_schema(), open_counter_handler,
            "Open a sticky session holding a counter; return its initial value.")
        .add_unary("increment_counter", params({arrow::field("by", arrow::int64())}),
            int_result_schema(), increment_counter_handler,
            "Increment the sticky session's counter; return the post-increment value.")
        .add_unary("close_counter", empty_schema(),
            int_result_schema(), close_counter_handler,
            "Close the sticky session; return the counter's final value before close.");

    // --- Producer Streams ---
    builder
        .add_producer("produce_n", params({arrow::field("count", arrow::int64())}),
            counter_schema(), make_produce_n, "Produce count batches with {index, value}.")
        .add_producer("produce_empty", empty_schema(), counter_schema(), make_produce_empty,
            "Produce zero batches (finish immediately).")
        .add_producer("produce_single", empty_schema(), counter_schema(), make_produce_single,
            "Produce exactly one batch.")
        .add_producer("produce_large_batches",
            params({arrow::field("rows_per_batch", arrow::int64()),
                    arrow::field("batch_count", arrow::int64())}),
            counter_schema(), make_produce_large,
            "Produce batch_count batches of rows_per_batch rows each.")
        .add_producer("produce_with_logs", params({arrow::field("count", arrow::int64())}),
            counter_schema(), make_produce_with_logs,
            "Produce batches with an INFO log before each.")
        .add_producer("produce_error_mid_stream",
            params({arrow::field("emit_before_error", arrow::int64())}),
            counter_schema(), make_produce_error_mid,
            "Raise after emitting emit_before_error batches.")
        .add_producer("produce_error_on_init", empty_schema(), counter_schema(),
            make_produce_error_init, "Raise during stream initialization.")
        .add_producer("produce_oversized_batch",
            params({arrow::field("rows_per_batch", arrow::int64())}),
            counter_schema(), make_produce_oversized_batch,
            "Emit one oversized batch of int64 rows, then finish.");

    // --- Producer Streams With Headers ---
    builder
        .add_producer("produce_with_header", params({arrow::field("count", arrow::int64())}),
            counter_schema(), make_produce_with_header,
            "Produce batches with a stream header.", conformance_header_schema())
        .add_producer("produce_with_header_and_logs",
            params({arrow::field("count", arrow::int64())}),
            counter_schema(), make_produce_with_header_and_logs,
            "Produce batches with a header and INFO logs.", conformance_header_schema())
        .add_producer("produce_with_rich_header",
            params({arrow::field("seed", arrow::int64()), arrow::field("count", arrow::int64())}),
            counter_schema(), make_produce_with_rich_header,
            "Produce batches with a rich multi-type stream header.", rich_header_schema())
        .add_producer("produce_dynamic_schema",
            params({arrow::field("seed", arrow::int64()), arrow::field("count", arrow::int64()),
                    arrow::field("include_strings", arrow::boolean()),
                    arrow::field("include_floats", arrow::boolean())}),
            counter_schema(), make_produce_dynamic_schema,
            "Produce batches with a dynamic output schema and rich header.",
            rich_header_schema());

    // --- Cancellation producer ---
    builder
        .add_producer("cancellable_producer", empty_schema(), counter_schema(),
            make_cancellable_producer,
            "Produce one batch per tick forever — designed to be cancelled.");

    // --- Sticky session streaming (HTTP-only at runtime) ---
    builder
        .add_producer("stream_session_counter",
            params({arrow::field("count", arrow::int64())}),
            session_value_schema(), make_stream_session_counter,
            "Emit count increments of the sticky session counter via a producer stream.");

    // --- Exchange Streams ---
    builder
        .add_exchange("exchange_scale", params({arrow::field("factor", arrow::float64())}),
            scale_input_schema(), scale_output_schema(), make_exchange_scale,
            "Multiply input values by factor.")
        .add_exchange("exchange_accumulate", empty_schema(),
            accum_input_schema(), accum_output_schema(), make_exchange_accumulate,
            "Accumulate running sum and exchange count across exchanges.")
        .add_exchange("exchange_with_logs", empty_schema(),
            scale_input_schema(), scale_output_schema(), make_exchange_with_logs,
            "Exchange with INFO + DEBUG logs per exchange.")
        .add_exchange("exchange_zero_columns", empty_schema(),
            empty_schema(), empty_schema(), make_exchange_zero_columns,
            "Exchange stream with zero-column input and output.")
        .add_exchange("exchange_error_on_nth", params({arrow::field("fail_on", arrow::int64())}),
            scale_input_schema(), scale_output_schema(), make_exchange_error_on_nth,
            "Raise on the Nth exchange (1-indexed).")
        .add_exchange("exchange_cast_compatible", empty_schema(),
            scale_input_schema(), scale_output_schema(), make_exchange_cast_compatible,
            "Exchange expecting float64 input — tests server-side cast.")
        .add_exchange("exchange_error_on_init", empty_schema(),
            scale_input_schema(), scale_output_schema(), make_exchange_error_on_init,
            "Raise during exchange stream initialization.")
        .add_exchange("exchange_oversized",
            params({arrow::field("rows_per_batch", arrow::int64())}),
            scale_input_schema(), counter_schema(), make_exchange_oversized,
            "Exchange that emits an oversized output batch for any input.");

    // --- Exchange Streams With Headers ---
    builder
        .add_exchange("exchange_with_header", params({arrow::field("factor", arrow::float64())}),
            scale_input_schema(), scale_output_schema(), make_exchange_with_header,
            "Exchange stream with a header.", conformance_header_schema())
        .add_exchange("exchange_with_rich_header",
            params({arrow::field("seed", arrow::int64()), arrow::field("factor", arrow::float64())}),
            scale_input_schema(), scale_output_schema(), make_exchange_with_rich_header,
            "Exchange stream with a rich multi-type header.", rich_header_schema());

    // --- Cancellation exchange ---
    builder
        .add_exchange("cancellable_exchange", empty_schema(),
            scale_input_schema(), scale_output_schema(), make_cancellable_exchange,
            "Echo each input batch — designed to be cancelled by the client.");

    // --- Sticky session streaming exchange (HTTP-only at runtime) ---
    builder
        .add_exchange("exchange_session_counter", empty_schema(),
            session_by_schema(), session_value_schema(), make_exchange_session_counter,
            "Exchange stream adding each input by column to the sticky session counter.");

    // A fixed marker rather than a real platform header: the contract under
    // test is capture-and-replay, and a stable name is what lets the shared
    // suite assert it. Real deployments substitute their own.
    if (http_cfg.sticky && sticky_echo) {
        http_cfg.sticky_echo_headers["x-vgi-conformance-echo"] = "conformance-fixed-marker";
    }

    builder.protocol_version("1.0.0");
    builder.enable_describe("ConformanceService");
    // We implement SHM, so we must answer the handshake: a worker that stays
    // silent is treated as "no SHM" and clients never negotiate it.
    builder.enable_transport_options();
    if (!server_id.empty()) builder.server_id(server_id);
    if (!access_log_path.empty()) {
        builder.access_log(access_log_path, access_log_max_record_bytes);
    }

    std::unique_ptr<vgi_rpc::Server> server;
    try {
        server = builder.build();
    } catch (const std::exception& e) {
        std::cerr << "vgi_rpc: failed to build server: " << e.what() << "\n";
        return 2;
    }

    try {
        if (http) {
            server->serve_http(http_cfg);
        } else if (unix_mode) {
            server->serve_unix(unix_path);
        } else if (tcp_mode) {
            server->serve_tcp(http_cfg.host, http_cfg.port);
        } else {
            server->run();
        }
    } catch (const std::exception& e) {
        // A misconfigured gate must abort rather than degrade to serving
        // something weaker than the operator believes they configured.
        std::cerr << "vgi_rpc: " << e.what() << "\n";
        return 2;
    }
    return 0;
}
