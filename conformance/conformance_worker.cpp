// Conformance worker implementing all ~43 methods from ConformanceService.
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

#include <cstdint>
#include <format>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

using namespace vgi_rpc;

// =========================================================================
// Schemas
// =========================================================================

// Reused counter schema for producer streams
static auto counter_schema() {
    static auto s = arrow::schema({
        arrow::field("index", arrow::int64()),
        arrow::field("value", arrow::int64()),
    });
    return s;
}

// Exchange schemas
static auto scale_input_schema() {
    static auto s = arrow::schema({arrow::field("value", arrow::float64())});
    return s;
}

static auto scale_output_schema() {
    static auto s = arrow::schema({arrow::field("value", arrow::float64())});
    return s;
}

static auto accum_input_schema() {
    static auto s = arrow::schema({arrow::field("value", arrow::float64())});
    return s;
}

static auto accum_output_schema() {
    static auto s = arrow::schema({
        arrow::field("running_sum", arrow::float64()),
        arrow::field("exchange_count", arrow::int64()),
    });
    return s;
}

// Logging exchange schema (same as scale)
static auto log_exchange_input_schema() {
    static auto s = arrow::schema({arrow::field("value", arrow::float64())});
    return s;
}

static auto log_exchange_output_schema() {
    static auto s = arrow::schema({arrow::field("value", arrow::float64())});
    return s;
}

// Header schema for ConformanceHeader
static auto conformance_header_schema() {
    static auto s = arrow::schema({
        arrow::field("total_expected", arrow::int64()),
        arrow::field("description", arrow::utf8()),
    });
    return s;
}

// =========================================================================
// Helper: Build a 1-row result batch from a single column (echo pattern)
// =========================================================================

static Result echo_column(const Request& req, std::string_view param_name,
                          const std::shared_ptr<arrow::Schema>& result_schema) {
    auto col = req.batch()->GetColumnByName(std::string(param_name));
    return Result::value(result_schema, {col});
}

// Helper: Checked cast for Arrow array columns
template <typename T>
static std::shared_ptr<T> checked_cast_column(
    const std::shared_ptr<arrow::RecordBatch>& batch, const std::string& name) {
    auto col = batch->GetColumnByName(name);
    if (!col) throw std::runtime_error("Column not found: " + name);
    auto typed = std::dynamic_pointer_cast<T>(col);
    if (!typed) throw std::runtime_error("Type mismatch for column: " + name);
    return typed;
}

// Helper: Read IPC bytes from a binary column and return a RecordBatch
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

// Helper: Build ConformanceHeader batch
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

// =========================================================================
// Unary: Scalar Echo
// =========================================================================

static auto str_result_schema() {
    static auto s = arrow::schema({arrow::field("result", arrow::utf8())});
    return s;
}

static auto bytes_result_schema() {
    static auto s = arrow::schema({arrow::field("result", arrow::binary())});
    return s;
}

static auto int_result_schema() {
    static auto s = arrow::schema({arrow::field("result", arrow::int64())});
    return s;
}

static auto float_result_schema() {
    static auto s = arrow::schema({arrow::field("result", arrow::float64())});
    return s;
}

static auto bool_result_schema() {
    static auto s = arrow::schema({arrow::field("result", arrow::boolean())});
    return s;
}

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

// =========================================================================
// Unary: Void Returns
// =========================================================================

static void void_noop_handler(const Request&, CallContext&) {}
static void void_with_param_handler(const Request&, CallContext&) {}

// =========================================================================
// Unary: Complex Type Echo
// =========================================================================

static auto enum_result_schema() {
    static auto s = arrow::schema({
        arrow::field("result", arrow::dictionary(arrow::int16(), arrow::utf8()))});
    return s;
}

static auto list_str_result_schema() {
    static auto s = arrow::schema({
        arrow::field("result", arrow::list(arrow::utf8()))});
    return s;
}

static auto dict_str_int_result_schema() {
    static auto s = arrow::schema({
        arrow::field("result", arrow::map(arrow::utf8(), arrow::int64()))});
    return s;
}

static auto nested_list_result_schema() {
    static auto s = arrow::schema({
        arrow::field("result", arrow::list(arrow::list(arrow::int64())))});
    return s;
}

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
// Unary: Optional/Nullable
// =========================================================================

static auto optional_str_result_schema() {
    static auto s = arrow::schema({
        arrow::field("result", arrow::utf8(), /*nullable=*/true)});
    return s;
}

static auto optional_int_result_schema() {
    static auto s = arrow::schema({
        arrow::field("result", arrow::int64(), /*nullable=*/true)});
    return s;
}

static Result echo_optional_string_handler(const Request& req, CallContext&) {
    return echo_column(req, "value", optional_str_result_schema());
}

static Result echo_optional_int_handler(const Request& req, CallContext&) {
    return echo_column(req, "value", optional_int_result_schema());
}

// =========================================================================
// Unary: Dataclass Round-trip
// =========================================================================

static auto binary_result_schema() {
    static auto s = arrow::schema({arrow::field("result", arrow::binary())});
    return s;
}

static Result echo_point_handler(const Request& req, CallContext&) {
    return echo_column(req, "point", binary_result_schema());
}

static Result echo_all_types_handler(const Request& req, CallContext&) {
    return echo_column(req, "data", binary_result_schema());
}

static Result echo_bounding_box_handler(const Request& req, CallContext&) {
    return echo_column(req, "box", binary_result_schema());
}

// =========================================================================
// Unary: Dataclass as Parameter
// =========================================================================

static Result inspect_point_handler(const Request& req, CallContext&) {
    auto batch = deserialize_dataclass(req, "point");
    auto x_col = checked_cast_column<arrow::DoubleArray>(batch, "x");
    auto y_col = checked_cast_column<arrow::DoubleArray>(batch, "y");
    double x = x_col->Value(0);
    double y = y_col->Value(0);

    // Format like Python: "Point(1.0, 2.0)"
    // Python's repr uses decimal point, so we need to ensure .0 for integers
    auto fmt_double = [](double v) -> std::string {
        auto s = std::to_string(v);
        // to_string always includes decimal point; trim trailing zeros but keep at least one
        auto dot = s.find('.');
        if (dot != std::string::npos) {
            auto last_nonzero = s.find_last_not_of('0');
            if (last_nonzero == dot) last_nonzero++;  // keep at least "x.0"
            s.erase(last_nonzero + 1);
        }
        return s;
    };
    auto result_str = std::format("Point({}, {})", fmt_double(x), fmt_double(y));

    arrow::StringBuilder builder;
    VGI_RPC_THROW_NOT_OK(builder.Append(result_str));
    return Result::value(str_result_schema(), {unwrap(builder.Finish())});
}

// =========================================================================
// Unary: Annotated Types
// =========================================================================

static auto int32_result_schema() {
    static auto s = arrow::schema({arrow::field("result", arrow::int32())});
    return s;
}

static auto float32_result_schema() {
    static auto s = arrow::schema({arrow::field("result", arrow::float32())});
    return s;
}

static Result echo_int32_handler(const Request& req, CallContext&) {
    return echo_column(req, "value", int32_result_schema());
}

static Result echo_float32_handler(const Request& req, CallContext&) {
    return echo_column(req, "value", float32_result_schema());
}

// =========================================================================
// Unary: Multi-Param & Defaults
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
// Unary: Error Propagation
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
// Unary: Client-Directed Logging
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

// =========================================================================
// Producer Stream States
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
    void produce(OutputCollector& out, CallContext&) override {
        out.finish();
    }
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

class LoggingProducerState : public ProducerState {
public:
    LoggingProducerState(int64_t count) : count_(count) {}

    void produce(OutputCollector& out, CallContext&) override {
        if (current_ >= count_) { out.finish(); return; }
        out.client_log(LogLevel::INFO,
                       std::format("producing batch {}", current_));
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
            throw std::runtime_error(
                std::format("intentional error after {} batches", n_));
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

// =========================================================================
// Exchange Stream States
// =========================================================================

class ScaleExchangeState : public ExchangeState {
public:
    ScaleExchangeState(double factor) : factor_(factor) {}

    void exchange(const AnnotatedBatch& input,
                  OutputCollector& out, CallContext&) override {
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
    void exchange(const AnnotatedBatch& input,
                  OutputCollector& out, CallContext&) override {
        auto col = checked_cast_column<arrow::DoubleArray>(input.batch, "value");
        for (int64_t i = 0; i < col->length(); ++i) {
            running_sum_ += col->Value(i);
        }
        ++exchange_count_;

        arrow::DoubleBuilder sum_builder;
        arrow::Int64Builder count_builder;
        VGI_RPC_THROW_NOT_OK(sum_builder.Append(running_sum_));
        VGI_RPC_THROW_NOT_OK(count_builder.Append(exchange_count_));
        out.emit_arrays({unwrap(sum_builder.Finish()),
                         unwrap(count_builder.Finish())});
    }
private:
    double running_sum_ = 0.0;
    int64_t exchange_count_ = 0;
};

class LoggingExchangeState : public ExchangeState {
public:
    void exchange(const AnnotatedBatch& input,
                  OutputCollector& out, CallContext&) override {
        out.client_log(LogLevel::INFO, "exchange processing");
        out.client_log(LogLevel::DEBUG, "exchange debug");
        out.emit_batch(input.batch);
    }
};

class FailOnExchangeNState : public ExchangeState {
public:
    FailOnExchangeNState(int64_t fail_on) : fail_on_(fail_on) {}

    void exchange(const AnnotatedBatch& input,
                  OutputCollector& out, CallContext&) override {
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

// =========================================================================
// Producer Stream Factories
// =========================================================================

static Stream make_produce_n(const Request& req, CallContext&) {
    auto count = req.get<int64_t>("count");
    return {counter_schema(), empty_schema(),
            std::make_shared<CounterState>(count), nullptr};
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
    auto rpb = req.get<int64_t>("rows_per_batch");
    auto bc = req.get<int64_t>("batch_count");
    return {counter_schema(), empty_schema(),
            std::make_shared<LargeProducerState>(rpb, bc), nullptr};
}

static Stream make_produce_with_logs(const Request& req, CallContext&) {
    auto count = req.get<int64_t>("count");
    return {counter_schema(), empty_schema(),
            std::make_shared<LoggingProducerState>(count), nullptr};
}

static Stream make_produce_error_mid(const Request& req, CallContext&) {
    auto n = req.get<int64_t>("emit_before_error");
    return {counter_schema(), empty_schema(),
            std::make_shared<ErrorAfterNState>(n), nullptr};
}

static Stream make_produce_error_init(const Request&, CallContext&) {
    throw std::runtime_error("intentional init error");
}

// Producer streams with headers
static Stream make_produce_with_header(const Request& req, CallContext&) {
    auto count = req.get<int64_t>("count");
    auto header = make_header_batch(
        count, std::format("producing {} batches", count));
    return {counter_schema(), empty_schema(),
            std::make_shared<HeaderProducerState>(count), header};
}

static Stream make_produce_with_header_and_logs(const Request& req, CallContext& ctx) {
    auto count = req.get<int64_t>("count");
    ctx.client_log(LogLevel::INFO, "stream init log");
    auto header = make_header_batch(
        count, std::format("producing {} with logs", count));
    return {counter_schema(), empty_schema(),
            std::make_shared<HeaderProducerState>(count), header};
}

// =========================================================================
// Exchange Stream Factories
// =========================================================================

static Stream make_exchange_scale(const Request& req, CallContext&) {
    auto factor = req.get<double>("factor");
    return {scale_output_schema(), scale_input_schema(),
            std::make_shared<ScaleExchangeState>(factor), nullptr};
}

static Stream make_exchange_accumulate(const Request&, CallContext&) {
    return {accum_output_schema(), accum_input_schema(),
            std::make_shared<AccumulatingExchangeState>(), nullptr};
}

static Stream make_exchange_with_logs(const Request&, CallContext&) {
    return {log_exchange_output_schema(), log_exchange_input_schema(),
            std::make_shared<LoggingExchangeState>(), nullptr};
}

static Stream make_exchange_error_on_nth(const Request& req, CallContext&) {
    auto n = req.get<int64_t>("fail_on");
    return {scale_output_schema(), scale_input_schema(),
            std::make_shared<FailOnExchangeNState>(n), nullptr};
}

static Stream make_exchange_error_on_init(const Request&, CallContext&) {
    throw std::runtime_error("intentional exchange init error");
}

static Stream make_exchange_with_header(const Request& req, CallContext&) {
    auto factor = req.get<double>("factor");
    // Python uses f"scale by {factor}" which formats 2.0 as "scale by 2.0"
    // std::format("{}", 2.0) produces "2", so use fixed-style
    auto factor_str = std::to_string(factor);
    // Trim trailing zeros but keep at least one decimal
    auto dot = factor_str.find('.');
    if (dot != std::string::npos) {
        auto last = factor_str.find_last_not_of('0');
        if (last == dot) last++;
        factor_str.erase(last + 1);
    }
    auto header = make_header_batch(
        0, "scale by " + factor_str);
    return {scale_output_schema(), scale_input_schema(),
            std::make_shared<ScaleExchangeState>(factor), header};
}

// =========================================================================
// Main: Register all methods
// =========================================================================

int main() {
    auto builder = ServerBuilder();

    // --- Scalar Echo ---
    builder
        .add_unary("echo_string",
            arrow::schema({arrow::field("value", arrow::utf8())}),
            str_result_schema(), echo_string_handler, "Echo a string value.")
        .add_unary("echo_bytes",
            arrow::schema({arrow::field("data", arrow::binary())}),
            bytes_result_schema(), echo_bytes_handler, "Echo a bytes value.")
        .add_unary("echo_int",
            arrow::schema({arrow::field("value", arrow::int64())}),
            int_result_schema(), echo_int_handler, "Echo an integer value.")
        .add_unary("echo_float",
            arrow::schema({arrow::field("value", arrow::float64())}),
            float_result_schema(), echo_float_handler, "Echo a float value.")
        .add_unary("echo_bool",
            arrow::schema({arrow::field("value", arrow::boolean())}),
            bool_result_schema(), echo_bool_handler, "Echo a boolean value.");

    // --- Void ---
    builder
        .add_void("void_noop",
            empty_schema(), void_noop_handler, "No-op returning void.")
        .add_void("void_with_param",
            arrow::schema({arrow::field("value", arrow::int64())}),
            void_with_param_handler, "Accept a parameter, return void.");

    // --- Complex Type Echo ---
    builder
        .add_unary("echo_enum",
            arrow::schema({arrow::field("status",
                arrow::dictionary(arrow::int16(), arrow::utf8()))}),
            enum_result_schema(), echo_enum_handler, "Echo an enum value.")
        .add_unary("echo_list",
            arrow::schema({arrow::field("values", arrow::list(arrow::utf8()))}),
            list_str_result_schema(), echo_list_handler, "Echo a list of strings.")
        .add_unary("echo_dict",
            arrow::schema({arrow::field("mapping",
                arrow::map(arrow::utf8(), arrow::int64()))}),
            dict_str_int_result_schema(), echo_dict_handler, "Echo a dict mapping.")
        .add_unary("echo_nested_list",
            arrow::schema({arrow::field("matrix",
                arrow::list(arrow::list(arrow::int64())))}),
            nested_list_result_schema(), echo_nested_list_handler,
            "Echo a nested list.");

    // --- Optional/Nullable ---
    builder
        .add_unary("echo_optional_string",
            arrow::schema({arrow::field("value", arrow::utf8(), true)}),
            optional_str_result_schema(), echo_optional_string_handler,
            "Echo an optional string (may be None).")
        .add_unary("echo_optional_int",
            arrow::schema({arrow::field("value", arrow::int64(), true)}),
            optional_int_result_schema(), echo_optional_int_handler,
            "Echo an optional int (may be None).");

    // --- Dataclass Round-trip ---
    builder
        .add_unary("echo_point",
            arrow::schema({arrow::field("point", arrow::binary())}),
            binary_result_schema(), echo_point_handler, "Echo a Point dataclass.")
        .add_unary("echo_all_types",
            arrow::schema({arrow::field("data", arrow::binary())}),
            binary_result_schema(), echo_all_types_handler,
            "Echo an AllTypes dataclass exercising every type mapping.")
        .add_unary("echo_bounding_box",
            arrow::schema({arrow::field("box", arrow::binary())}),
            binary_result_schema(), echo_bounding_box_handler,
            "Echo a BoundingBox with nested Points.");

    // --- Dataclass as Parameter ---
    builder
        .add_unary("inspect_point",
            arrow::schema({arrow::field("point", arrow::binary())}),
            str_result_schema(), inspect_point_handler,
            "Accept a Point param (pa.binary() on wire), return formatted string.");

    // --- Annotated Types ---
    builder
        .add_unary("echo_int32",
            arrow::schema({arrow::field("value", arrow::int32())}),
            int32_result_schema(), echo_int32_handler, "Echo an int32 value.")
        .add_unary("echo_float32",
            arrow::schema({arrow::field("value", arrow::float32())}),
            float32_result_schema(), echo_float32_handler, "Echo a float32 value.");

    // --- Multi-Param & Defaults ---
    builder
        .add_unary("add_floats",
            arrow::schema({arrow::field("a", arrow::float64()),
                           arrow::field("b", arrow::float64())}),
            float_result_schema(), add_floats_handler, "Add two floats.")
        .add_unary("concatenate",
            arrow::schema({arrow::field("prefix", arrow::utf8()),
                           arrow::field("suffix", arrow::utf8()),
                           arrow::field("separator", arrow::utf8())}),
            str_result_schema(), concatenate_handler,
            "Concatenate prefix + separator + suffix.")
        .add_unary("with_defaults",
            arrow::schema({arrow::field("required", arrow::int64()),
                           arrow::field("optional_str", arrow::utf8()),
                           arrow::field("optional_int", arrow::int64())}),
            str_result_schema(), with_defaults_handler,
            "Return a formatted string showing all param values.");

    // --- Error Propagation ---
    builder
        .add_unary("raise_value_error",
            arrow::schema({arrow::field("message", arrow::utf8())}),
            str_result_schema(), raise_value_error_handler,
            "Raise a ValueError with the given message.")
        .add_unary("raise_runtime_error",
            arrow::schema({arrow::field("message", arrow::utf8())}),
            str_result_schema(), raise_runtime_error_handler,
            "Raise a RuntimeError with the given message.")
        .add_unary("raise_type_error",
            arrow::schema({arrow::field("message", arrow::utf8())}),
            str_result_schema(), raise_type_error_handler,
            "Raise a TypeError with the given message.");

    // --- Client-Directed Logging ---
    builder
        .add_unary("echo_with_info_log",
            arrow::schema({arrow::field("value", arrow::utf8())}),
            str_result_schema(), echo_with_info_log_handler,
            "Echo value, emitting one INFO log.")
        .add_unary("echo_with_multi_logs",
            arrow::schema({arrow::field("value", arrow::utf8())}),
            str_result_schema(), echo_with_multi_logs_handler,
            "Echo value, emitting DEBUG + INFO + WARN logs.")
        .add_unary("echo_with_log_extras",
            arrow::schema({arrow::field("value", arrow::utf8())}),
            str_result_schema(), echo_with_log_extras_handler,
            "Echo value, emitting an INFO log with extra key-value pairs.");

    // --- Producer Streams ---
    builder
        .add_producer("produce_n",
            arrow::schema({arrow::field("count", arrow::int64())}),
            counter_schema(), make_produce_n,
            "Produce count batches with {index, value}.")
        .add_producer("produce_empty",
            empty_schema(), counter_schema(), make_produce_empty,
            "Produce zero batches (finish immediately).")
        .add_producer("produce_single",
            empty_schema(), counter_schema(), make_produce_single,
            "Produce exactly one batch.")
        .add_producer("produce_large_batches",
            arrow::schema({arrow::field("rows_per_batch", arrow::int64()),
                           arrow::field("batch_count", arrow::int64())}),
            counter_schema(), make_produce_large,
            "Produce batch_count batches of rows_per_batch rows each.")
        .add_producer("produce_with_logs",
            arrow::schema({arrow::field("count", arrow::int64())}),
            counter_schema(), make_produce_with_logs,
            "Produce batches with an INFO log before each.")
        .add_producer("produce_error_mid_stream",
            arrow::schema({arrow::field("emit_before_error", arrow::int64())}),
            counter_schema(), make_produce_error_mid,
            "Raise after emitting emit_before_error batches.")
        .add_producer("produce_error_on_init",
            empty_schema(), counter_schema(), make_produce_error_init,
            "Raise during stream initialization.");

    // --- Producer Streams With Headers ---
    builder
        .add_producer("produce_with_header",
            arrow::schema({arrow::field("count", arrow::int64())}),
            counter_schema(), make_produce_with_header,
            "Produce batches with a stream header.",
            conformance_header_schema())
        .add_producer("produce_with_header_and_logs",
            arrow::schema({arrow::field("count", arrow::int64())}),
            counter_schema(), make_produce_with_header_and_logs,
            "Produce batches with a header and INFO logs.",
            conformance_header_schema());

    // --- Exchange Streams ---
    builder
        .add_exchange("exchange_scale",
            arrow::schema({arrow::field("factor", arrow::float64())}),
            scale_input_schema(), scale_output_schema(),
            make_exchange_scale, "Multiply input values by factor.")
        .add_exchange("exchange_accumulate",
            empty_schema(), accum_input_schema(), accum_output_schema(),
            make_exchange_accumulate,
            "Accumulate running sum and exchange count across exchanges.")
        .add_exchange("exchange_with_logs",
            empty_schema(), log_exchange_input_schema(), log_exchange_output_schema(),
            make_exchange_with_logs,
            "Exchange with INFO + DEBUG logs per exchange.")
        .add_exchange("exchange_error_on_nth",
            arrow::schema({arrow::field("fail_on", arrow::int64())}),
            scale_input_schema(), scale_output_schema(),
            make_exchange_error_on_nth,
            "Raise on the Nth exchange (1-indexed).")
        .add_exchange("exchange_error_on_init",
            empty_schema(), scale_input_schema(), scale_output_schema(),
            make_exchange_error_on_init,
            "Raise during exchange stream initialization.");

    // --- Exchange Streams With Headers ---
    builder
        .add_exchange("exchange_with_header",
            arrow::schema({arrow::field("factor", arrow::float64())}),
            scale_input_schema(), scale_output_schema(),
            make_exchange_with_header,
            "Exchange stream with a header.",
            conformance_header_schema());

    builder.enable_describe("ConformanceService");

    auto server = builder.build();
    server->run();
    return 0;
}
