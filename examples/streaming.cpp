// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/server.h"
#include "vgi_rpc/stream.h"
#include "vgi_rpc/metadata.h"
#include "vgi_rpc/arrow_utils.h"

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/type.h>

using namespace vgi_rpc;

// --- Counter producer: emits {index, value} batches ---

static auto counter_schema() {
    return arrow::schema({
        arrow::field("index", arrow::int64()),
        arrow::field("value", arrow::int64()),
    });
}

class CounterState : public ProducerState {
public:
    CounterState(int64_t count) : count_(count) {}

    void produce(OutputCollector& out, CallContext& /*ctx*/) override {
        if (current_ >= count_) {
            out.finish();
            return;
        }

        arrow::Int64Builder idx_builder, val_builder;
        VGI_RPC_THROW_NOT_OK(idx_builder.Append(current_));
        VGI_RPC_THROW_NOT_OK(val_builder.Append(current_ * 10));

        auto idx_arr = unwrap(idx_builder.Finish());
        auto val_arr = unwrap(val_builder.Finish());

        out.emit_arrays({idx_arr, val_arr});
        ++current_;
    }

private:
    int64_t count_;
    int64_t current_ = 0;
};

static Stream make_counter(const Request& req, CallContext& /*ctx*/) {
    auto count = req.get<int64_t>("count");

    Stream s;
    s.output_schema = counter_schema();
    s.input_schema = empty_schema();
    s.state = std::make_shared<CounterState>(count);
    return s;
}

// --- Scale exchange: multiplies input values by factor ---

static auto scale_input_schema() {
    return arrow::schema({arrow::field("value", arrow::float64())});
}

static auto scale_output_schema() {
    return arrow::schema({arrow::field("value", arrow::float64())});
}

class ScaleState : public ExchangeState {
public:
    ScaleState(double factor) : factor_(factor) {}

    void exchange(const AnnotatedBatch& input,
                  OutputCollector& out, CallContext& /*ctx*/) override {
        auto col = std::static_pointer_cast<arrow::DoubleArray>(
            input.batch->column(0));

        arrow::DoubleBuilder builder;
        for (int64_t i = 0; i < col->length(); ++i) {
            VGI_RPC_THROW_NOT_OK(builder.Append(col->Value(i) * factor_));
        }
        auto result_arr = unwrap(builder.Finish());
        out.emit_arrays({result_arr});
    }

private:
    double factor_;
};

static Stream make_scale(const Request& req, CallContext& /*ctx*/) {
    auto factor = req.get<double>("factor");

    Stream s;
    s.output_schema = scale_output_schema();
    s.input_schema = scale_input_schema();
    s.state = std::make_shared<ScaleState>(factor);
    return s;
}

int main() {
    auto server = ServerBuilder()
        .add_producer(
            "produce_n",
            arrow::schema({arrow::field("count", arrow::int64())}),
            counter_schema(),
            make_counter,
            "Produce N batches with index and value=index*10")
        .add_exchange(
            "exchange_scale",
            arrow::schema({arrow::field("factor", arrow::float64())}),
            scale_input_schema(),
            scale_output_schema(),
            make_scale,
            "Scale input values by a factor")
        .enable_describe("StreamingExample")
        .build();

    server->run();
    return 0;
}
