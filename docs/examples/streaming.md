# Streaming

Producer and exchange stream examples demonstrating stateful batch-oriented data processing.

```cpp title="examples/streaming.cpp"
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
```

## Producer Pattern

A producer generates output batches without receiving input data. The client sends initial parameters, then repeatedly ticks the server until the stream finishes.

1. Subclass `ProducerState` and implement `produce()`
2. Use `OutputCollector::emit_arrays()` or `emit_batch()` to send data
3. Call `out.finish()` when the stream is complete
4. Return a `Stream` from the factory with `input_schema` set to `empty_schema()`

```cpp
class MyProducer : public ProducerState {
    void produce(OutputCollector& out, CallContext& ctx) override {
        if (done_) {
            out.finish();
            return;
        }
        // Build arrays and emit
        out.emit_arrays({my_array});
    }
};
```

## Exchange Pattern

An exchange processes input batches and produces output batches. The client sends parameters to initialize the stream, then sends data batches that the server transforms.

1. Subclass `ExchangeState` and implement `exchange()`
2. The `input` parameter contains the client's data batch
3. Emit transformed data via the `OutputCollector`

```cpp
class MyExchange : public ExchangeState {
    void exchange(const AnnotatedBatch& input,
                  OutputCollector& out, CallContext& ctx) override {
        // Process input.batch, emit transformed output
        out.emit_arrays({transformed_array});
    }
};
```

## Stream Factory

Both patterns use a factory function that receives the initial request parameters and returns a `Stream` struct:

```cpp
Stream my_factory(const Request& req, CallContext& ctx) {
    auto param = req.get<int64_t>("param");

    Stream s;
    s.output_schema = my_output_schema();
    s.input_schema = my_input_schema();  // or empty_schema() for producer
    s.state = std::make_shared<MyState>(param);
    s.header = nullptr;  // optional header batch
    return s;
}
```
