# Streaming

`#include <vgi_rpc/stream.h>`

Streaming support for producer and exchange method patterns.

## StreamState

```cpp
class StreamState {
public:
    virtual ~StreamState() noexcept = default;
    virtual void process(const AnnotatedBatch& input,
                         OutputCollector& out, CallContext& ctx) = 0;
};
```

Base class for stream processors. You won't typically subclass this directly — use `ProducerState` or `ExchangeState` instead.

## ProducerState

```cpp
class ProducerState : public StreamState {
public:
    virtual void produce(OutputCollector& out, CallContext& ctx) = 0;
};
```

Subclass for producer streams that generate output without consuming input data. The framework calls `produce()` repeatedly until `out.finish()` is called.

```cpp
class MyProducer : public vgi_rpc::ProducerState {
public:
    void produce(vgi_rpc::OutputCollector& out, vgi_rpc::CallContext& ctx) override {
        if (done_) {
            out.finish();
            return;
        }
        // Build and emit arrays
        out.emit_arrays({array1, array2});
    }
};
```

## ExchangeState

```cpp
class ExchangeState : public StreamState {
public:
    virtual void exchange(const AnnotatedBatch& input,
                          OutputCollector& out, CallContext& ctx) = 0;
};
```

Subclass for exchange streams that transform input batches into output batches.

```cpp
class MyExchange : public vgi_rpc::ExchangeState {
public:
    void exchange(const vgi_rpc::AnnotatedBatch& input,
                  vgi_rpc::OutputCollector& out, vgi_rpc::CallContext& ctx) override {
        // Process input.batch columns
        // Emit transformed output
        out.emit_arrays({result_array});
    }
};
```

## Stream

```cpp
struct Stream {
    std::shared_ptr<arrow::Schema> output_schema;
    std::shared_ptr<arrow::Schema> input_schema;  // empty_schema() for producer
    std::shared_ptr<StreamState> state;
    std::shared_ptr<arrow::RecordBatch> header;    // nullptr if no header
};
```

Returned by stream factory functions. Contains the schemas, the stream state object, and an optional header batch sent before the first data batch.

## OutputCollector

`#include <vgi_rpc/output_collector.h>`

Accumulates output batches and log messages during a single stream tick.

### `emit_batch`

```cpp
void emit_batch(std::shared_ptr<arrow::RecordBatch> batch);
```

Emit a pre-built data batch.

### `emit_arrays`

```cpp
void emit_arrays(const std::vector<std::shared_ptr<arrow::Array>>& arrays);
```

Emit a data batch from a vector of arrays (matched against the output schema).

### `client_log`

```cpp
void client_log(LogLevel level, std::string_view message);
```

Emit a log message to the client.

### `finish`

```cpp
void finish();
```

Signal stream completion. Only valid in producer mode.

### `is_finished`

```cpp
bool is_finished() const noexcept;
```

## AnnotatedBatch

`#include <vgi_rpc/annotated_batch.h>`

A record batch paired with optional per-batch custom metadata.

```cpp
struct AnnotatedBatch {
    std::shared_ptr<arrow::RecordBatch> batch;
    std::shared_ptr<arrow::KeyValueMetadata> custom_metadata;

    static AnnotatedBatch data(std::shared_ptr<arrow::RecordBatch> b);
    static AnnotatedBatch with_metadata(
        std::shared_ptr<arrow::RecordBatch> b,
        std::shared_ptr<arrow::KeyValueMetadata> md);

    BatchType type() const;
};
```
