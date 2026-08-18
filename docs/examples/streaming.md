# Streaming

Producer and exchange stream examples demonstrating stateful batch-oriented data processing.

```cpp title="examples/streaming.cpp"
--8<-- "examples/streaming.cpp"
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
