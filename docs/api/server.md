# Server

`#include <vgi_rpc/server.h>`

## ServerBuilder

Fluent builder for constructing a `Server` with registered methods.

### Methods

#### `add_unary`

```cpp
ServerBuilder& add_unary(
    const std::string& name,
    std::shared_ptr<arrow::Schema> params_schema,
    std::shared_ptr<arrow::Schema> result_schema,
    std::function<Result(const Request&, CallContext&)> handler,
    const std::string& doc = "");
```

Register a unary method. The handler receives a `Request` and returns a `Result`.

#### `add_void`

```cpp
ServerBuilder& add_void(
    const std::string& name,
    std::shared_ptr<arrow::Schema> params_schema,
    std::function<void(const Request&, CallContext&)> handler,
    const std::string& doc = "");
```

Register a void unary method. The handler performs a side effect and returns nothing.

#### `add_producer`

```cpp
ServerBuilder& add_producer(
    const std::string& name,
    std::shared_ptr<arrow::Schema> params_schema,
    std::shared_ptr<arrow::Schema> output_schema,
    std::function<Stream(const Request&, CallContext&)> factory,
    const std::string& doc = "",
    std::shared_ptr<arrow::Schema> header_schema = nullptr);
```

Register a producer stream method. The factory receives initial parameters and returns a `Stream` whose `ProducerState` generates output batches.

#### `add_exchange`

```cpp
ServerBuilder& add_exchange(
    const std::string& name,
    std::shared_ptr<arrow::Schema> params_schema,
    std::shared_ptr<arrow::Schema> input_schema,
    std::shared_ptr<arrow::Schema> output_schema,
    std::function<Stream(const Request&, CallContext&)> factory,
    const std::string& doc = "",
    std::shared_ptr<arrow::Schema> header_schema = nullptr);
```

Register an exchange stream method. The factory returns a `Stream` whose `ExchangeState` processes input batches and emits output batches.

#### `server_id`

```cpp
ServerBuilder& server_id(std::string id);
```

Set a deterministic server ID. Defaults to `random_hex(12)` if not set.

#### `enable_describe`

```cpp
ServerBuilder& enable_describe(const std::string& protocol_name = "");
```

Enable the `__describe__` introspection method. The describe response is a snapshot captured at `build()` time.

#### `build`

```cpp
std::unique_ptr<Server> build();
```

Build and return the server. Can only be called once.

## Server

Pipe dispatch is single-threaded. HTTP and raw socket listeners can dispatch
unrelated calls concurrently; handlers that share mutable application state
must synchronize it.

### Methods

#### `run`

```cpp
void run();
```

Enter the main request loop: read requests from stdin, dispatch to handlers, write responses to stdout. Returns on EOF.

#### `serve_one`

```cpp
bool serve_one(
    const std::shared_ptr<arrow::io::InputStream>& input,
    const std::shared_ptr<arrow::io::OutputStream>& output);
```

Process a single request. Returns `true` if a request was served, `false` on EOF (clean shutdown). Useful for testing with custom I/O streams.

#### `serve_tcp`

```cpp
void serve_tcp(const std::string& host, int port);
void serve_tcp(const std::string& host, int port,
               const TcpServerOptions& options);
```

Serve persistent raw Arrow-IPC connections. `TcpServerOptions` configures
finite active/pending admission, complete-setup and idle-read deadlines,
bounded response writes, trusted PROXY v2 parsing, and an optional
connection-snapshot identity resolver. Excess connections are rejected
without blocking the accept loop. See [Trusted PROXY protocol v2
listeners](../proxy-protocol-v2.md) for the trust and availability model.

#### `server_id`

```cpp
const std::string& server_id() const noexcept;
```

#### `methods`

```cpp
const std::unordered_map<std::string, MethodInfo>& methods() const noexcept;
```

## MethodType

```cpp
enum class MethodType {
    UNARY,
    STREAM,
};
```

## MethodInfo

```cpp
struct MethodInfo {
    std::string name;
    MethodType method_type;
    std::shared_ptr<arrow::Schema> params_schema;
    std::shared_ptr<arrow::Schema> result_schema;
    std::function<Result(const Request&, CallContext&)> handler;
    std::string doc;
    bool has_return = true;

    // Streaming fields (nullptr for unary)
    std::shared_ptr<arrow::Schema> input_schema;
    std::shared_ptr<arrow::Schema> output_schema;
    std::shared_ptr<arrow::Schema> header_schema;
    std::function<Stream(const Request&, CallContext&)> stream_factory;
};
```
