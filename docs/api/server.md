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

#### `add_protocol`

```cpp
ServerBuilder& add_protocol(ProtocolBuilder protocol);
```

Host another application protocol beside the primary. A `ProtocolBuilder`
is one `(name, version, implementation)` triple: its constructor takes the
routing key and an optional canonical-semver version, and it has the same
`add_unary` / `add_void` / `add_producer` / `add_exchange` methods as
`ServerBuilder` (which registers its primary through one).

```cpp
vgi_rpc::ProtocolBuilder reports("acme.Reports.v1");
reports.add_unary("render", params, result, render_handler);

auto server = vgi_rpc::ServerBuilder()
                  .protocol("acme.Service.v2")
                  .protocol_version("2.0.0")
                  .add_unary("echo", params, result, echo_handler)
                  .add_protocol(std::move(reports))
                  .build();
```

The registered set is fixed when `build()` runs and is hosted on every
transport — pipe, unix, TCP and HTTP — so reflection output and protocol
hashes are stable for the server's life. `list_protocols` lists application
protocols in registration order, primary first. Dispatch resolves the pair
`(protocol, method)`, and each protocol is version-gated against its *own*
declared version (a protocol that declares none enforces nothing).

There is no way to host a subset of a protocol's methods: the protocol is the
unit of optionality (WIRE_PROTOCOL §3.1). `build()` throws
`std::invalid_argument` for a name under the reserved `vgi_rpc.` prefix (the
primary's too), a name registered twice, a malformed name or version, or
additional protocols on a server whose primary declared no name.

#### `include_tracebacks`

```cpp
ServerBuilder& include_tracebacks(bool include);
```

Whether EXCEPTION batches carry `log_extra.traceback`. On by default on every
transport (WIRE_PROTOCOL §8): the DuckDB extension shows the remote traceback
to users. `include_tracebacks(false)` turns it off for the whole server, on
every transport. C++ exceptions capture no stack, so the traceback is
synthesized: the `protocol/method` that raised and the `Type: message` line,
on every EXCEPTION batch, framework refusals included. The code, kind and
details are sent either way.

Registration is sealed once `build()` runs: calling any registration method
(`add_*`, `add_protocol`, `protocol`, `protocol_version`, `identity`,
`include_tracebacks`) afterwards throws `std::logic_error`.

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
connection-snapshot identity resolver. `iroh_proxy_issuer` enables the fixed
bridge-forwarded Iroh EndpointId form only on required PROXY v2, while
`peer_authentication_policy` evaluates the combined evidence snapshot. Excess connections are rejected
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
