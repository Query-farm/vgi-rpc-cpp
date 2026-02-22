# vgi-rpc C++

A C++20 RPC framework built on [Apache Arrow](https://arrow.apache.org/) IPC for high-performance columnar data exchange over pipe-based transport.

## Overview

vgi-rpc provides a server framework for implementing RPC methods that communicate using Arrow's columnar IPC format over stdin/stdout. It supports three method patterns:

- **Unary** — single request, single response
- **Producer** — parameters in, multiple response batches out
- **Exchange** — parameters + input batches in, output batches out

The framework is single-threaded by design, processing one request at a time. Method handlers receive typed parameters via the `Request` class and return results as Arrow record batches.

## Quick Example

```cpp title="examples/quick_example.cpp"
--8<-- "examples/quick_example.cpp"
```

## Features

- **Typed parameter extraction** — `get<T>(name)` and `get_optional<T>(name)` for doubles, ints, strings, booleans, and list types
- **Builder pattern** — fluent `ServerBuilder` API for registering methods
- **Streaming support** — producer and exchange patterns for batch-oriented data processing
- **Introspection** — optional `__describe__` method exposes method schemas and documentation
- **Client logging** — in-band log messages from server to client during request handling
- **Error handling** — exceptions automatically converted to protocol error responses

## License

Apache 2.0 — see [LICENSE](https://github.com/Query-farm/vgi-rpc-cpp/blob/main/LICENSE).
