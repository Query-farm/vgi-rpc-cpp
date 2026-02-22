# vgi-rpc C++

A C++20 RPC framework built on [Apache Arrow](https://arrow.apache.org/) IPC for high-performance columnar data exchange over pipe-based transport.

## Overview

vgi-rpc provides a server framework for implementing RPC methods that communicate using Arrow's columnar IPC format over stdin/stdout. It supports three method patterns:

- **Unary** — single request, single response
- **Producer** — parameters in, multiple response batches out
- **Exchange** — parameters + input batches in, output batches out

The framework is single-threaded by design, processing one request at a time. Method handlers receive typed parameters via the `Request` class and return results as Arrow record batches.

## Quick Example

```cpp
#include <vgi_rpc/server.h>
#include <vgi_rpc/request.h>
#include <vgi_rpc/result.h>
#include <vgi_rpc/arrow_utils.h>

#include <arrow/array/builder_primitive.h>
#include <arrow/type.h>

int main() {
    auto server = vgi_rpc::ServerBuilder()
        .add_unary("add",
            arrow::schema({
                arrow::field("a", arrow::float64()),
                arrow::field("b", arrow::float64()),
            }),
            arrow::schema({arrow::field("result", arrow::float64())}),
            [](const vgi_rpc::Request& req, vgi_rpc::CallContext& ctx) {
                double a = req.get<double>("a");
                double b = req.get<double>("b");

                arrow::DoubleBuilder builder;
                VGI_RPC_THROW_NOT_OK(builder.Append(a + b));
                auto array = vgi_rpc::unwrap(builder.Finish());

                return vgi_rpc::Result::value(
                    arrow::schema({arrow::field("result", arrow::float64())}),
                    {array});
            },
            "Add two numbers together.")
        .enable_describe("MyServer")
        .build();

    server->run();
}
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
