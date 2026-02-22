# Hello World

The simplest possible vgi-rpc server: a single unary method that adds two numbers.

```cpp title="examples/hello_world.cpp"
#include <vgi_rpc/server.h>
#include <vgi_rpc/request.h>
#include <vgi_rpc/result.h>
#include <vgi_rpc/arrow_utils.h>

#include <arrow/array/builder_primitive.h>
#include <arrow/type.h>

int main() {
    auto params_schema = arrow::schema({
        arrow::field("a", arrow::float64()),
        arrow::field("b", arrow::float64()),
    });

    auto result_schema = arrow::schema({
        arrow::field("result", arrow::float64()),
    });

    auto server = vgi_rpc::ServerBuilder()
        .add_unary("add", params_schema, result_schema,
            [](const vgi_rpc::Request& req, vgi_rpc::CallContext& ctx) {
                double a = req.get<double>("a");
                double b = req.get<double>("b");
                double sum = a + b;

                arrow::DoubleBuilder builder;
                VGI_RPC_THROW_NOT_OK(builder.Append(sum));
                auto array = vgi_rpc::unwrap(builder.Finish());

                return vgi_rpc::Result::value(
                    arrow::schema({arrow::field("result", arrow::float64())}),
                    {array});
            },
            "Add two numbers together.")
        .enable_describe("HelloWorld")
        .build();

    server->run();
    return 0;
}
```

## Key Concepts

**Schema definition** — Both parameters and results are described by Arrow schemas. The `params_schema` declares what the client must send; the `result_schema` declares what the handler returns.

**Parameter extraction** — `req.get<double>("a")` extracts column `"a"` from the request batch as a `double`. Throws if the column is missing, null, or the wrong type.

**Result construction** — `Result::value(schema, {array})` builds a one-row response batch from a schema and a vector of Arrow arrays.

**Error handling** — `VGI_RPC_THROW_NOT_OK()` checks Arrow status codes and throws `std::runtime_error` on failure. `unwrap()` does the same for `arrow::Result<T>`.

**Introspection** — `.enable_describe("HelloWorld")` registers the `__describe__` method, allowing clients to discover this server's methods and schemas at runtime.
