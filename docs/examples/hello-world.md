# Hello World

The simplest possible vgi-rpc server: a single unary method that adds two numbers.

```cpp title="examples/hello_world.cpp"
--8<-- "examples/hello_world.cpp"
```

## Key Concepts

**Schema definition** — Both parameters and results are described by Arrow schemas. The `params_schema` declares what the client must send; the `result_schema` declares what the handler returns.

**Parameter extraction** — `req.get<double>("a")` extracts column `"a"` from the request batch as a `double`. Throws if the column is missing, null, or the wrong type.

**Result construction** — `Result::value(schema, {array})` builds a one-row response batch from a schema and a vector of Arrow arrays.

**Error handling** — `VGI_RPC_THROW_NOT_OK()` checks Arrow status codes and throws `std::runtime_error` on failure. `unwrap()` does the same for `arrow::Result<T>`.

**Introspection** — `.enable_describe("HelloWorld")` registers the `__describe__` method, allowing clients to discover this server's methods and schemas at runtime.
