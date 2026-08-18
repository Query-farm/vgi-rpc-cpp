# Calculator

A server with multiple unary methods demonstrating logging and error handling.

```cpp title="examples/calculator.cpp"
--8<-- "examples/calculator.cpp"
```

## Key Concepts

**Multiple methods** — Call `add_unary()` multiple times to register several methods on the same server. Each gets its own schema and handler.

**Client logging** — `ctx.client_log(LogLevel::DEBUG, "message")` sends a log message to the client as an in-band LOG batch. Available log levels: `TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR`.

**Error handling** — Throwing `std::runtime_error` (or any exception) from a handler automatically converts it to a protocol-level EXCEPTION response. The client receives the error message without crashing the server.

**Helper functions** — Factor out common result construction (like `make_double_result`) to reduce boilerplate across handlers.
