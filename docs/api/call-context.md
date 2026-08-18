# CallContext

`#include <vgi_rpc/call_context.h>`

Per-request context passed to method handlers and stream processors. Provides the server/request IDs and a log sink for emitting client-visible log messages.

## Methods

### `client_log`

```cpp
void client_log(LogLevel level, std::string_view message,
                const nlohmann::json& extra = {});
void client_log(const Message& msg);
```

Emit a log message to the client. The message is sent as an in-band LOG batch in the response stream. The `extra` parameter allows attaching structured JSON data to the log entry.

### `server_id`

```cpp
const std::string& server_id() const noexcept;
```

### `request_id`

```cpp
const std::string& request_id() const noexcept;
```

### `log_sink`

```cpp
std::shared_ptr<LogSink> log_sink() const noexcept;
```

Access the underlying log sink directly.

## Log Levels

```cpp
enum class LogLevel {
    TRACE,
    DEBUG,
    INFO,
    WARN,
    ERROR,
    EXCEPTION
};
```

## Example

```cpp
auto handler = [](const vgi_rpc::Request& req, vgi_rpc::CallContext& ctx) {
    ctx.client_log(vgi_rpc::LogLevel::INFO, "Processing request");

    // With structured extra data
    ctx.client_log(vgi_rpc::LogLevel::DEBUG, "Input received",
                   {{"param_count", 3}, {"method", "add"}});

    // ...
};
```
