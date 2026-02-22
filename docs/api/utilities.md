# Utilities

## Arrow Error Handling

`#include <vgi_rpc/arrow_utils.h>`

### `unwrap`

```cpp
template <typename T>
T unwrap(arrow::Result<T>&& result);

template <typename T>
T unwrap(arrow::Result<T>&& result, const char* context);
```

Extract a value from `arrow::Result<T>`, throwing `std::runtime_error` on failure. The two-argument form prepends `context` to the error message.

```cpp
auto array = vgi_rpc::unwrap(builder.Finish());
auto array = vgi_rpc::unwrap(builder.Finish(), "building result array");
```

### `VGI_RPC_THROW_NOT_OK`

```cpp
#define VGI_RPC_THROW_NOT_OK(expr)
```

Check an `arrow::Status` expression and throw `std::runtime_error` on failure.

```cpp
VGI_RPC_THROW_NOT_OK(builder.Append(42.0));
```

## Protocol Constants

`#include <vgi_rpc/metadata.h>`

### Metadata Keys (`vgi_rpc::keys`)

| Key | Value |
|---|---|
| `METHOD` | `"vgi_rpc.method"` |
| `REQUEST_VERSION` | `"vgi_rpc.request_version"` |
| `REQUEST_ID` | `"vgi_rpc.request_id"` |
| `SERVER_ID` | `"vgi_rpc.server_id"` |
| `STREAM_STATE` | `"vgi_rpc.stream_state"` |
| `LOG_LEVEL` | `"vgi_rpc.log_level"` |
| `LOG_MESSAGE` | `"vgi_rpc.log_message"` |
| `LOG_EXTRA` | `"vgi_rpc.log_extra"` |
| `PROTOCOL_NAME` | `"vgi_rpc.protocol_name"` |
| `DESCRIBE_VERSION` | `"vgi_rpc.describe_version"` |
| `TRACEPARENT` | `"traceparent"` |
| `TRACESTATE` | `"tracestate"` |

### Protocol Version Constants

```cpp
inline constexpr const char* REQUEST_VERSION_VALUE = "1";
inline constexpr const char* DESCRIBE_VERSION_VALUE = "2";
inline constexpr const char* DESCRIBE_METHOD_NAME = "__describe__";
```

### Helper Functions

#### `empty_schema`

```cpp
const std::shared_ptr<arrow::Schema>& empty_schema();
```

Returns a shared empty schema. Used for void results, protocol errors, and producer tick input.

#### `make_empty_batch`

```cpp
std::shared_ptr<arrow::RecordBatch> make_empty_batch(
    const std::shared_ptr<arrow::Schema>& schema);
```

Create a zero-row batch on the given schema.

#### `random_hex`

```cpp
std::string random_hex(size_t length);
```

Generate a random hex string of the given length.

#### `get_metadata_value`

```cpp
std::string get_metadata_value(
    const std::shared_ptr<arrow::KeyValueMetadata>& metadata,
    const std::string& key,
    const std::string& default_value = "");
```

Retrieve a metadata value by key, returning `default_value` if not found.

## Logging

`#include <vgi_rpc/log.h>`

### `LogLevel`

```cpp
enum class LogLevel {
    TRACE, DEBUG, INFO, WARN, ERROR, EXCEPTION
};
```

### `Message`

```cpp
struct Message {
    LogLevel level;
    std::string message;
    nlohmann::json extra = {};
};
```

### Conversion Functions

```cpp
const char* log_level_to_string(LogLevel level);
LogLevel log_level_from_string(const std::string& s);
```
