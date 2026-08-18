# Request

`#include <vgi_rpc/request.h>`

Typed read-only view over an incoming RPC request batch. Wraps a single-row Arrow RecordBatch and its custom metadata.

## Parameter Extraction

### `get<T>(name)`

```cpp
template <typename T>
T get(std::string_view name) const;
```

Extract a typed parameter by column name from row 0. Throws on missing column, null value, or type mismatch.

### `get_optional<T>(name)`

```cpp
template <typename T>
std::optional<T> get_optional(std::string_view name) const;
```

Like `get<T>()` but returns `std::nullopt` when the column is missing or the value is null. Throws only on type mismatch.

### Supported Types

| C++ Type | Arrow Type |
|---|---|
| `double` | `float64` |
| `float` | `float32` |
| `int64_t` | `int64` |
| `int32_t` | `int32` |
| `bool` | `boolean` |
| `std::string` | `utf8` |
| `std::vector<uint8_t>` | `binary` |
| `std::vector<std::string>` | `list<utf8>` |
| `std::vector<int64_t>` | `list<int64>` |
| `std::vector<double>` | `list<float64>` |

## Other Methods

### `method_name`

```cpp
std::string method_name() const;
```

The method name from request metadata.

### `request_id`

```cpp
std::string request_id() const;
```

The request ID from request metadata.

### `request_version`

```cpp
std::string request_version() const;
```

### `schema`

```cpp
const std::shared_ptr<arrow::Schema>& schema() const;
```

The request batch schema.

### `batch`

```cpp
const std::shared_ptr<arrow::RecordBatch>& batch() const noexcept;
```

The underlying Arrow record batch.

### `metadata`

```cpp
const std::shared_ptr<arrow::KeyValueMetadata>& metadata() const noexcept;
```

The raw request metadata.

### `has_param`

```cpp
bool has_param(std::string_view name) const;
```

Check whether a column exists in the request batch.

### `get_column`

```cpp
std::shared_ptr<arrow::Array> get_column(std::string_view name) const;
```

Raw column accessor. Returns `nullptr` if column not found.

## Example

```cpp
auto handler = [](const vgi_rpc::Request& req, vgi_rpc::CallContext& ctx) {
    // Required parameter
    double value = req.get<double>("value");

    // Optional parameter with default
    auto scale = req.get_optional<double>("scale").value_or(1.0);

    // Check existence
    if (req.has_param("label")) {
        std::string label = req.get<std::string>("label");
    }

    // ...
};
```
