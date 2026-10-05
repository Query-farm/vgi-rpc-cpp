# Result

`#include <vgi_rpc/result.h>`

Constructs RPC response batches returned from unary method handlers.

## Static Factories

### `value` (from batch)

```cpp
static Result value(std::shared_ptr<arrow::RecordBatch> batch);
```

Create a result wrapping a pre-built record batch.

### `value` (from schema + arrays)

```cpp
static Result value(
    std::shared_ptr<arrow::Schema> schema,
    std::vector<std::shared_ptr<arrow::Array>> arrays);
```

Create a result from a schema and a vector of arrays. Builds a one-row batch.

### `from_annotated_batch`

```cpp
static Result from_annotated_batch(AnnotatedBatch ab);
```

Create a result from an `AnnotatedBatch` (batch + custom metadata).

### `void_result`

```cpp
static Result void_result();
```

Create a void result (zero-row batch on an empty schema). Use for `add_void()` handlers that need to return explicitly.

### `from_external_ref`

```cpp
static Result from_external_ref(ExternalRef ref);
```

Answer a unary call with a pre-published object (`#include <vgi_rpc/external.h>`;
build the ref with `publish_external`). The server writes the external-location
pointer batch for `ref` on the method's result schema — `vgi_rpc.location`, plus
`vgi_rpc.location.sha256` only when `ref.sha256()` is set — on every transport,
with no serialization or upload during the call, regardless of
`externalize_threshold` or whether storage is configured, never through shared
memory, and without charging `max_externalized_response_bytes`. Unary methods
only.

```cpp
ExternalRef(std::string url, std::optional<std::string> sha256 = std::nullopt);

ExternalRef publish_external(const std::shared_ptr<arrow::RecordBatch>& batch,  // 1 row
                             ExternalStorage& storage,
                             const std::string& compression = "",  // "" or "zstd"
                             bool include_sha256 = true);
```

`ExternalRef` throws `std::invalid_argument` for an empty URL or a digest that
is not 64 lowercase hex characters. `publish_external` serializes the batch as
the per-call externalizer would, hashes the raw bytes, compresses when asked,
uploads once and returns the ref; cache it and own the object's lifecycle.

### `error`

```cpp
static Result error(
    std::shared_ptr<arrow::Schema> schema,
    const std::string& exception_type,
    const std::string& message,
    const std::string& server_id = "",
    const std::string& request_id = "");
```

Create an error result with EXCEPTION metadata. Typically you don't need this — throwing an exception from a handler is the preferred way to signal errors.

## Accessors

### `annotated_batch`

```cpp
const AnnotatedBatch& annotated_batch() const noexcept;
```

### `schema`

```cpp
const std::shared_ptr<arrow::Schema>& schema() const;
```

### `external_ref`

```cpp
const std::optional<ExternalRef>& external_ref() const noexcept;
```

The pre-published reference this result answers with, if any. When set,
`annotated_batch()` is a placeholder the dispatcher does not send.

## `make_error_metadata`

```cpp
std::shared_ptr<arrow::KeyValueMetadata> make_error_metadata(
    const std::string& exception_type,
    const std::string& message,
    const std::string& server_id = "",
    const std::string& request_id = "");
```

Build the standard error metadata used for both unary error results and mid-stream error batches.

## Example

```cpp
// Return a value
auto handler = [](const vgi_rpc::Request& req, vgi_rpc::CallContext& ctx) {
    arrow::DoubleBuilder builder;
    VGI_RPC_THROW_NOT_OK(builder.Append(42.0));
    auto array = vgi_rpc::unwrap(builder.Finish());

    return vgi_rpc::Result::value(
        arrow::schema({arrow::field("answer", arrow::float64())}),
        {array});
};

// Signal error (prefer throwing instead)
// throw std::runtime_error("something went wrong");
```
