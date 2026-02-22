# Wire Protocol

`#include <vgi_rpc/wire.h>`

Low-level IPC stream serialization and batch classification.

## BatchType

```cpp
enum class BatchType {
    DATA,
    LOG,
    ERROR,
    EXTERNAL_POINTER,
    SHM_POINTER,
    STATE_TOKEN,
};
```

Classification of batches based on their metadata.

### `classify_batch`

```cpp
BatchType classify_batch(const AnnotatedBatch& ab);
```

Determine the type of a batch from its custom metadata.

## IPC Stream I/O

### `IpcStreamContents`

```cpp
struct IpcStreamContents {
    std::shared_ptr<arrow::Schema> schema;
    std::vector<AnnotatedBatch> batches;
};
```

### `read_ipc_stream`

```cpp
std::optional<IpcStreamContents> read_ipc_stream(
    const std::shared_ptr<arrow::io::InputStream>& input);
```

Read a complete IPC stream (schema + batches + EOS) from an input stream. Returns `std::nullopt` on clean EOF (no data available). Throws on corrupt or partial data.

### `write_ipc_stream`

```cpp
void write_ipc_stream(
    const std::shared_ptr<arrow::io::OutputStream>& output,
    const std::shared_ptr<arrow::Schema>& schema,
    const std::vector<AnnotatedBatch>& batches);
```

Write a complete IPC stream (schema + batches + EOS) to an output stream.

### `drain_reader`

```cpp
void drain_reader(
    const std::shared_ptr<arrow::ipc::RecordBatchStreamReader>& reader);
```

Consume remaining batches from an IPC reader through EOS.

## StdoutStream

```cpp
class StdoutStream : public arrow::io::OutputStream {
public:
    arrow::Status Close() override;
    bool closed() const override;
    arrow::Result<int64_t> Tell() const override;
    arrow::Status Write(const void* data, int64_t nbytes) override;
    arrow::Status Flush() override;
};
```

Thin `arrow::io::OutputStream` adapter for stdout. Used internally by `Server::run()`.
