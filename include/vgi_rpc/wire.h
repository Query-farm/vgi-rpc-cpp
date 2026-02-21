#pragma once

#include <memory>
#include <vector>
#include <arrow/io/interfaces.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include "vgi_rpc/annotated_batch.h"
#include "vgi_rpc/export.h"

namespace vgi_rpc {

// Batch classification
enum class BatchType {
    DATA,
    LOG,
    ERROR,
    EXTERNAL_POINTER,
    SHM_POINTER,
    STATE_TOKEN,
};

VGI_RPC_EXPORT BatchType classify_batch(const AnnotatedBatch& ab);

// Parsed IPC stream
struct IpcStreamContents {
    std::shared_ptr<arrow::Schema> schema;
    std::vector<AnnotatedBatch> batches;
};

// Read a complete IPC stream (schema + batches + EOS) from an input stream.
// Returns nullopt on clean EOF (no data available).
// Throws on corrupt/partial data.
VGI_RPC_EXPORT std::optional<IpcStreamContents> read_ipc_stream(
    const std::shared_ptr<arrow::io::InputStream>& input);

// Write a complete IPC stream (schema + batches + EOS) to an output stream.
VGI_RPC_EXPORT void write_ipc_stream(
    const std::shared_ptr<arrow::io::OutputStream>& output,
    const std::shared_ptr<arrow::Schema>& schema,
    const std::vector<AnnotatedBatch>& batches);

// Drain remaining batches from an IPC reader (consume through EOS).
VGI_RPC_EXPORT void drain_reader(const std::shared_ptr<arrow::ipc::RecordBatchStreamReader>& reader);

// StdoutStream — thin OutputStream adapter for stdout
class VGI_RPC_EXPORT StdoutStream : public arrow::io::OutputStream {
public:
    StdoutStream() = default;
    ~StdoutStream() override = default;

    arrow::Status Close() override;
    bool closed() const override { return closed_; }
    arrow::Result<int64_t> Tell() const override { return position_; }
    arrow::Status Write(const void* data, int64_t nbytes) override;
    arrow::Status Flush() override;

private:
    bool closed_ = false;
    int64_t position_ = 0;
};

}  // namespace vgi_rpc
