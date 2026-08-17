// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

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
    // Named for what the wire protocol calls it — an EXCEPTION batch — and
    // not ERROR, which <windows.h> defines as a macro and would rewrite this
    // enumerator out from under any consumer that included it first.
    EXCEPTION,
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

// Byte length of the self-contained IPC stream `write_ipc_stream` would emit
// for `batch`, computed without materializing it.  Lets a caller decide
// whether a payload is worth serializing before paying for a copy of it.
VGI_RPC_EXPORT int64_t ipc_stream_byte_size(const std::shared_ptr<arrow::RecordBatch>& batch);

// Drain remaining batches from an IPC reader (consume through EOS).
VGI_RPC_EXPORT void drain_reader(const std::shared_ptr<arrow::ipc::RecordBatchStreamReader>& reader);

// Largest count handed to a single read(2)/write(2)/recv(2)/send(2).
//
// A payload above INT_MAX bytes is not portable as one syscall: macOS pipes
// return a short count of exactly INT_MAX with no error, macOS sockets refuse
// the call outright with EINVAL, and Linux silently caps a transfer at
// 0x7ffff000.  Only clamping *and* looping survives all three — looping alone
// passes on pipes and fails on sockets.  1 GiB sits below every one of those
// ceilings.  See docs/cross-language-conformance.md, "Large payloads".
inline constexpr int64_t kMaxIoChunk = int64_t{1} << 30;

// Write every byte of [data, data + nbytes) to `fd`, looping over short counts
// and clamping each syscall to kMaxIoChunk.  Retries on EINTR.
VGI_RPC_EXPORT arrow::Status write_all_fd(int fd, const void* data, int64_t nbytes);

// Read into `out` until `nbytes` are filled or the peer reaches EOF, clamping
// each syscall to kMaxIoChunk.  Returns the number of bytes actually read; a
// value below `nbytes` means EOF.  Looping is not optional: Arrow asks for a
// whole message body in one Read and reports a short answer as a corrupt
// stream rather than retrying.
VGI_RPC_EXPORT arrow::Result<int64_t> read_full_fd(int fd, void* out, int64_t nbytes);

// InputStream over a raw file descriptor (stdin, a Unix socket, a TCP socket).
class VGI_RPC_EXPORT FdInputStream : public arrow::io::InputStream {
public:
    explicit FdInputStream(int fd) : fd_(fd) {}
    ~FdInputStream() override = default;

    arrow::Status Close() override;
    bool closed() const override { return closed_; }
    arrow::Result<int64_t> Tell() const override { return position_; }
    arrow::Result<int64_t> Read(int64_t nbytes, void* out) override;
    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override;

private:
    int fd_;
    bool closed_ = false;
    int64_t position_ = 0;
};

// OutputStream over a raw file descriptor (stdout, a Unix socket, a TCP socket).
class VGI_RPC_EXPORT FdOutputStream : public arrow::io::OutputStream {
public:
    explicit FdOutputStream(int fd) : fd_(fd) {}
    ~FdOutputStream() override = default;

    arrow::Status Close() override;
    bool closed() const override { return closed_; }
    arrow::Result<int64_t> Tell() const override { return position_; }
    arrow::Status Write(const void* data, int64_t nbytes) override;
    arrow::Status Flush() override;

private:
    int fd_;
    bool closed_ = false;
    int64_t position_ = 0;
};

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
