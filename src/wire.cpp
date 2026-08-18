// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/wire.h"
#include "vgi_rpc/arrow_utils.h"
#include "vgi_rpc/metadata.h"

#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/buffer.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#ifdef _WIN32
#include <io.h>
// windows.h defines min/max as macros and ERROR as a constant, all of
// which collide with ordinary C++ spellings; these three keep it to itself.
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#define NOGDI
#include <windows.h>
#else
#include <unistd.h>
#endif
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <stdexcept>

namespace vgi_rpc {

BatchType classify_batch(const AnnotatedBatch& ab) {
    const auto& md = ab.custom_metadata;

    // 1. No metadata → DATA
    if (!md) return BatchType::DATA;

    // 2. num_rows > 0 → DATA
    if (ab.batch && ab.batch->num_rows() > 0) return BatchType::DATA;

    // 3. Has log_level + log_message → LOG or ERROR
    auto level_idx = md->FindKey(keys::LOG_LEVEL);
    auto msg_idx = md->FindKey(keys::LOG_MESSAGE);
    if (level_idx >= 0 && msg_idx >= 0) {
        auto level_str = md->value(level_idx);
        if (level_str == "EXCEPTION") return BatchType::EXCEPTION;
        return BatchType::LOG;
    }

    // 4. Has location → EXTERNAL_POINTER
    if (md->FindKey(keys::LOCATION) >= 0) return BatchType::EXTERNAL_POINTER;

    // 5. Has shm_offset → SHM_POINTER
    if (md->FindKey(keys::SHM_OFFSET) >= 0) return BatchType::SHM_POINTER;

    // 6. Has stream_state → STATE_TOKEN
    if (md->FindKey(keys::STREAM_STATE) >= 0) return BatchType::STATE_TOKEN;

    // 7. Otherwise → DATA (void return)
    return BatchType::DATA;
}

BatchType AnnotatedBatch::type() const {
    return classify_batch(*this);
}

std::optional<IpcStreamContents> read_ipc_stream(
    const std::shared_ptr<arrow::io::InputStream>& input) {
    auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(input);
    if (!reader_result.ok()) {
        auto& status = reader_result.status();
        // Arrow raises IOError for truncated/empty streams (clean EOF).
        if (status.IsIOError()) {
            return std::nullopt;
        }
        // Arrow also raises Invalid with "null or length 0" for empty input —
        // treat that as clean EOF too.  Other Invalid statuses (corrupt magic
        // bytes, bad flatbuffer, etc.) should propagate as errors.
        if (status.IsInvalid()) {
            auto msg = status.message();
            if (msg.find("was null or length 0") != std::string::npos) {
                return std::nullopt;
            }
        }
        throw std::runtime_error("Failed to open IPC reader: " + status.ToString());
    }
    auto reader = std::move(reader_result).ValueUnsafe();

    IpcStreamContents contents;
    contents.schema = reader->schema();

    while (true) {
        auto result = reader->ReadNext();
        if (!result.ok()) {
            throw std::runtime_error("Error reading IPC batch: " + result.status().ToString());
        }
        auto batch_with_md = std::move(result).ValueUnsafe();
        if (!batch_with_md.batch) break;  // EOS

        AnnotatedBatch ab;
        ab.batch = batch_with_md.batch;
        if (batch_with_md.custom_metadata) {
            ab.custom_metadata = std::static_pointer_cast<arrow::KeyValueMetadata>(
                batch_with_md.custom_metadata->Copy());
        }
        contents.batches.push_back(std::move(ab));
    }

    return contents;
}

void write_ipc_stream(const std::shared_ptr<arrow::io::OutputStream>& output,
                      const std::shared_ptr<arrow::Schema>& schema,
                      const std::vector<AnnotatedBatch>& batches) {
    auto writer =
        unwrap(arrow::ipc::MakeStreamWriter(output, schema), "Failed to create IPC writer");

    for (const auto& ab : batches) {
        if (ab.custom_metadata) {
            VGI_RPC_THROW_NOT_OK(writer->WriteRecordBatch(*ab.batch, ab.custom_metadata));
        } else {
            VGI_RPC_THROW_NOT_OK(writer->WriteRecordBatch(*ab.batch));
        }
    }

    VGI_RPC_THROW_NOT_OK(writer->Close());
}

int64_t ipc_stream_byte_size(const std::shared_ptr<arrow::RecordBatch>& batch) {
    auto counter = std::make_shared<arrow::io::MockOutputStream>();
    write_ipc_stream(counter, batch->schema(), {AnnotatedBatch::data(batch)});
    return counter->GetExtentBytesWritten();
}

// Consumes remaining batches from an IPC reader through the end-of-stream
// marker.  This call blocks until the sender closes their end of the stream.
// Used after early-exit to keep the pipe in a consistent state for the next
// request.
void drain_reader(const std::shared_ptr<arrow::ipc::RecordBatchStreamReader>& reader) {
    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        auto status = reader->ReadNext(&batch);
        if (!status.ok() || !batch) break;
    }
}

// Clamped, looping fd primitives.  Both the clamp and the loop are load
// bearing — see the comment on kMaxIoChunk in wire.h.
arrow::Status write_all_fd(int fd, const void* data, int64_t nbytes) {
    const auto* bytes = static_cast<const uint8_t*>(data);
    int64_t written = 0;
    while (written < nbytes) {
        const int64_t chunk = std::min<int64_t>(nbytes - written, kMaxIoChunk);
#ifdef _WIN32
        auto n = ::_write(fd, bytes + written, static_cast<unsigned int>(chunk));
#else
        auto n = ::write(fd, bytes + written, static_cast<size_t>(chunk));
#endif
        if (n < 0) {
            if (errno == EINTR) continue;
            if (errno == EPIPE) return arrow::Status::IOError("Broken pipe");
            return arrow::Status::IOError("Write failed: ", std::strerror(errno));
        }
        if (n == 0) return arrow::Status::IOError("Write made no progress");
        written += n;
    }
    return arrow::Status::OK();
}

arrow::Result<int64_t> read_full_fd(int fd, void* out, int64_t nbytes) {
    auto* bytes = static_cast<uint8_t*>(out);
    int64_t total = 0;
    while (total < nbytes) {
        const int64_t chunk = std::min<int64_t>(nbytes - total, kMaxIoChunk);
#ifdef _WIN32
        auto n = ::_read(fd, bytes + total, static_cast<unsigned int>(chunk));
#else
        auto n = ::read(fd, bytes + total, static_cast<size_t>(chunk));
#endif
        if (n < 0) {
            if (errno == EINTR) continue;
            return arrow::Status::IOError("Read failed: ", std::strerror(errno));
        }
        if (n == 0) break;  // EOF
        total += n;
    }
    return total;
}

// FdInputStream implementation
arrow::Status FdInputStream::Close() {
    closed_ = true;
    return arrow::Status::OK();
}

arrow::Result<int64_t> FdInputStream::Read(int64_t nbytes, void* out) {
    if (closed_) return arrow::Status::IOError("FdInputStream is closed");
    ARROW_ASSIGN_OR_RAISE(int64_t n, read_full_fd(fd_, out, nbytes));
    position_ += n;
    return n;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> FdInputStream::Read(int64_t nbytes) {
    ARROW_ASSIGN_OR_RAISE(auto buf, arrow::AllocateResizableBuffer(nbytes));
    ARROW_ASSIGN_OR_RAISE(int64_t n, Read(nbytes, buf->mutable_data()));
    if (n < nbytes) {
        RETURN_NOT_OK(buf->Resize(n, /*shrink_to_fit=*/false));
    }
    return std::shared_ptr<arrow::Buffer>(std::move(buf));
}

// FdOutputStream implementation
arrow::Status FdOutputStream::Close() {
    closed_ = true;
    return arrow::Status::OK();
}

arrow::Status FdOutputStream::Write(const void* data, int64_t nbytes) {
    if (closed_) return arrow::Status::IOError("FdOutputStream is closed");
    RETURN_NOT_OK(write_all_fd(fd_, data, nbytes));
    position_ += nbytes;
    return arrow::Status::OK();
}

arrow::Status FdOutputStream::Flush() {
    // Sockets and pipes have no userspace buffer here — the bytes are in the
    // kernel once write(2) returns — and fsync on a non-seekable fd is either
    // redundant or an EINVAL.
    return arrow::Status::OK();
}

// StdoutStream implementation
arrow::Status StdoutStream::Close() {
    if (!closed_) {
        auto s = Flush();
        closed_ = true;
        return s;
    }
    return arrow::Status::OK();
}

arrow::Status StdoutStream::Write(const void* data, int64_t nbytes) {
    if (closed_) return arrow::Status::IOError("StdoutStream is closed");

#ifdef _WIN32
    // Go through the Win32 handle rather than the CRT fd so that stdout is not
    // subject to text-mode translation, which would corrupt Arrow IPC.
    const auto* bytes = static_cast<const uint8_t*>(data);
    HANDLE h = GetStdHandle(STD_OUTPUT_HANDLE);
    if (h == INVALID_HANDLE_VALUE) {
        return arrow::Status::IOError("Failed to get stdout handle");
    }
    int64_t written = 0;
    while (written < nbytes) {
        DWORD to_write = static_cast<DWORD>(std::min<int64_t>(nbytes - written, kMaxIoChunk));
        DWORD n = 0;
        if (!WriteFile(h, bytes + written, to_write, &n, nullptr)) {
            return arrow::Status::IOError("Write to stdout failed");
        }
        if (n == 0) return arrow::Status::IOError("Write to stdout made no progress");
        written += n;
    }
#else
    RETURN_NOT_OK(write_all_fd(STDOUT_FILENO, data, nbytes));
#endif
    position_ += nbytes;
    return arrow::Status::OK();
}

arrow::Status StdoutStream::Flush() {
    // Intentional no-op: stdout writes go to a pipe, and POSIX pipes have no
    // fsync semantic.  The data is already in the kernel buffer after write().
    // Calling fflush/fsync here would either be redundant or, on some
    // platforms, return EINVAL for non-seekable file descriptors.
    return arrow::Status::OK();
}

}  // namespace vgi_rpc
