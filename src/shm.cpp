// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/shm.h"

#include "vgi_rpc/arrow_utils.h"
#include "vgi_rpc/server.h"
#include "vgi_rpc/metadata.h"
#include "vgi_rpc/wire.h"

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <stdexcept>

#ifndef _WIN32
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace vgi_rpc {

namespace {

constexpr char kMagic[4] = {'V', 'G', 'I', 'S'};

// The 8-byte end-of-stream marker Arrow writes to close an IPC stream.
constexpr char kIpcEos[8] = {'\xff', '\xff', '\xff', '\xff', '\0', '\0', '\0', '\0'};

uint32_t load_u32(const uint8_t* p) {
    uint32_t v;
    std::memcpy(&v, p, sizeof(v));
    return v;
}

uint64_t load_u64(const uint8_t* p) {
    uint64_t v;
    std::memcpy(&v, p, sizeof(v));
    return v;
}

void store_u32(uint8_t* p, uint32_t v) {
    std::memcpy(p, &v, sizeof(v));
}
void store_u64(uint8_t* p, uint64_t v) {
    std::memcpy(p, &v, sizeof(v));
}

bool has_dictionary_columns(const std::shared_ptr<arrow::Schema>& schema) {
    for (const auto& field : schema->fields()) {
        if (field->type()->id() == arrow::Type::DICTIONARY) return true;
    }
    return false;
}

// Serialize a dictionary-encoded batch for storage: a full IPC stream with the
// schema message and the EOS marker stripped, leaving the dictionary messages
// and the record batch.  The reader rebuilds the stream around them, because
// the schema already travels on the pointer batch.
std::string serialize_dictionary_batch(const std::shared_ptr<arrow::RecordBatch>& batch) {
    auto sink = unwrap(arrow::io::BufferOutputStream::Create());
    auto writer = unwrap(arrow::ipc::MakeStreamWriter(sink, batch->schema()));
    VGI_RPC_THROW_NOT_OK(writer->WriteRecordBatch(*batch));
    VGI_RPC_THROW_NOT_OK(writer->Close());
    auto full = unwrap(sink->Finish());

    // Re-read the stream a message at a time and keep everything after the
    // schema, so the split lands on a real message boundary rather than a
    // guessed byte offset.
    auto reader = std::make_shared<arrow::io::BufferReader>(full);
    auto message_reader = arrow::ipc::MessageReader::Open(reader);
    auto schema_message = unwrap(message_reader->ReadNextMessage());  // discard
    (void)schema_message;

    auto kept = unwrap(arrow::io::BufferOutputStream::Create());
    while (true) {
        auto msg_result = message_reader->ReadNextMessage();
        if (!msg_result.ok()) break;
        auto msg = std::move(msg_result).ValueUnsafe();
        if (!msg) break;  // EOS
        int64_t written = 0;
        VGI_RPC_THROW_NOT_OK(
            msg->SerializeTo(kept.get(), arrow::ipc::IpcWriteOptions::Defaults(), &written));
    }
    auto buf = unwrap(kept->Finish());
    return std::string(reinterpret_cast<const char*>(buf->data()),
                       static_cast<size_t>(buf->size()));
}

// Inverse of the above, plus the trivial case: a non-dictionary batch is
// stored as a complete stream and parses directly.
std::shared_ptr<arrow::RecordBatch> deserialize_from_shm(
    const std::string& stored, const std::shared_ptr<arrow::Schema>& schema) {
    std::string stream;
    if (has_dictionary_columns(schema)) {
        // Rebuild the stream: a schema message (an empty stream, minus its
        // trailing EOS), then the stored messages, then EOS.
        auto sink = unwrap(arrow::io::BufferOutputStream::Create());
        auto writer = unwrap(arrow::ipc::MakeStreamWriter(sink, schema));
        VGI_RPC_THROW_NOT_OK(writer->Close());
        auto empty = unwrap(sink->Finish());
        const auto* p = reinterpret_cast<const char*>(empty->data());
        const size_t schema_len = static_cast<size_t>(empty->size()) - sizeof(kIpcEos);
        stream.assign(p, schema_len);
        stream += stored;
        stream.append(kIpcEos, sizeof(kIpcEos));
    } else {
        stream = stored;
    }

    auto buf = arrow::Buffer::FromString(stream);
    auto reader = std::make_shared<arrow::io::BufferReader>(buf);
    auto stream_reader = unwrap(arrow::ipc::RecordBatchStreamReader::Open(reader));
    auto batch = unwrap(stream_reader->Next());
    if (!batch) throw std::runtime_error("shared-memory region held no record batch");
    return batch;
}

// An OutputStream over a region of the mapped segment.  Arrow's IPC writer
// writes through it directly, which is the point: the alternative is to
// serialize into a heap buffer and then copy that in, paying for the batch
// twice.
//
// Hard-bounded.  The region has to be reserved before the exact byte count is
// known, so a size estimate decides it; a writer that ran past the reservation
// would silently corrupt whatever was allocated next, which is the worst
// failure this file could have.  Overrunning fails the write instead, and the
// caller falls back to sending the batch inline.
class ShmSink : public arrow::io::OutputStream {
public:
    ShmSink(uint8_t* base, int64_t capacity) : base_(base), capacity_(capacity) {}

    arrow::Status Close() override {
        closed_ = true;
        return arrow::Status::OK();
    }
    bool closed() const override { return closed_; }
    arrow::Result<int64_t> Tell() const override { return position_; }

    arrow::Status Write(const void* data, int64_t nbytes) override {
        if (position_ + nbytes > capacity_) {
            return arrow::Status::CapacityError(
                "shared-memory reservation too small: needed at least ", position_ + nbytes,
                ", reserved ", capacity_);
        }
        std::memcpy(base_ + position_, data, static_cast<size_t>(nbytes));
        position_ += nbytes;
        return arrow::Status::OK();
    }

    arrow::Status Flush() override { return arrow::Status::OK(); }

    int64_t bytes_written() const noexcept { return position_; }

private:
    uint8_t* base_;
    int64_t capacity_;
    int64_t position_ = 0;
    bool closed_ = false;
};

// Bytes an IPC stream carrying `batch` needs: the schema message, the record
// batch message, and the end-of-stream marker.  The record batch half comes
// from GetRecordBatchSize, which reads the buffer layout rather than
// serializing; the schema half is measured against a counting stream, which
// touches no data.  Neither pass copies the batch.
arrow::Result<int64_t> estimate_stream_size(const std::shared_ptr<arrow::RecordBatch>& batch) {
    int64_t body_size = 0;
    ARROW_RETURN_NOT_OK(arrow::ipc::GetRecordBatchSize(*batch, &body_size));

    auto counter = std::make_shared<arrow::io::MockOutputStream>();
    ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeStreamWriter(counter, batch->schema()));
    ARROW_RETURN_NOT_OK(writer->Close());  // schema message + EOS
    const int64_t schema_and_eos = counter->GetExtentBytesWritten();

    // A little slack for alignment padding the two measurements do not model
    // between them.  Costs nothing: the pointer records what was actually
    // written, so slack is only ever reserved, never transmitted.
    return schema_and_eos + body_size + 64;
}

}  // namespace

bool shm_available() {
#ifdef _WIN32
    return false;
#else
    return true;
#endif
}

int64_t shm_min_batch_bytes() {
    // Explicit return type: std::stoll yields long long while int64_t is long
    // on LP64 Linux, so a deduced type is ambiguous there and compiles only by
    // luck on platforms where the two happen to be the same.
    static const int64_t value = []() -> int64_t {
        if (const char* raw = std::getenv("VGI_RPC_SHM_MIN_BATCH_BYTES")) {
            try {
                return std::stoll(raw);
            } catch (const std::exception&) {
                // Fall through to the default rather than refusing to start.
            }
        }
#ifdef _WIN32
        // The page-file mapping plus a fast overlapped-pipe read pushes the
        // crossover much higher on Windows than on POSIX.
        return int64_t{1024} * 1024;
#else
        return int64_t{128} * 1024;
#endif
    }();
    return value;
}

// ---------------------------------------------------------------------------
// ShmSegment
// ---------------------------------------------------------------------------

#ifndef _WIN32

std::shared_ptr<ShmSegment> ShmSegment::create(const std::string& name, size_t size) {
    if (size <= kShmHeaderSize) return nullptr;
    const std::string path = name.empty() || name[0] == '/' ? name : "/" + name;
    ::shm_unlink(path.c_str());  // a leftover from an unclean exit would collide
    int fd = ::shm_open(path.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
    if (fd < 0) return nullptr;
    if (::ftruncate(fd, static_cast<off_t>(size)) != 0) {
        ::close(fd);
        ::shm_unlink(path.c_str());
        return nullptr;
    }

    struct stat st{};
    size_t actual = size;
    if (::fstat(fd, &st) == 0 && st.st_size > 0) actual = static_cast<size_t>(st.st_size);

    void* base = ::mmap(nullptr, actual, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    ::close(fd);
    if (base == MAP_FAILED) {
        ::shm_unlink(path.c_str());
        return nullptr;
    }

    // The header records the *actual* mapped size: macOS rounds a segment up
    // to a page boundary, and a peer attaching would otherwise see a
    // data_size mismatch and refuse the segment.
    auto* header = static_cast<uint8_t*>(base);
    std::memcpy(header, kMagic, sizeof(kMagic));
    store_u32(header + 4, kShmVersion);
    store_u64(header + 8, static_cast<uint64_t>(actual - kShmHeaderSize));
    store_u32(header + 16, 0);
    store_u32(header + 20, 0);

    return std::shared_ptr<ShmSegment>(new ShmSegment(base, actual, name, /*owned=*/true));
}

std::shared_ptr<ShmSegment> ShmSegment::attach(const std::string& name, size_t size) {
    // Python's SharedMemory reports its name without the leading slash and
    // prepends one when opening, so a peer's advertised name arrives bare.
    const std::string path = name.empty() || name[0] == '/' ? name : "/" + name;
    int fd = ::shm_open(path.c_str(), O_RDWR, 0600);
    if (fd < 0) return nullptr;

    // Trust the kernel's size over the peer's hint: macOS rounds a segment up
    // to a page boundary, and the header was written with the rounded value.
    struct stat st{};
    size_t actual = size;
    if (::fstat(fd, &st) == 0 && st.st_size > 0) {
        actual = static_cast<size_t>(st.st_size);
    }
    if (actual <= kShmHeaderSize) {
        ::close(fd);
        return nullptr;
    }

    void* base = ::mmap(nullptr, actual, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    ::close(fd);  // the mapping keeps the segment alive
    if (base == MAP_FAILED) return nullptr;

    const auto* header = static_cast<const uint8_t*>(base);
    if (std::memcmp(header, kMagic, sizeof(kMagic)) != 0 || load_u32(header + 4) != kShmVersion ||
        load_u64(header + 8) != static_cast<uint64_t>(actual - kShmHeaderSize)) {
        ::munmap(base, actual);
        return nullptr;
    }

    return std::shared_ptr<ShmSegment>(new ShmSegment(base, actual, name, /*owned=*/false));
}

ShmSegment::~ShmSegment() {
    if (base_) ::munmap(base_, size_);
    if (owned_) {
        const std::string path = name_.empty() || name_[0] == '/' ? name_ : "/" + name_;
        ::shm_unlink(path.c_str());
    }
}

#else  // _WIN32

std::shared_ptr<ShmSegment> ShmSegment::create(const std::string&, size_t) {
    return nullptr;
}

std::shared_ptr<ShmSegment> ShmSegment::attach(const std::string&, size_t) {
    return nullptr;  // reported as shm = "false" by __transport_options__
}

ShmSegment::~ShmSegment() = default;

#endif  // _WIN32

uint32_t ShmSegment::num_allocs() const noexcept {
    return load_u32(bytes() + 16);
}

uint32_t ShmSegment::live_allocations() const noexcept {
    return num_allocs();
}

void ShmSegment::reset() {
    set_num_allocs(0);
}

void ShmSegment::set_num_allocs(uint32_t n) noexcept {
    store_u32(bytes() + 16, n);
}

std::optional<int64_t> ShmSegment::allocate(int64_t size) {
    if (size <= 0) return std::nullopt;
    const uint32_t count = num_allocs();
    if (count >= kShmMaxAllocs) return std::nullopt;

    uint8_t* table = bytes() + kShmHeaderStructSize;
    auto entry_offset = [&](uint32_t i) { return load_u64(table + i * kShmAllocStructSize); };
    auto entry_length = [&](uint32_t i) { return load_u64(table + i * kShmAllocStructSize + 8); };

    // Insert into the sorted list at the first gap that fits.  Only occupied
    // regions are tracked, so a freed neighbour widens the gap on its own —
    // that is what "implicit coalescing" means here.
    auto insert_at = [&](uint32_t index, uint64_t offset) {
        std::memmove(table + (index + 1) * kShmAllocStructSize, table + index * kShmAllocStructSize,
                     (count - index) * kShmAllocStructSize);
        store_u64(table + index * kShmAllocStructSize, offset);
        store_u64(table + index * kShmAllocStructSize + 8, static_cast<uint64_t>(size));
        set_num_allocs(count + 1);
    };

    uint64_t prev_end = kShmHeaderSize;
    for (uint32_t i = 0; i < count; ++i) {
        const uint64_t off = entry_offset(i);
        if (off >= prev_end && off - prev_end >= static_cast<uint64_t>(size)) {
            insert_at(i, prev_end);
            return static_cast<int64_t>(prev_end);
        }
        prev_end = off + entry_length(i);
    }
    if (size_ > prev_end && size_ - prev_end >= static_cast<uint64_t>(size)) {
        insert_at(count, prev_end);
        return static_cast<int64_t>(prev_end);
    }
    return std::nullopt;
}

void ShmSegment::free_alloc(int64_t offset) {
    const uint32_t count = num_allocs();
    uint8_t* table = bytes() + kShmHeaderStructSize;
    for (uint32_t i = 0; i < count; ++i) {
        if (load_u64(table + i * kShmAllocStructSize) == static_cast<uint64_t>(offset)) {
            std::memmove(table + i * kShmAllocStructSize, table + (i + 1) * kShmAllocStructSize,
                         (count - i - 1) * kShmAllocStructSize);
            set_num_allocs(count - 1);
            return;
        }
    }
    // Not an error worth throwing over: a peer may have reset the segment
    // between the write and the free, and the region is dead either way.
}

std::string ShmSegment::read(int64_t offset, int64_t length) const {
    if (offset < static_cast<int64_t>(kShmHeaderSize) || length < 0 ||
        static_cast<uint64_t>(offset) + static_cast<uint64_t>(length) > size_) {
        throw std::runtime_error("shared-memory pointer is outside the segment");
    }
    return std::string(reinterpret_cast<const char*>(bytes() + offset),
                       static_cast<size_t>(length));
}

std::optional<std::pair<int64_t, int64_t>> ShmSegment::allocate_and_write(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (has_dictionary_columns(batch->schema())) {
        const std::string payload = serialize_dictionary_batch(batch);
        auto offset = allocate(static_cast<int64_t>(payload.size()));
        if (!offset) return std::nullopt;
        std::memcpy(bytes() + *offset, payload.data(), payload.size());
        return std::make_pair(*offset, static_cast<int64_t>(payload.size()));
    }

    // Non-dictionary: reserve from a measured estimate and let Arrow write
    // straight into the segment, so the batch is paid for once rather than
    // serialized to a heap buffer and copied in.
    auto estimate = estimate_stream_size(batch);
    if (!estimate.ok()) return std::nullopt;

    auto offset = allocate(*estimate);
    if (!offset) return std::nullopt;

    auto sink = std::make_shared<ShmSink>(bytes() + *offset, *estimate);
    auto writer_result = arrow::ipc::MakeStreamWriter(sink, batch->schema());
    if (!writer_result.ok()) {
        free_alloc(*offset);
        return std::nullopt;
    }
    auto writer = std::move(writer_result).ValueUnsafe();
    if (!writer->WriteRecordBatch(*batch).ok() || !writer->Close().ok()) {
        // Includes the estimate coming up short, which the sink refuses rather
        // than writing past the reservation.
        free_alloc(*offset);
        return std::nullopt;
    }
    // The reservation keeps the estimate so the next allocation cannot overlap
    // the slack; the pointer carries what was actually written.
    return std::make_pair(*offset, sink->bytes_written());
}

// ---------------------------------------------------------------------------
// Pointer batches
// ---------------------------------------------------------------------------

bool is_shm_pointer_batch(const std::shared_ptr<arrow::RecordBatch>& batch,
                          const std::shared_ptr<arrow::KeyValueMetadata>& custom_metadata) {
    if (!batch || batch->num_rows() != 0 || !custom_metadata) return false;
    if (custom_metadata->FindKey(keys::SHM_OFFSET) < 0) return false;
    // A log batch is zero-row too; the level key is what tells them apart.
    return custom_metadata->FindKey(keys::LOG_LEVEL) < 0;
}

std::shared_ptr<arrow::RecordBatch> resolve_shm_batch(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    std::shared_ptr<arrow::KeyValueMetadata>* custom_metadata,
    const std::shared_ptr<ShmSegment>& segment, int64_t* out_free_offset) {
    if (out_free_offset) *out_free_offset = -1;
    if (!segment || !custom_metadata || !is_shm_pointer_batch(batch, *custom_metadata)) {
        return batch;
    }

    const std::string offset_str = get_metadata_value(*custom_metadata, keys::SHM_OFFSET);
    const std::string length_str = get_metadata_value(*custom_metadata, keys::SHM_LENGTH);
    int64_t offset = 0, length = 0;
    try {
        offset = std::stoll(offset_str);
        length = std::stoll(length_str);
    } catch (const std::exception&) {
        throw std::runtime_error("shared-memory pointer batch has an unreadable offset/length");
    }

    auto resolved = deserialize_from_shm(segment->read(offset, length), batch->schema());

    // Strip the pointer keys and record where the bytes came from, so a
    // downstream reader cannot mistake the resolved batch for another pointer.
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    for (int64_t i = 0; i < (*custom_metadata)->size(); ++i) {
        const std::string& key = (*custom_metadata)->key(i);
        if (key == keys::SHM_OFFSET || key == keys::SHM_LENGTH) continue;
        md->Append(key, (*custom_metadata)->value(i));
    }
    md->Append(keys::SHM_SOURCE, segment->name());
    *custom_metadata = md;

    if (out_free_offset) *out_free_offset = offset;
    return resolved;
}

std::shared_ptr<arrow::RecordBatch> maybe_write_to_shm(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    std::shared_ptr<arrow::KeyValueMetadata>* custom_metadata,
    const std::shared_ptr<ShmSegment>& segment) {
    if (!segment || !batch || batch->num_rows() == 0) return batch;

    int64_t nbytes = 0;
    for (const auto& column : batch->columns()) {
        for (const auto& buffer : column->data()->buffers) {
            if (buffer) nbytes += buffer->size();
        }
    }
    if (nbytes < shm_min_batch_bytes()) return batch;

    auto written = segment->allocate_and_write(batch);
    // A full allocator is not a failure: send the batch inline instead.
    if (!written) return batch;

    auto md = std::make_shared<arrow::KeyValueMetadata>();
    if (custom_metadata && *custom_metadata) {
        for (int64_t i = 0; i < (*custom_metadata)->size(); ++i) {
            md->Append((*custom_metadata)->key(i), (*custom_metadata)->value(i));
        }
    }
    md->Append(keys::SHM_OFFSET, std::to_string(written->first));
    md->Append(keys::SHM_LENGTH, std::to_string(written->second));
    if (custom_metadata) *custom_metadata = md;

    return make_empty_batch(batch->schema());
}

void register_transport_options(std::unordered_map<std::string, MethodInfo>& methods,
                                const std::string& server_id) {
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    // Advertised false on a build that cannot map a segment at all, so a peer
    // stays on the pipe instead of writing pointers we could never resolve.
    md->Append(keys::TRANSPORT_SHM, shm_available() ? "true" : "false");
    md->Append(keys::SERVER_ID, server_id);
    md->Append(keys::REQUEST_VERSION, REQUEST_VERSION_VALUE);

    MethodInfo info;
    info.name = TRANSPORT_OPTIONS_METHOD_NAME;
    info.method_type = MethodType::UNARY;
    info.params_schema = empty_schema();
    info.result_schema = empty_schema();
    info.has_return = true;
    info.doc = "Report this server's transport capabilities.";
    info.handler = [md](const Request&, CallContext&) -> Result {
        // An empty batch: the answer rides entirely in custom_metadata, whose
        // vgi_rpc.transport.* keys are matched by prefix so unknown ones are
        // simply ignored by an older peer.
        AnnotatedBatch ab;
        ab.batch = make_empty_batch(empty_schema());
        ab.custom_metadata = md;
        return Result::from_annotated_batch(std::move(ab));
    };
    methods[TRANSPORT_OPTIONS_METHOD_NAME] = std::move(info);
}

}  // namespace vgi_rpc
