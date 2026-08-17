// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/// Shared-memory side channel for zero-copy batch transfer between co-located
/// processes.  See docs/WIRE_PROTOCOL.md §11.
///
/// It rides alongside a pipe (or Unix-socket) transport rather than replacing
/// it: the pipe carries control messages and small batches, while a large
/// batch is written into the segment and replaced on the pipe by a zero-row
/// pointer batch naming its offset and length.
///
/// Use is gated on the `__transport_options__` handshake (§15).  A client only
/// advertises a segment to a server that confirmed support, and a server that
/// has not attached one but receives an inbound pointer batch must fail loudly
/// rather than treat the zero-row pointer as empty input.
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>

#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include "vgi_rpc/export.h"

namespace vgi_rpc {

struct MethodInfo;

// Segment layout (§11).  The header is a fixed 64 KiB prefix; the data region
// begins immediately after it, and every offset on the wire is absolute —
// measured from the start of the segment, not the start of the data region.
inline constexpr size_t kShmHeaderSize = 65536;
inline constexpr size_t kShmHeaderStructSize = 24;   // magic|version|data_size|num_allocs|pad
inline constexpr size_t kShmAllocStructSize = 16;    // offset|length
inline constexpr uint32_t kShmVersion = 1;
inline constexpr size_t kShmMaxAllocs =
    (kShmHeaderSize - kShmHeaderStructSize) / kShmAllocStructSize;  // 4094

// Smallest batch worth shipping through the segment.  The fixed per-batch cost
// — a slot allocation, a pointer round trip, and the peer's resolve and free —
// only pays off once the copy it avoids is large enough; below the crossover
// the pipe simply wins.  Overridable via VGI_RPC_SHM_MIN_BATCH_BYTES so the
// gate can be moved without a rebuild.
VGI_RPC_EXPORT int64_t shm_min_batch_bytes();

// A segment owned by the peer, attached read-write for the life of a call.
// Read-write because freeing an allocation rewrites the header's allocation
// list, which a read-only mapping could not do.
class VGI_RPC_EXPORT ShmSegment {
public:
    ~ShmSegment();
    ShmSegment(const ShmSegment&) = delete;
    ShmSegment& operator=(const ShmSegment&) = delete;

    // Create a segment and initialize its allocator header.  A server
    // normally attaches to a peer's segment rather than owning one; this
    // exists so the allocator can be exercised without a second process, and
    // for a future client-side implementation.  `unlink` on destruction
    // removes the name, which only the creator should do.
    static std::shared_ptr<ShmSegment> create(const std::string& name, size_t size);

    // Attach to an existing POSIX segment, validating the header's magic,
    // version, and data_size.  Returns nullptr when the segment cannot be
    // opened or the header does not check out — a caller treats that as "no
    // SHM for this call" and stays on the pipe.
    static std::shared_ptr<ShmSegment> attach(const std::string& name, size_t size);

    // Drop every allocation, leaving the data region reusable.
    void reset();

    // Number of live allocations, for tests and diagnostics.
    uint32_t live_allocations() const noexcept;

    const std::string& name() const noexcept { return name_; }
    size_t size() const noexcept { return size_; }

    // Serialize `batch` into the segment.  Returns (offset, length), or
    // nullopt when the allocator has no gap big enough — in which case the
    // caller sends the batch inline instead of failing.
    std::optional<std::pair<int64_t, int64_t>> allocate_and_write(
        const std::shared_ptr<arrow::RecordBatch>& batch);

    // Read back a region previously written by a peer.
    std::string read(int64_t offset, int64_t length) const;

    // Release a region by its offset.  Adjacent free space coalesces
    // implicitly, since the header tracks only occupied regions.
    void free_alloc(int64_t offset);

private:
    ShmSegment(void* base, size_t size, std::string name, bool owned)
        : base_(base), size_(size), name_(std::move(name)), owned_(owned) {}

    uint8_t* bytes() const noexcept { return static_cast<uint8_t*>(base_); }
    uint32_t num_allocs() const noexcept;
    void set_num_allocs(uint32_t n) noexcept;
    std::optional<int64_t> allocate(int64_t size);

    void* base_ = nullptr;
    size_t size_ = 0;
    std::string name_;
    bool owned_ = false;  // unlink on destruction
};

// True when `batch` is a zero-row batch carrying SHM pointer metadata.  A log
// batch is also zero-row, so `vgi_rpc.log_level` disqualifies it.
VGI_RPC_EXPORT bool is_shm_pointer_batch(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::shared_ptr<arrow::KeyValueMetadata>& custom_metadata);

// Replace a pointer batch with the batch it points at.  `out_free_offset`
// receives the region to release once the columns have been materialized;
// -1 when nothing was resolved.  Throws when the pointer cannot be resolved —
// silently treating it as an empty batch would hand a method zero rows the
// caller never sent.
VGI_RPC_EXPORT std::shared_ptr<arrow::RecordBatch> resolve_shm_batch(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    std::shared_ptr<arrow::KeyValueMetadata>* custom_metadata,
    const std::shared_ptr<ShmSegment>& segment,
    int64_t* out_free_offset);

// Write `batch` into the segment and return a pointer batch, or return the
// batch unchanged when there is no segment, it is too small to be worth it, or
// the allocator is full.
VGI_RPC_EXPORT std::shared_ptr<arrow::RecordBatch> maybe_write_to_shm(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    std::shared_ptr<arrow::KeyValueMetadata>* custom_metadata,
    const std::shared_ptr<ShmSegment>& segment);

// Register the __transport_options__ synthetic method, which answers with an
// empty batch whose custom_metadata carries this server's transport
// capabilities.  Declared here rather than in describe.h because what it
// advertises is decided by whether this build can attach a segment at all.
VGI_RPC_EXPORT void register_transport_options(
    std::unordered_map<std::string, MethodInfo>& methods,
    const std::string& server_id);

}  // namespace vgi_rpc
