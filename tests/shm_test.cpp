// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// The allocator writes a header two processes share, so a mistake here is a
// wrong answer in the peer rather than a crash here. These pin the layout and
// the first-fit/coalesce behaviour that docs/WIRE_PROTOCOL.md §11 specifies.

#include <catch2/catch_test_macros.hpp>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include "vgi_rpc/arrow_utils.h"
#include "vgi_rpc/metadata.h"
#include "vgi_rpc/shm.h"

using namespace vgi_rpc;

namespace {

// Windows has no POSIX shared memory, so there is nothing here to exercise —
// the library reports shm = "false" and peers stay on the pipe.  Skipped
// rather than compiled out, so the absence is visible in the test report.
#define REQUIRE_SHM()                                                    \
    do {                                                                 \
        if (!shm_available()) SKIP("shared memory is POSIX-only");        \
    } while (0)

// A distinct name per test, so a leftover segment from a crashed run cannot
// make the next one pass or fail for the wrong reason.
std::string unique_name(const char* tag) {
    static int counter = 0;
    return std::string("vgi-rpc-test-") + tag + "-" + std::to_string(++counter);
}

std::shared_ptr<arrow::RecordBatch> int_batch(int64_t rows) {
    arrow::Int64Builder builder;
    for (int64_t i = 0; i < rows; ++i) {
        REQUIRE(builder.Append(i).ok());
    }
    auto schema = arrow::schema({arrow::field("value", arrow::int64())});
    return arrow::RecordBatch::Make(schema, rows, {unwrap(builder.Finish())});
}

// Comfortably over the routing threshold, derived rather than hardcoded so
// these keep testing what they claim if the default gate moves.
std::shared_ptr<arrow::RecordBatch> big_int_batch() {
    return int_batch((shm_min_batch_bytes() / 8) * 2);
}

int64_t big_batch_bytes() { return (shm_min_batch_bytes() / 8) * 2 * 8; }

std::shared_ptr<arrow::RecordBatch> dictionary_batch() {
    arrow::StringDictionary32Builder builder;
    const int64_t rows = (shm_min_batch_bytes() / 4) * 2;  // 4-byte indices
    for (int64_t i = 0; i < rows; ++i) {
        REQUIRE(builder.Append(i % 2 == 0 ? "alpha" : "beta").ok());
    }
    auto array = unwrap(builder.Finish());
    auto schema = arrow::schema({arrow::field("label", array->type())});
    return arrow::RecordBatch::Make(schema, array->length(), {array});
}

}  // namespace

TEST_CASE("shm: a created segment attaches and reports the same size", "[shm]") {
    REQUIRE_SHM();
    const auto name = unique_name("attach");
    auto owner = ShmSegment::create(name, 1 << 20);
    REQUIRE(owner != nullptr);
    REQUIRE(owner->size() >= (1u << 20));

    auto peer = ShmSegment::attach(name, owner->size());
    REQUIRE(peer != nullptr);
    REQUIRE(peer->name() == name);
    REQUIRE(peer->size() == owner->size());
}

TEST_CASE("shm: attaching a segment that does not exist fails softly", "[shm]") {
    REQUIRE_SHM();
    // nullptr rather than an exception: a peer offering a channel we cannot
    // use is not an error, it just means staying on the pipe.
    REQUIRE(ShmSegment::attach(unique_name("absent"), 1 << 20) == nullptr);
}

TEST_CASE("shm: a batch round-trips through the segment", "[shm]") {
    REQUIRE_SHM();
    const auto name = unique_name("roundtrip");
    auto owner = ShmSegment::create(name, 4 << 20);
    REQUIRE(owner != nullptr);

    auto batch = big_int_batch();
    std::shared_ptr<arrow::KeyValueMetadata> md;
    auto pointer = maybe_write_to_shm(batch, &md, owner);

    // The pointer is a zero-row batch on the original schema, carrying the
    // offset and length and nothing else of the data.
    REQUIRE(pointer->num_rows() == 0);
    REQUIRE(pointer->schema()->Equals(*batch->schema()));
    REQUIRE(md != nullptr);
    REQUIRE(md->FindKey(keys::SHM_OFFSET) >= 0);
    REQUIRE(md->FindKey(keys::SHM_LENGTH) >= 0);
    REQUIRE(is_shm_pointer_batch(pointer, md));
    REQUIRE(owner->live_allocations() == 1);

    auto peer = ShmSegment::attach(name, owner->size());
    REQUIRE(peer != nullptr);
    int64_t free_offset = -1;
    auto resolved = resolve_shm_batch(pointer, &md, peer, &free_offset);

    REQUIRE(resolved->num_rows() == batch->num_rows());
    REQUIRE(resolved->Equals(*batch));
    // Pointer keys are stripped and provenance added, so a downstream reader
    // cannot mistake the resolved batch for another pointer.
    REQUIRE(md->FindKey(keys::SHM_OFFSET) < 0);
    REQUIRE(md->FindKey(keys::SHM_LENGTH) < 0);
    REQUIRE(md->FindKey(keys::SHM_SOURCE) >= 0);
    REQUIRE(free_offset >= static_cast<int64_t>(kShmHeaderSize));

    peer->free_alloc(free_offset);
    REQUIRE(owner->live_allocations() == 0);
}

TEST_CASE("shm: a dictionary-encoded batch round-trips", "[shm]") {
    REQUIRE_SHM();
    // Stored without its schema message, so the reader has to rebuild the
    // stream around the dictionary and record-batch messages. A null
    // out-parameter in that path once corrupted every enum-valued response.
    const auto name = unique_name("dict");
    auto owner = ShmSegment::create(name, 4 << 20);
    REQUIRE(owner != nullptr);

    auto batch = dictionary_batch();
    std::shared_ptr<arrow::KeyValueMetadata> md;
    auto pointer = maybe_write_to_shm(batch, &md, owner);
    REQUIRE(is_shm_pointer_batch(pointer, md));

    int64_t free_offset = -1;
    auto resolved = resolve_shm_batch(pointer, &md, owner, &free_offset);
    REQUIRE(resolved->num_rows() == batch->num_rows());
    REQUIRE(resolved->Equals(*batch));
}

TEST_CASE("shm: allocations are first-fit and free space coalesces", "[shm]") {
    REQUIRE_SHM();
    const auto name = unique_name("alloc");
    auto owner = ShmSegment::create(name, 8 << 20);
    REQUIRE(owner != nullptr);

    std::vector<int64_t> offsets;
    std::vector<std::shared_ptr<arrow::KeyValueMetadata>> mds;
    for (int i = 0; i < 3; ++i) {
        auto md = std::shared_ptr<arrow::KeyValueMetadata>();
        auto pointer = maybe_write_to_shm(big_int_batch(), &md, owner);
        REQUIRE(is_shm_pointer_batch(pointer, md));
        offsets.push_back(std::stoll(get_metadata_value(md, keys::SHM_OFFSET)));
        mds.push_back(md);
    }
    REQUIRE(owner->live_allocations() == 3);
    // Sorted by offset, and the first starts right after the header.
    REQUIRE(offsets[0] == static_cast<int64_t>(kShmHeaderSize));
    REQUIRE(offsets[0] < offsets[1]);
    REQUIRE(offsets[1] < offsets[2]);

    // Freeing the middle one reopens exactly that gap, and the next
    // allocation of the same size lands back in it — that is the whole of
    // "implicit coalescing": only occupied regions are tracked.
    owner->free_alloc(offsets[1]);
    REQUIRE(owner->live_allocations() == 2);

    std::shared_ptr<arrow::KeyValueMetadata> md;
    auto pointer = maybe_write_to_shm(big_int_batch(), &md, owner);
    REQUIRE(is_shm_pointer_batch(pointer, md));
    REQUIRE(std::stoll(get_metadata_value(md, keys::SHM_OFFSET)) == offsets[1]);
}

TEST_CASE("shm: a batch too large for the segment stays inline", "[shm]") {
    REQUIRE_SHM();
    // Not an error: a full allocator means the pipe carries this one.
    const auto name = unique_name("full");
    auto owner = ShmSegment::create(name, kShmHeaderSize + 4096);
    REQUIRE(owner != nullptr);

    auto batch = big_int_batch();
    REQUIRE(big_batch_bytes() > 4096);
    std::shared_ptr<arrow::KeyValueMetadata> md;
    auto result = maybe_write_to_shm(batch, &md, owner);
    REQUIRE(result.get() == batch.get());
    REQUIRE(md == nullptr);
    REQUIRE(owner->live_allocations() == 0);
}

TEST_CASE("shm: a batch below the threshold stays inline", "[shm]") {
    REQUIRE_SHM();
    const auto name = unique_name("small");
    auto owner = ShmSegment::create(name, 1 << 20);
    REQUIRE(owner != nullptr);

    auto batch = int_batch(1);  // far below shm_min_batch_bytes()
    std::shared_ptr<arrow::KeyValueMetadata> md;
    auto result = maybe_write_to_shm(batch, &md, owner);
    REQUIRE(result.get() == batch.get());
    REQUIRE(owner->live_allocations() == 0);
}

TEST_CASE("shm: a log batch is not mistaken for a pointer", "[shm]") {
    REQUIRE_SHM();
    // Both are zero-row; only the level key tells them apart, and treating a
    // log batch as a pointer would read garbage out of the data region.
    auto schema = arrow::schema({arrow::field("value", arrow::int64())});
    auto empty = make_empty_batch(schema);

    auto pointer_md = std::make_shared<arrow::KeyValueMetadata>();
    pointer_md->Append(keys::SHM_OFFSET, "65536");
    pointer_md->Append(keys::SHM_LENGTH, "128");
    REQUIRE(is_shm_pointer_batch(empty, pointer_md));

    auto log_md = std::make_shared<arrow::KeyValueMetadata>();
    log_md->Append(keys::SHM_OFFSET, "65536");
    log_md->Append(keys::SHM_LENGTH, "128");
    log_md->Append(keys::LOG_LEVEL, "INFO");
    REQUIRE_FALSE(is_shm_pointer_batch(empty, log_md));

    REQUIRE_FALSE(is_shm_pointer_batch(empty, nullptr));
}

TEST_CASE("shm: a pointer outside the segment is refused", "[shm]") {
    REQUIRE_SHM();
    // Rather than reading whatever happens to be mapped there.
    const auto name = unique_name("bounds");
    auto owner = ShmSegment::create(name, 1 << 20);
    REQUIRE(owner != nullptr);

    auto schema = arrow::schema({arrow::field("value", arrow::int64())});
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(keys::SHM_OFFSET, std::to_string(owner->size() - 8));
    md->Append(keys::SHM_LENGTH, "4096");

    int64_t free_offset = -1;
    REQUIRE_THROWS(resolve_shm_batch(make_empty_batch(schema), &md, owner, &free_offset));
}

TEST_CASE("shm: reset drops every allocation", "[shm]") {
    REQUIRE_SHM();
    const auto name = unique_name("reset");
    auto owner = ShmSegment::create(name, 1 << 20);
    REQUIRE(owner != nullptr);

    std::shared_ptr<arrow::KeyValueMetadata> md;
    maybe_write_to_shm(big_int_batch(), &md, owner);
    REQUIRE(owner->live_allocations() == 1);
    owner->reset();
    REQUIRE(owner->live_allocations() == 0);
}
