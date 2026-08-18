// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include <catch2/catch_test_macros.hpp>

#include "vgi_rpc/wire.h"
#include "vgi_rpc/metadata.h"

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/io/memory.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

using namespace vgi_rpc;

// ── classify_batch ──────────────────────────────────────────────────

TEST_CASE("classify_batch: no metadata -> DATA", "[wire]") {
    auto batch = make_empty_batch(empty_schema());
    AnnotatedBatch ab{batch, nullptr};
    REQUIRE(ab.type() == BatchType::DATA);
}

TEST_CASE("classify_batch: batch with rows -> DATA", "[wire]") {
    arrow::Int64Builder b;
    REQUIRE(b.Append(42).ok());
    auto arr = *b.Finish();
    auto schema = arrow::schema({arrow::field("x", arrow::int64())});
    auto batch = arrow::RecordBatch::Make(schema, 1, {arr});

    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(keys::LOG_LEVEL, "INFO");
    md->Append(keys::LOG_MESSAGE, "hello");
    AnnotatedBatch ab{batch, md};
    REQUIRE(classify_batch(ab) == BatchType::DATA);
}

TEST_CASE("classify_batch: log metadata -> LOG", "[wire]") {
    auto batch = make_empty_batch(empty_schema());
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(keys::LOG_LEVEL, "INFO");
    md->Append(keys::LOG_MESSAGE, "hello");
    AnnotatedBatch ab{batch, md};
    REQUIRE(classify_batch(ab) == BatchType::LOG);
}

TEST_CASE("classify_batch: EXCEPTION level -> ERROR", "[wire]") {
    auto batch = make_empty_batch(empty_schema());
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(keys::LOG_LEVEL, "EXCEPTION");
    md->Append(keys::LOG_MESSAGE, "boom");
    AnnotatedBatch ab{batch, md};
    REQUIRE(classify_batch(ab) == BatchType::EXCEPTION);
}

TEST_CASE("classify_batch: location -> EXTERNAL_POINTER", "[wire]") {
    auto batch = make_empty_batch(empty_schema());
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(keys::LOCATION, "s3://bucket/key");
    AnnotatedBatch ab{batch, md};
    REQUIRE(classify_batch(ab) == BatchType::EXTERNAL_POINTER);
}

TEST_CASE("classify_batch: shm_offset -> SHM_POINTER", "[wire]") {
    auto batch = make_empty_batch(empty_schema());
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(keys::SHM_OFFSET, "0");
    AnnotatedBatch ab{batch, md};
    REQUIRE(classify_batch(ab) == BatchType::SHM_POINTER);
}

TEST_CASE("classify_batch: stream_state -> STATE_TOKEN", "[wire]") {
    auto batch = make_empty_batch(empty_schema());
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(keys::STREAM_STATE, "open");
    AnnotatedBatch ab{batch, md};
    REQUIRE(classify_batch(ab) == BatchType::STATE_TOKEN);
}

TEST_CASE("classify_batch: empty metadata, zero rows -> DATA (void)", "[wire]") {
    auto batch = make_empty_batch(empty_schema());
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    AnnotatedBatch ab{batch, md};
    REQUIRE(classify_batch(ab) == BatchType::DATA);
}

// ── IPC round-trip ──────────────────────────────────────────────────

TEST_CASE("read/write_ipc_stream round-trip", "[wire]") {
    auto schema = arrow::schema({arrow::field("x", arrow::int64())});

    arrow::Int64Builder b;
    REQUIRE(b.Append(7).ok());
    auto arr = *b.Finish();
    auto batch = arrow::RecordBatch::Make(schema, 1, {arr});

    AnnotatedBatch ab = AnnotatedBatch::data(batch);

    // Write to buffer
    auto sink = arrow::io::BufferOutputStream::Create().ValueUnsafe();
    write_ipc_stream(sink, schema, {ab});
    auto buf = sink->Finish().ValueUnsafe();

    // Read back
    auto source = std::make_shared<arrow::io::BufferReader>(buf);
    auto result = read_ipc_stream(source);
    REQUIRE(result.has_value());
    REQUIRE(result->batches.size() == 1);
    REQUIRE(result->batches[0].batch->num_rows() == 1);
    REQUIRE(result->schema->Equals(*schema));
}

TEST_CASE("read_ipc_stream returns nullopt on empty input", "[wire]") {
    auto empty_buf = std::make_shared<arrow::Buffer>("");
    auto source = std::make_shared<arrow::io::BufferReader>(empty_buf);
    auto result = read_ipc_stream(source);
    REQUIRE_FALSE(result.has_value());
}

TEST_CASE("IPC round-trip preserves custom metadata", "[wire]") {
    auto schema = arrow::schema({arrow::field("v", arrow::utf8())});

    arrow::StringBuilder sb;
    REQUIRE(sb.Append("hello").ok());
    auto arr = *sb.Finish();
    auto batch = arrow::RecordBatch::Make(schema, 1, {arr});

    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append("custom_key", "custom_value");
    AnnotatedBatch ab = AnnotatedBatch::with_metadata(batch, md);

    auto sink = arrow::io::BufferOutputStream::Create().ValueUnsafe();
    write_ipc_stream(sink, schema, {ab});
    auto buf = sink->Finish().ValueUnsafe();

    auto source = std::make_shared<arrow::io::BufferReader>(buf);
    auto result = read_ipc_stream(source);
    REQUIRE(result.has_value());
    REQUIRE(result->batches.size() == 1);
    REQUIRE(result->batches[0].custom_metadata != nullptr);
    auto key_idx = result->batches[0].custom_metadata->FindKey("custom_key");
    REQUIRE(key_idx >= 0);
    REQUIRE(result->batches[0].custom_metadata->value(key_idx) == "custom_value");
}

TEST_CASE("read_ipc_stream throws on corrupt/garbage data", "[wire]") {
    // Garbage bytes that are not a valid IPC stream
    std::string garbage = "this is not a valid IPC stream at all!!!!";
    auto buf = std::make_shared<arrow::Buffer>(garbage);
    auto source = std::make_shared<arrow::io::BufferReader>(buf);
    REQUIRE_THROWS_AS(read_ipc_stream(source), std::runtime_error);
}

// ── AnnotatedBatch convenience ──────────────────────────────────────

TEST_CASE("AnnotatedBatch::data sets null metadata", "[wire]") {
    auto batch = make_empty_batch(empty_schema());
    auto ab = AnnotatedBatch::data(batch);
    REQUIRE(ab.batch == batch);
    REQUIRE(ab.custom_metadata == nullptr);
}

TEST_CASE("AnnotatedBatch::with_metadata preserves both", "[wire]") {
    auto batch = make_empty_batch(empty_schema());
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append("k", "v");
    auto ab = AnnotatedBatch::with_metadata(batch, md);
    REQUIRE(ab.batch == batch);
    REQUIRE(ab.custom_metadata == md);
}
