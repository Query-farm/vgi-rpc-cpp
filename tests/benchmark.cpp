// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/// Benchmarks matching the Python vgi-rpc test_benchmarks.py for comparison.
/// Run: ./build/tests/vgi_rpc_benchmark
///
/// Comparable Python benchmarks (from docs/benchmarks.md):
///   Serialize 10K-row batch:   33 us
///   Deserialize 10K-row batch: 60 us
///   Serialize 100K-row batch:  0.67 ms
///   Deserialize 100K-row batch: 0.59 ms
#include <catch2/catch_test_macros.hpp>
#include <catch2/benchmark/catch_benchmark.hpp>

#include "vgi_rpc/wire.h"
#include "vgi_rpc/arrow_utils.h"
#include "vgi_rpc/metadata.h"

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

using namespace vgi_rpc;

namespace {

// Schema and data matching Python's test_benchmarks.py:
//   pa.RecordBatch.from_pydict({
//       "id": list(range(n)),
//       "value": [float(i) * 1.1 for i in range(n)],
//       "label": [f"item_{i}" for i in range(n)],
//   })
std::shared_ptr<arrow::RecordBatch> make_bench_batch(int64_t num_rows) {
    arrow::Int64Builder id_builder;
    arrow::DoubleBuilder value_builder;
    arrow::StringBuilder label_builder;

    (void)id_builder.Reserve(num_rows);
    (void)value_builder.Reserve(num_rows);
    (void)label_builder.Reserve(num_rows);

    for (int64_t i = 0; i < num_rows; ++i) {
        (void)id_builder.Append(i);
        (void)value_builder.Append(static_cast<double>(i) * 1.1);
        (void)label_builder.Append("item_" + std::to_string(i));
    }

    auto schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("value", arrow::float64()),
        arrow::field("label", arrow::utf8()),
    });

    return arrow::RecordBatch::Make(schema, num_rows, {
        *id_builder.Finish(),
        *value_builder.Finish(),
        *label_builder.Finish(),
    });
}

// Pre-serialize a batch to a buffer for deserialization benchmarks
std::shared_ptr<arrow::Buffer> serialize_to_buffer(
    const std::shared_ptr<arrow::Schema>& schema,
    const std::vector<AnnotatedBatch>& batches) {
    auto sink = arrow::io::BufferOutputStream::Create().ValueUnsafe();
    write_ipc_stream(sink, schema, batches);
    return sink->Finish().ValueUnsafe();
}

}  // namespace

// ---------------------------------------------------------------------------
// Group 1: Serialization — matches Python TestSerializationBenchmarks
// ---------------------------------------------------------------------------

TEST_CASE("Serialization", "[benchmark][serialization]") {
    // -- 10K-row batch (matches Python test_serialize_large_batch) --
    auto batch_10k = make_bench_batch(10'000);
    auto schema_10k = batch_10k->schema();
    AnnotatedBatch ab_10k = AnnotatedBatch::data(batch_10k);

    BENCHMARK("serialize 10K-row batch") {
        auto sink = arrow::io::BufferOutputStream::Create().ValueUnsafe();
        write_ipc_stream(sink, schema_10k, {ab_10k});
        return sink->Finish().ValueUnsafe();
    };

    // -- 10K-row deserialization (matches Python test_deserialize_large_batch) --
    auto buf_10k = serialize_to_buffer(schema_10k, {ab_10k});

    BENCHMARK("deserialize 10K-row batch") {
        auto source = std::make_shared<arrow::io::BufferReader>(buf_10k);
        return read_ipc_stream(source);
    };

    // -- 100K-row batch (matches Python test_large_batch_serialize_memory) --
    auto batch_100k = make_bench_batch(100'000);
    auto schema_100k = batch_100k->schema();
    AnnotatedBatch ab_100k = AnnotatedBatch::data(batch_100k);

    BENCHMARK("serialize 100K-row batch") {
        auto sink = arrow::io::BufferOutputStream::Create().ValueUnsafe();
        write_ipc_stream(sink, schema_100k, {ab_100k});
        return sink->Finish().ValueUnsafe();
    };

    // -- 100K-row deserialization (matches Python test_large_batch_deserialize_memory) --
    auto buf_100k = serialize_to_buffer(schema_100k, {ab_100k});

    BENCHMARK("deserialize 100K-row batch") {
        auto source = std::make_shared<arrow::io::BufferReader>(buf_100k);
        return read_ipc_stream(source);
    };
}

// ---------------------------------------------------------------------------
// Group 2: Round-trip (serialize + deserialize combined)
// ---------------------------------------------------------------------------

TEST_CASE("Round-trip", "[benchmark][roundtrip]") {
    auto batch_1 = make_bench_batch(1);
    auto schema_1 = batch_1->schema();
    AnnotatedBatch ab_1 = AnnotatedBatch::data(batch_1);

    BENCHMARK("1-row round-trip") {
        auto sink = arrow::io::BufferOutputStream::Create().ValueUnsafe();
        write_ipc_stream(sink, schema_1, {ab_1});
        auto buf = sink->Finish().ValueUnsafe();
        auto source = std::make_shared<arrow::io::BufferReader>(buf);
        return read_ipc_stream(source);
    };

    auto batch_100 = make_bench_batch(100);
    auto schema_100 = batch_100->schema();
    AnnotatedBatch ab_100 = AnnotatedBatch::data(batch_100);

    BENCHMARK("100-row round-trip") {
        auto sink = arrow::io::BufferOutputStream::Create().ValueUnsafe();
        write_ipc_stream(sink, schema_100, {ab_100});
        auto buf = sink->Finish().ValueUnsafe();
        auto source = std::make_shared<arrow::io::BufferReader>(buf);
        return read_ipc_stream(source);
    };

    auto batch_10k = make_bench_batch(10'000);
    auto schema_10k = batch_10k->schema();
    AnnotatedBatch ab_10k = AnnotatedBatch::data(batch_10k);

    BENCHMARK("10K-row round-trip") {
        auto sink = arrow::io::BufferOutputStream::Create().ValueUnsafe();
        write_ipc_stream(sink, schema_10k, {ab_10k});
        auto buf = sink->Finish().ValueUnsafe();
        auto source = std::make_shared<arrow::io::BufferReader>(buf);
        return read_ipc_stream(source);
    };

    // With custom metadata
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(keys::SERVER_ID, "bench-server");
    md->Append(keys::REQUEST_ID, "bench-request-001");
    AnnotatedBatch ab_md = AnnotatedBatch::with_metadata(batch_100, md);

    BENCHMARK("100-row round-trip with metadata") {
        auto sink = arrow::io::BufferOutputStream::Create().ValueUnsafe();
        write_ipc_stream(sink, schema_100, {ab_md});
        auto buf = sink->Finish().ValueUnsafe();
        auto source = std::make_shared<arrow::io::BufferReader>(buf);
        return read_ipc_stream(source);
    };
}

// ---------------------------------------------------------------------------
// Group 3: Batch classification throughput
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Group 4: Serialization breakdown — isolate where time goes
// ---------------------------------------------------------------------------

TEST_CASE("Serialize breakdown (10K rows)", "[benchmark][breakdown]") {
    auto batch = make_bench_batch(10'000);
    auto schema = batch->schema();
    AnnotatedBatch ab = AnnotatedBatch::data(batch);

    // Baseline: full write_ipc_stream (same as Group 1)
    BENCHMARK("baseline: write_ipc_stream") {
        auto sink = arrow::io::BufferOutputStream::Create().ValueUnsafe();
        write_ipc_stream(sink, schema, {ab});
        return sink->Finish().ValueUnsafe();
    };

    // Step 1: Just allocate the buffer output stream
    BENCHMARK("step: BufferOutputStream::Create") {
        auto sink = arrow::io::BufferOutputStream::Create().ValueUnsafe();
        return sink;
    };

    // Step 2: Create + make writer (no batch write)
    BENCHMARK("step: Create + MakeStreamWriter") {
        auto sink = arrow::io::BufferOutputStream::Create().ValueUnsafe();
        auto writer = unwrap(arrow::ipc::MakeStreamWriter(sink, schema));
        return writer;
    };

    // Step 3: Create + writer + write batch (no close)
    BENCHMARK("step: Create + writer + WriteBatch") {
        auto sink = arrow::io::BufferOutputStream::Create().ValueUnsafe();
        auto writer = unwrap(arrow::ipc::MakeStreamWriter(sink, schema));
        VGI_RPC_THROW_NOT_OK(writer->WriteRecordBatch(*ab.batch));
        return writer;
    };

    // Step 4: Create + writer + write + close (no Finish)
    BENCHMARK("step: Create + writer + Write + Close") {
        auto sink = arrow::io::BufferOutputStream::Create().ValueUnsafe();
        auto writer = unwrap(arrow::ipc::MakeStreamWriter(sink, schema));
        VGI_RPC_THROW_NOT_OK(writer->WriteRecordBatch(*ab.batch));
        VGI_RPC_THROW_NOT_OK(writer->Close());
        return sink;
    };

    // Alternative: pre-allocate buffer to avoid resizing
    // Measure serialized size first
    {
        auto probe = arrow::io::BufferOutputStream::Create().ValueUnsafe();
        write_ipc_stream(probe, schema, {ab});
        auto probe_buf = probe->Finish().ValueUnsafe();
        auto prealloc_size = probe_buf->size();
        INFO("Serialized size: " << prealloc_size << " bytes");

        BENCHMARK("alt: pre-allocated buffer") {
            auto sink = arrow::io::BufferOutputStream::Create(prealloc_size).ValueUnsafe();
            write_ipc_stream(sink, schema, {ab});
            return sink->Finish().ValueUnsafe();
        };
    }

    // Alternative: reuse a MockOutputStream that counts bytes but doesn't alloc
    // This isolates Arrow's serialization work from buffer management
    struct NullOutputStream : public arrow::io::OutputStream {
        int64_t pos = 0;
        arrow::Status Close() override { return arrow::Status::OK(); }
        bool closed() const override { return false; }
        arrow::Result<int64_t> Tell() const override { return pos; }
        arrow::Status Write(const void*, int64_t nbytes) override {
            pos += nbytes;
            return arrow::Status::OK();
        }
    };

    BENCHMARK("alt: NullOutputStream (no alloc)") {
        auto sink = std::make_shared<NullOutputStream>();
        auto writer = unwrap(arrow::ipc::MakeStreamWriter(sink, schema));
        VGI_RPC_THROW_NOT_OK(writer->WriteRecordBatch(*ab.batch));
        VGI_RPC_THROW_NOT_OK(writer->Close());
        return sink->pos;
    };
}

TEST_CASE("Batch classification", "[benchmark][classify]") {
    auto batch = make_empty_batch(empty_schema());

    AnnotatedBatch data_ab{batch, nullptr};
    BENCHMARK("classify DATA (null metadata)") {
        return classify_batch(data_ab);
    };

    auto log_md = std::make_shared<arrow::KeyValueMetadata>();
    log_md->Append(keys::LOG_LEVEL, "INFO");
    log_md->Append(keys::LOG_MESSAGE, "benchmark log");
    AnnotatedBatch log_ab{batch, log_md};
    BENCHMARK("classify LOG") {
        return classify_batch(log_ab);
    };

    auto err_md = std::make_shared<arrow::KeyValueMetadata>();
    err_md->Append(keys::LOG_LEVEL, "EXCEPTION");
    err_md->Append(keys::LOG_MESSAGE, "benchmark error");
    AnnotatedBatch err_ab{batch, err_md};
    BENCHMARK("classify ERROR") {
        return classify_batch(err_ab);
    };

    auto loc_md = std::make_shared<arrow::KeyValueMetadata>();
    loc_md->Append(keys::LOCATION, "s3://bucket/key");
    AnnotatedBatch loc_ab{batch, loc_md};
    BENCHMARK("classify EXTERNAL_POINTER") {
        return classify_batch(loc_ab);
    };
}
