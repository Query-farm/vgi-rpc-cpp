#include <catch2/catch_test_macros.hpp>

#include "vgi_rpc/output_collector.h"
#include "vgi_rpc/metadata.h"
#include "vgi_rpc/wire.h"

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

using namespace vgi_rpc;

static auto test_schema() {
    static auto s = arrow::schema({arrow::field("x", arrow::int64())});
    return s;
}

TEST_CASE("emit_batch stores a single data batch", "[output_collector]") {
    OutputCollector out(test_schema(), true);

    arrow::Int64Builder b;
    REQUIRE(b.Append(1).ok());
    auto arr = *b.Finish();
    auto batch = arrow::RecordBatch::Make(test_schema(), 1, {arr});

    out.emit_batch(batch);
    REQUIRE(out.batches().size() == 1);
    REQUIRE(out.batches()[0].batch->num_rows() == 1);
    REQUIRE(out.batches()[0].custom_metadata == nullptr);
}

TEST_CASE("emit_batch throws on second data batch", "[output_collector]") {
    OutputCollector out(test_schema(), true);

    arrow::Int64Builder b1;
    REQUIRE(b1.Append(1).ok());
    auto arr1 = *b1.Finish();
    auto batch1 = arrow::RecordBatch::Make(test_schema(), 1, {arr1});
    out.emit_batch(batch1);

    arrow::Int64Builder b2;
    REQUIRE(b2.Append(2).ok());
    auto arr2 = *b2.Finish();
    auto batch2 = arrow::RecordBatch::Make(test_schema(), 1, {arr2});
    REQUIRE_THROWS_AS(out.emit_batch(batch2), std::runtime_error);
}

TEST_CASE("emit_arrays builds batch from arrays", "[output_collector]") {
    OutputCollector out(test_schema(), true);

    arrow::Int64Builder b;
    REQUIRE(b.Append(99).ok());
    auto arr = *b.Finish();

    out.emit_arrays({arr});
    REQUIRE(out.batches().size() == 1);
    REQUIRE(out.batches()[0].batch->num_rows() == 1);
}

TEST_CASE("client_log emits zero-row batch with metadata", "[output_collector]") {
    OutputCollector out(test_schema(), true, "srv");
    out.client_log(LogLevel::INFO, "hello");

    REQUIRE(out.batches().size() == 1);
    auto& ab = out.batches()[0];
    REQUIRE(ab.batch->num_rows() == 0);
    REQUIRE(ab.custom_metadata != nullptr);
    REQUIRE(classify_batch(ab) == BatchType::LOG);
}

TEST_CASE("finish sets is_finished for producer mode", "[output_collector]") {
    OutputCollector out(test_schema(), true);
    REQUIRE_FALSE(out.is_finished());
    out.finish();
    REQUIRE(out.is_finished());
}

TEST_CASE("finish throws for exchange mode", "[output_collector]") {
    OutputCollector out(test_schema(), false);
    REQUIRE_THROWS_AS(out.finish(), std::runtime_error);
}

TEST_CASE("client_log includes request_id when provided", "[output_collector]") {
    OutputCollector out(test_schema(), true, "srv", "req123");
    out.client_log(LogLevel::INFO, "hello");

    REQUIRE(out.batches().size() == 1);
    auto& ab = out.batches()[0];
    REQUIRE(ab.custom_metadata != nullptr);
    auto idx = ab.custom_metadata->FindKey(keys::REQUEST_ID);
    REQUIRE(idx >= 0);
    REQUIRE(ab.custom_metadata->value(idx) == "req123");
}

TEST_CASE("emit_batch throws on schema mismatch", "[output_collector]") {
    OutputCollector out(test_schema(), true);

    auto wrong_schema = arrow::schema({arrow::field("y", arrow::float64())});
    arrow::DoubleBuilder db;
    REQUIRE(db.Append(1.0).ok());
    auto arr = *db.Finish();
    auto batch = arrow::RecordBatch::Make(wrong_schema, 1, {arr});

    REQUIRE_THROWS_AS(out.emit_batch(batch), std::runtime_error);
}
