#include <catch2/catch_test_macros.hpp>

#include "vgi_rpc/result.h"
#include "vgi_rpc/metadata.h"
#include "vgi_rpc/wire.h"

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

using namespace vgi_rpc;

TEST_CASE("Result::value from RecordBatch", "[result]") {
    auto schema = arrow::schema({arrow::field("x", arrow::int64())});
    arrow::Int64Builder b;
    REQUIRE(b.Append(42).ok());
    auto arr = *b.Finish();
    auto batch = arrow::RecordBatch::Make(schema, 1, {arr});

    auto result = Result::value(batch);
    REQUIRE(result.annotated_batch().batch->num_rows() == 1);
    REQUIRE(result.annotated_batch().custom_metadata == nullptr);
    REQUIRE(result.schema()->Equals(*schema));
}

TEST_CASE("Result::value from schema + arrays", "[result]") {
    auto schema = arrow::schema({arrow::field("y", arrow::float64())});
    arrow::DoubleBuilder b;
    REQUIRE(b.Append(3.14).ok());
    auto arr = *b.Finish();

    auto result = Result::value(schema, {arr});
    REQUIRE(result.annotated_batch().batch->num_rows() == 1);
}

TEST_CASE("Result::void_result produces zero-row empty-schema batch", "[result]") {
    auto result = Result::void_result();
    auto& ab = result.annotated_batch();
    REQUIRE(ab.batch->num_rows() == 0);
    REQUIRE(ab.batch->num_columns() == 0);
    REQUIRE(ab.custom_metadata == nullptr);
    REQUIRE(ab.type() == BatchType::DATA);
}

TEST_CASE("Result::error produces zero-row batch with EXCEPTION metadata", "[result]") {
    auto schema = arrow::schema({arrow::field("x", arrow::int64())});
    auto result = Result::error(schema, "ValueError", "bad input", "srv", "req");
    auto& ab = result.annotated_batch();
    REQUIRE(ab.batch->num_rows() == 0);
    REQUIRE(ab.batch->num_columns() == 1);
    REQUIRE(ab.type() == BatchType::ERROR);
    REQUIRE(ab.custom_metadata != nullptr);

    auto idx = ab.custom_metadata->FindKey(keys::LOG_LEVEL);
    REQUIRE(idx >= 0);
    REQUIRE(ab.custom_metadata->value(idx) == "EXCEPTION");
}

TEST_CASE("Result::from_annotated_batch wraps correctly", "[result]") {
    auto batch = make_empty_batch(empty_schema());
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append("k", "v");
    AnnotatedBatch ab = AnnotatedBatch::with_metadata(batch, md);

    auto result = Result::from_annotated_batch(std::move(ab));
    REQUIRE(result.annotated_batch().custom_metadata != nullptr);
}
