// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include <catch2/catch_test_macros.hpp>

#include "vgi_rpc/metadata.h"

#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

using namespace vgi_rpc;

TEST_CASE("empty_schema returns a schema with no fields", "[metadata]") {
    auto s = empty_schema();
    REQUIRE(s->num_fields() == 0);
    // Same pointer each call (singleton).
    REQUIRE(s.get() == empty_schema().get());
}

TEST_CASE("random_hex produces correct length", "[metadata]") {
    auto h = random_hex(16);
    REQUIRE(h.size() == 16);
    for (char c : h) {
        REQUIRE(((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')));
    }
}

TEST_CASE("random_hex produces distinct values", "[metadata]") {
    auto a = random_hex(32);
    auto b = random_hex(32);
    REQUIRE(a != b);
}

TEST_CASE("get_metadata_value returns value when present", "[metadata]") {
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append("key1", "value1");
    REQUIRE(get_metadata_value(md, "key1") == "value1");
}

TEST_CASE("get_metadata_value returns default when missing", "[metadata]") {
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    REQUIRE(get_metadata_value(md, "missing") == "");
    REQUIRE(get_metadata_value(md, "missing", "fallback") == "fallback");
}

TEST_CASE("get_metadata_value handles nullptr", "[metadata]") {
    REQUIRE(get_metadata_value(nullptr, "key") == "");
    REQUIRE(get_metadata_value(nullptr, "key", "x") == "x");
}

TEST_CASE("make_empty_batch on empty schema", "[metadata]") {
    auto batch = make_empty_batch(empty_schema());
    REQUIRE(batch->num_rows() == 0);
    REQUIRE(batch->num_columns() == 0);
}

TEST_CASE("make_empty_batch on non-trivial schema", "[metadata]") {
    auto schema = arrow::schema({
        arrow::field("x", arrow::int64()),
        arrow::field("y", arrow::utf8()),
    });
    auto batch = make_empty_batch(schema);
    REQUIRE(batch->num_rows() == 0);
    REQUIRE(batch->num_columns() == 2);
    REQUIRE(batch->schema()->Equals(*schema));
}
