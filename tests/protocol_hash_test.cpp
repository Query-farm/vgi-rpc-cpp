// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/protocol_hash.h"

#include "vgi_rpc/type_tokens.h"

#include <arrow/api.h>
#include <catch2/catch_test_macros.hpp>

using namespace vgi_rpc;

namespace {

std::shared_ptr<arrow::Schema> Utf8Schema(const std::string& name) {
  return arrow::schema({arrow::field(name, arrow::utf8(), /*nullable=*/false)});
}

}  // namespace

// The cross-port contract. This digest is produced by the Python reference
// (vgi_rpc/rpc/_protocol_hash.py); a mismatch means this port and that one would
// disagree about whether they speak the same protocol.
//
// A failure is a JSON diff, not a guess: CanonicalDescription returns the exact
// preimage, so print it and compare.
TEST_CASE("protocol hash matches the Python reference digest") {
  std::vector<HashMethod> methods;
  methods.push_back(HashMethod{"echo", "unary", /*has_return=*/true, /*has_header=*/false,
                               Utf8Schema("value"), Utf8Schema("result"), nullptr});
  auto hash = ComputeProtocolHash("demo.Hash.v1", methods);
  REQUIRE(hash.ok());
  CHECK(*hash == "a4b8ae57bf777c906081ff3610d435836b77dbc1f17a381c6a2febf1a2adb115");
}

// Absent and empty are different: a method returning nothing is not a method
// returning an empty struct, and they must not hash alike.
TEST_CASE("protocol hash omits result for a method returning nothing") {
  std::vector<HashMethod> methods;
  methods.push_back(HashMethod{"fire", "unary", /*has_return=*/false, /*has_header=*/false,
                               Utf8Schema("v"), nullptr, nullptr});
  auto desc = CanonicalDescription("demo.Void.v1", methods);
  REQUIRE(desc.ok());
  CHECK(*desc ==
        R"({"methods":[{"has_header":false,"has_return":false,"name":"fire",)"
        R"("params":[{"name":"v","nullable":false,"type":"utf8"}],"type":"unary"}],)"
        R"("protocol":"demo.Void.v1"})");
}

// A port iterating a hash map must still produce this order.
TEST_CASE("protocol hash sorts methods by name") {
  auto mk = [](const std::string& name) {
    return HashMethod{name, "unary", false, false, arrow::schema({}), nullptr, nullptr};
  };
  auto ab = ComputeProtocolHash("p", {mk("a"), mk("b")});
  auto ba = ComputeProtocolHash("p", {mk("b"), mk("a")});
  REQUIRE(ab.ok());
  REQUIRE(ba.ok());
  CHECK(*ab == *ba);
}

// Arrow ignores a list child's *name*, so the token must too -- otherwise two
// ports that default differently hash the same protocol differently.
// Nullability it does not ignore, so the token keeps that.
TEST_CASE("list child name is normalised but nullability is kept") {
  auto named = arrow::field(
      "col", arrow::list(arrow::field("element", arrow::int64(), /*nullable=*/true)), false);
  auto named_token = TypeToken(*named);
  REQUIRE(named_token.ok());
  CHECK(*named_token == "list<item?:int64>");

  auto non_null = arrow::field(
      "col", arrow::list(arrow::field("item", arrow::int64(), /*nullable=*/false)), false);
  auto non_null_token = TypeToken(*non_null);
  REQUIRE(non_null_token.ok());
  CHECK(*non_null_token == "list<item:int64>");
}

// An unrecognised type is an error, never a fallback to ToString(): a silent
// fallback would be a one-sided hash divergence surfacing as an unexplained
// mismatch at somebody else's client.
TEST_CASE("an unsupported type is an error rather than a guess") {
  auto rle = arrow::field("c", arrow::run_end_encoded(arrow::int32(), arrow::utf8()), false);
  CHECK_FALSE(TypeToken(*rle).ok());
}
