#include <catch2/catch_test_macros.hpp>

#include "vgi_rpc/request.h"
#include "vgi_rpc/metadata.h"

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

using namespace vgi_rpc;

namespace {

// Helper to create a single-row Request with one typed column
template <typename BuilderT, typename ValueT>
Request make_request(const std::string& col_name,
                     const std::shared_ptr<arrow::DataType>& type,
                     const ValueT& value) {
    BuilderT builder;
    REQUIRE(builder.Append(value).ok());
    auto arr = *builder.Finish();
    auto schema = arrow::schema({arrow::field(col_name, type)});
    auto batch = arrow::RecordBatch::Make(schema, 1, {arr});
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(keys::METHOD, "test");
    md->Append(keys::REQUEST_VERSION, "1");
    return Request(batch, md);
}

Request make_null_request(const std::string& col_name,
                          const std::shared_ptr<arrow::DataType>& type) {
    auto arr = arrow::MakeArrayOfNull(type, 1).ValueUnsafe();
    auto schema = arrow::schema({arrow::field(col_name, type, true)});
    auto batch = arrow::RecordBatch::Make(schema, 1, {arr});
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(keys::METHOD, "test");
    md->Append(keys::REQUEST_VERSION, "1");
    return Request(batch, md);
}

Request make_empty_request() {
    auto schema = empty_schema();
    auto batch = make_empty_batch(schema);
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(keys::METHOD, "test");
    md->Append(keys::REQUEST_VERSION, "1");
    return Request(batch, md);
}

}  // anonymous namespace

// ── get<T> ──────────────────────────────────────────────────────────

TEST_CASE("get<int64_t> returns correct value", "[request]") {
    auto req = make_request<arrow::Int64Builder>("x", arrow::int64(), int64_t{42});
    REQUIRE(req.get<int64_t>("x") == 42);
}

TEST_CASE("get<double> returns correct value", "[request]") {
    auto req = make_request<arrow::DoubleBuilder>("x", arrow::float64(), 3.14);
    REQUIRE(req.get<double>("x") == 3.14);
}

TEST_CASE("get<bool> returns correct value", "[request]") {
    auto req = make_request<arrow::BooleanBuilder>("x", arrow::boolean(), true);
    REQUIRE(req.get<bool>("x") == true);
}

TEST_CASE("get<string> returns correct value", "[request]") {
    auto req = make_request<arrow::StringBuilder>("x", arrow::utf8(), std::string("hello"));
    REQUIRE(req.get<std::string>("x") == "hello");
}

TEST_CASE("get<T> throws on missing column", "[request]") {
    auto req = make_request<arrow::Int64Builder>("x", arrow::int64(), int64_t{1});
    REQUIRE_THROWS_AS(req.get<int64_t>("nonexistent"), std::runtime_error);
}

TEST_CASE("get<T> throws on null value", "[request]") {
    auto req = make_null_request("x", arrow::int64());
    REQUIRE_THROWS_AS(req.get<int64_t>("x"), std::runtime_error);
}

TEST_CASE("get<T> throws on type mismatch", "[request]") {
    auto req = make_request<arrow::Int64Builder>("x", arrow::int64(), int64_t{1});
    REQUIRE_THROWS_AS(req.get<double>("x"), std::runtime_error);
}

// ── has_param ───────────────────────────────────────────────────────

TEST_CASE("has_param returns true for existing column", "[request]") {
    auto req = make_request<arrow::Int64Builder>("x", arrow::int64(), int64_t{1});
    REQUIRE(req.has_param("x"));
}

TEST_CASE("has_param returns false for missing column", "[request]") {
    auto req = make_request<arrow::Int64Builder>("x", arrow::int64(), int64_t{1});
    REQUIRE_FALSE(req.has_param("y"));
}

// ── get_column ──────────────────────────────────────────────────────

TEST_CASE("get_column returns array for existing column", "[request]") {
    auto req = make_request<arrow::Int64Builder>("x", arrow::int64(), int64_t{99});
    auto col = req.get_column("x");
    REQUIRE(col != nullptr);
    REQUIRE(col->length() == 1);
}

TEST_CASE("get_column returns nullptr for missing column", "[request]") {
    auto req = make_request<arrow::Int64Builder>("x", arrow::int64(), int64_t{99});
    REQUIRE(req.get_column("nope") == nullptr);
}

// ── get_optional<T> ─────────────────────────────────────────────────

TEST_CASE("get_optional<int64_t> returns value when present", "[request]") {
    auto req = make_request<arrow::Int64Builder>("x", arrow::int64(), int64_t{5});
    auto opt = req.get_optional<int64_t>("x");
    REQUIRE(opt.has_value());
    REQUIRE(*opt == 5);
}

TEST_CASE("get_optional<int64_t> returns nullopt on missing column", "[request]") {
    auto req = make_request<arrow::Int64Builder>("x", arrow::int64(), int64_t{5});
    REQUIRE_FALSE(req.get_optional<int64_t>("y").has_value());
}

TEST_CASE("get_optional<int64_t> returns nullopt on null value", "[request]") {
    auto req = make_null_request("x", arrow::int64());
    REQUIRE_FALSE(req.get_optional<int64_t>("x").has_value());
}

TEST_CASE("get_optional<string> returns value when present", "[request]") {
    auto req = make_request<arrow::StringBuilder>("x", arrow::utf8(), std::string("hi"));
    auto opt = req.get_optional<std::string>("x");
    REQUIRE(opt.has_value());
    REQUIRE(*opt == "hi");
}

TEST_CASE("get_optional<string> returns nullopt on null", "[request]") {
    auto req = make_null_request("x", arrow::utf8());
    REQUIRE_FALSE(req.get_optional<std::string>("x").has_value());
}

// ── list specializations ────────────────────────────────────────────

TEST_CASE("get<vector<string>> from list column", "[request]") {
    auto list_type = arrow::list(arrow::utf8());
    arrow::ListBuilder list_builder(arrow::default_memory_pool(),
                                    std::make_shared<arrow::StringBuilder>());
    auto& value_builder = dynamic_cast<arrow::StringBuilder&>(*list_builder.value_builder());

    REQUIRE(list_builder.Append().ok());
    REQUIRE(value_builder.Append("a").ok());
    REQUIRE(value_builder.Append("b").ok());
    REQUIRE(value_builder.Append("c").ok());
    auto arr = *list_builder.Finish();

    auto schema = arrow::schema({arrow::field("tags", list_type)});
    auto batch = arrow::RecordBatch::Make(schema, 1, {arr});
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(keys::METHOD, "test");
    md->Append(keys::REQUEST_VERSION, "1");
    Request req(batch, md);

    auto result = req.get<std::vector<std::string>>("tags");
    REQUIRE(result == std::vector<std::string>{"a", "b", "c"});
}

TEST_CASE("get<vector<int64_t>> from list column", "[request]") {
    auto list_type = arrow::list(arrow::int64());
    arrow::ListBuilder list_builder(arrow::default_memory_pool(),
                                    std::make_shared<arrow::Int64Builder>());
    auto& value_builder = dynamic_cast<arrow::Int64Builder&>(*list_builder.value_builder());

    REQUIRE(list_builder.Append().ok());
    REQUIRE(value_builder.Append(10).ok());
    REQUIRE(value_builder.Append(20).ok());
    auto arr = *list_builder.Finish();

    auto schema = arrow::schema({arrow::field("nums", list_type)});
    auto batch = arrow::RecordBatch::Make(schema, 1, {arr});
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(keys::METHOD, "test");
    md->Append(keys::REQUEST_VERSION, "1");
    Request req(batch, md);

    auto result = req.get<std::vector<int64_t>>("nums");
    REQUIRE(result == std::vector<int64_t>{10, 20});
}

TEST_CASE("get<vector<double>> from list column", "[request]") {
    auto list_type = arrow::list(arrow::float64());
    arrow::ListBuilder list_builder(arrow::default_memory_pool(),
                                    std::make_shared<arrow::DoubleBuilder>());
    auto& value_builder = dynamic_cast<arrow::DoubleBuilder&>(*list_builder.value_builder());

    REQUIRE(list_builder.Append().ok());
    REQUIRE(value_builder.Append(1.5).ok());
    REQUIRE(value_builder.Append(2.5).ok());
    auto arr = *list_builder.Finish();

    auto schema = arrow::schema({arrow::field("vals", list_type)});
    auto batch = arrow::RecordBatch::Make(schema, 1, {arr});
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(keys::METHOD, "test");
    md->Append(keys::REQUEST_VERSION, "1");
    Request req(batch, md);

    auto result = req.get<std::vector<double>>("vals");
    REQUIRE(result == std::vector<double>{1.5, 2.5});
}

// ── LargeListArray support ───────────────────────────────────────────

TEST_CASE("get<vector<string>> from large_list column", "[request]") {
    auto list_type = arrow::large_list(arrow::utf8());
    arrow::LargeListBuilder list_builder(arrow::default_memory_pool(),
                                         std::make_shared<arrow::StringBuilder>());
    auto& value_builder = dynamic_cast<arrow::StringBuilder&>(*list_builder.value_builder());

    REQUIRE(list_builder.Append().ok());
    REQUIRE(value_builder.Append("x").ok());
    REQUIRE(value_builder.Append("y").ok());
    auto arr = *list_builder.Finish();

    auto schema = arrow::schema({arrow::field("tags", list_type)});
    auto batch = arrow::RecordBatch::Make(schema, 1, {arr});
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(keys::METHOD, "test");
    md->Append(keys::REQUEST_VERSION, "1");
    Request req(batch, md);

    auto result = req.get<std::vector<std::string>>("tags");
    REQUIRE(result == std::vector<std::string>{"x", "y"});
}

TEST_CASE("get<vector<int64_t>> from large_list column", "[request]") {
    auto list_type = arrow::large_list(arrow::int64());
    arrow::LargeListBuilder list_builder(arrow::default_memory_pool(),
                                         std::make_shared<arrow::Int64Builder>());
    auto& value_builder = dynamic_cast<arrow::Int64Builder&>(*list_builder.value_builder());

    REQUIRE(list_builder.Append().ok());
    REQUIRE(value_builder.Append(100).ok());
    REQUIRE(value_builder.Append(200).ok());
    auto arr = *list_builder.Finish();

    auto schema = arrow::schema({arrow::field("nums", list_type)});
    auto batch = arrow::RecordBatch::Make(schema, 1, {arr});
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(keys::METHOD, "test");
    md->Append(keys::REQUEST_VERSION, "1");
    Request req(batch, md);

    auto result = req.get<std::vector<int64_t>>("nums");
    REQUIRE(result == std::vector<int64_t>{100, 200});
}

TEST_CASE("get<vector<double>> from large_list column", "[request]") {
    auto list_type = arrow::large_list(arrow::float64());
    arrow::LargeListBuilder list_builder(arrow::default_memory_pool(),
                                         std::make_shared<arrow::DoubleBuilder>());
    auto& value_builder = dynamic_cast<arrow::DoubleBuilder&>(*list_builder.value_builder());

    REQUIRE(list_builder.Append().ok());
    REQUIRE(value_builder.Append(9.5).ok());
    auto arr = *list_builder.Finish();

    auto schema = arrow::schema({arrow::field("vals", list_type)});
    auto batch = arrow::RecordBatch::Make(schema, 1, {arr});
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(keys::METHOD, "test");
    md->Append(keys::REQUEST_VERSION, "1");
    Request req(batch, md);

    auto result = req.get<std::vector<double>>("vals");
    REQUIRE(result == std::vector<double>{9.5});
}

// ── get_optional<vector<T>> ─────────────────────────────────────────

TEST_CASE("get_optional<vector<string>> returns value when present", "[request]") {
    auto list_type = arrow::list(arrow::utf8());
    arrow::ListBuilder list_builder(arrow::default_memory_pool(),
                                    std::make_shared<arrow::StringBuilder>());
    auto& vb = dynamic_cast<arrow::StringBuilder&>(*list_builder.value_builder());
    REQUIRE(list_builder.Append().ok());
    REQUIRE(vb.Append("a").ok());
    auto arr = *list_builder.Finish();

    auto schema = arrow::schema({arrow::field("tags", list_type)});
    auto batch = arrow::RecordBatch::Make(schema, 1, {arr});
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(keys::METHOD, "test");
    md->Append(keys::REQUEST_VERSION, "1");
    Request req(batch, md);

    auto opt = req.get_optional<std::vector<std::string>>("tags");
    REQUIRE(opt.has_value());
    REQUIRE(*opt == std::vector<std::string>{"a"});
}

TEST_CASE("get_optional<vector<string>> returns nullopt on missing column", "[request]") {
    auto req = make_empty_request();
    REQUIRE_FALSE(req.get_optional<std::vector<std::string>>("tags").has_value());
}

TEST_CASE("get_optional<vector<string>> returns nullopt on null", "[request]") {
    auto req = make_null_request("tags", arrow::list(arrow::utf8()));
    REQUIRE_FALSE(req.get_optional<std::vector<std::string>>("tags").has_value());
}

TEST_CASE("get_optional<vector<int64_t>> returns value when present", "[request]") {
    auto list_type = arrow::list(arrow::int64());
    arrow::ListBuilder list_builder(arrow::default_memory_pool(),
                                    std::make_shared<arrow::Int64Builder>());
    auto& vb = dynamic_cast<arrow::Int64Builder&>(*list_builder.value_builder());
    REQUIRE(list_builder.Append().ok());
    REQUIRE(vb.Append(7).ok());
    auto arr = *list_builder.Finish();

    auto schema = arrow::schema({arrow::field("nums", list_type)});
    auto batch = arrow::RecordBatch::Make(schema, 1, {arr});
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(keys::METHOD, "test");
    md->Append(keys::REQUEST_VERSION, "1");
    Request req(batch, md);

    auto opt = req.get_optional<std::vector<int64_t>>("nums");
    REQUIRE(opt.has_value());
    REQUIRE(*opt == std::vector<int64_t>{7});
}

TEST_CASE("get_optional<vector<int64_t>> returns nullopt on missing column", "[request]") {
    auto req = make_empty_request();
    REQUIRE_FALSE(req.get_optional<std::vector<int64_t>>("nums").has_value());
}

TEST_CASE("get_optional<vector<int64_t>> returns nullopt on null", "[request]") {
    auto req = make_null_request("nums", arrow::list(arrow::int64()));
    REQUIRE_FALSE(req.get_optional<std::vector<int64_t>>("nums").has_value());
}

TEST_CASE("get_optional<vector<double>> returns value when present", "[request]") {
    auto list_type = arrow::list(arrow::float64());
    arrow::ListBuilder list_builder(arrow::default_memory_pool(),
                                    std::make_shared<arrow::DoubleBuilder>());
    auto& vb = dynamic_cast<arrow::DoubleBuilder&>(*list_builder.value_builder());
    REQUIRE(list_builder.Append().ok());
    REQUIRE(vb.Append(2.5).ok());
    auto arr = *list_builder.Finish();

    auto schema = arrow::schema({arrow::field("vals", list_type)});
    auto batch = arrow::RecordBatch::Make(schema, 1, {arr});
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(keys::METHOD, "test");
    md->Append(keys::REQUEST_VERSION, "1");
    Request req(batch, md);

    auto opt = req.get_optional<std::vector<double>>("vals");
    REQUIRE(opt.has_value());
    REQUIRE(*opt == std::vector<double>{2.5});
}

TEST_CASE("get_optional<vector<double>> returns nullopt on missing column", "[request]") {
    auto req = make_empty_request();
    REQUIRE_FALSE(req.get_optional<std::vector<double>>("vals").has_value());
}

TEST_CASE("get_optional<vector<double>> returns nullopt on null", "[request]") {
    auto req = make_null_request("vals", arrow::list(arrow::float64()));
    REQUIRE_FALSE(req.get_optional<std::vector<double>>("vals").has_value());
}

// ── method_name / metadata ──────────────────────────────────────────

TEST_CASE("method_name and request_version from metadata", "[request]") {
    auto req = make_request<arrow::Int64Builder>("x", arrow::int64(), int64_t{1});
    REQUIRE(req.method_name() == "test");
    REQUIRE(req.request_version() == "1");
}
