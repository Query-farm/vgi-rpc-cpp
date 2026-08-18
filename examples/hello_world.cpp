// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include <vgi_rpc/server.h>
#include <vgi_rpc/request.h>
#include <vgi_rpc/result.h>
#include <vgi_rpc/arrow_utils.h>

#include <arrow/array/builder_primitive.h>
#include <arrow/type.h>

int main() {
    auto params_schema = arrow::schema({
        arrow::field("a", arrow::float64()),
        arrow::field("b", arrow::float64()),
    });

    auto result_schema = arrow::schema({
        arrow::field("result", arrow::float64()),
    });

    auto server = vgi_rpc::ServerBuilder()
        .add_unary("add", params_schema, result_schema,
            [](const vgi_rpc::Request& req, vgi_rpc::CallContext& /*ctx*/) {
                double a = req.get<double>("a");
                double b = req.get<double>("b");
                double sum = a + b;

                arrow::DoubleBuilder builder;
                VGI_RPC_THROW_NOT_OK(builder.Append(sum));
                auto array = vgi_rpc::unwrap(builder.Finish());

                return vgi_rpc::Result::value(
                    arrow::schema({arrow::field("result", arrow::float64())}),
                    {array});
            },
            "Add two numbers together.")
        .enable_describe("HelloWorld")
        .build();

    server->run();
    return 0;
}
