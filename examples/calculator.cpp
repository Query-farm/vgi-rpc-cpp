// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include <vgi_rpc/server.h>
#include <vgi_rpc/request.h>
#include <vgi_rpc/result.h>
#include <vgi_rpc/call_context.h>
#include <vgi_rpc/log.h>
#include <vgi_rpc/arrow_utils.h>

#include <arrow/array/builder_primitive.h>
#include <arrow/type.h>

#include <stdexcept>

namespace {

auto params_ab() {
    return arrow::schema({
        arrow::field("a", arrow::float64()),
        arrow::field("b", arrow::float64()),
    });
}

auto result_double() {
    return arrow::schema({
        arrow::field("result", arrow::float64()),
    });
}

vgi_rpc::Result make_double_result(double value) {
    arrow::DoubleBuilder builder;
    VGI_RPC_THROW_NOT_OK(builder.Append(value));
    auto array = vgi_rpc::unwrap(builder.Finish());
    return vgi_rpc::Result::value(result_double(), {array});
}

}  // anonymous namespace

int main() {
    auto server = vgi_rpc::ServerBuilder()
        .add_unary("add", params_ab(), result_double(),
            [](const vgi_rpc::Request& req, vgi_rpc::CallContext& ctx) {
                double a = req.get<double>("a");
                double b = req.get<double>("b");
                ctx.client_log(vgi_rpc::LogLevel::DEBUG,
                               "Computing " + std::to_string(a) + " + " + std::to_string(b));
                return make_double_result(a + b);
            },
            "Add two numbers.")
        .add_unary("multiply", params_ab(), result_double(),
            [](const vgi_rpc::Request& req, vgi_rpc::CallContext& ctx) {
                double a = req.get<double>("a");
                double b = req.get<double>("b");
                return make_double_result(a * b);
            },
            "Multiply two numbers.")
        .add_unary("divide", params_ab(), result_double(),
            [](const vgi_rpc::Request& req, vgi_rpc::CallContext& ctx) {
                double a = req.get<double>("a");
                double b = req.get<double>("b");
                if (b == 0.0) {
                    throw std::runtime_error("division by zero");
                }
                return make_double_result(a / b);
            },
            "Divide a by b. Raises error if b is zero.")
        .enable_describe("Calculator")
        .build();

    server->run();
    return 0;
}
