// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// The `conformance_negotiating_client` runner hook (vgi-rpc
// PROTOCOL_VERSIONS_V1.md §9): this port's HTTP client, bound to
// `conformance.Negotiated.v1`, declaring a newest version and a supported
// window, and negotiating per endpoint (§6).
//
//   conformance_negotiating_client BASE_URL PROTOCOL VERSION RANGE
//
// One line per call on stdin: `ping`.  One JSON line per call on stdout:
// `{"ok": "<result>"}`, or `{"error_kind": ..., "error_message": ...}`.
// The client-driver protocol is not used: its harness stamps
// `vgi_rpc.protocol_version` itself, which is the thing under test here.

#include <iostream>
#include <string>

#include <arrow/api.h>
#include <nlohmann/json.hpp>

#include "vgi_rpc/annotated_batch.h"
#include "vgi_rpc/arrow_utils.h"
#include "vgi_rpc/http_client.h"
#include "vgi_rpc/protocol_versions.h"

using namespace vgi_rpc;

int main(int argc, char** argv) {
    if (argc != 5) {
        std::cerr << "usage: conformance_negotiating_client BASE_URL PROTOCOL VERSION RANGE\n";
        return 2;
    }
    const std::string base_url = argv[1];
    std::optional<HttpClient> client;
    try {
        client.emplace(HttpClient::builder(base_url)
                           .prefix("")
                           .protocol(argv[2])
                           .protocol_version(argv[3])
                           .protocol_version_range(argv[4])
                           .build());
    } catch (const std::exception& e) {
        std::cout << nlohmann::json{{"error_kind", "setup"}, {"error_message", e.what()}}.dump()
                  << std::endl;
        return 1;
    }
    std::string line;
    while (std::getline(std::cin, line)) {
        if (line != "ping") {
            std::cout << nlohmann::json{{"error_kind", "driver"},
                                        {"error_message", "unknown command: " + line}}
                             .dump()
                      << std::endl;
            continue;
        }
        nlohmann::json out;
        try {
            const auto reply =
                client->call("ping", AnnotatedBatch::data(make_empty_batch(arrow::schema({}))));
            const auto column = reply.batch->column(0);
            std::string value;
            if (auto strings = std::dynamic_pointer_cast<arrow::StringArray>(column)) {
                value = strings->GetString(0);
            } else if (auto large = std::dynamic_pointer_cast<arrow::LargeStringArray>(column)) {
                value = large->GetString(0);
            }
            out = {{"ok", value}};
        } catch (const RpcRemoteError& e) {
            out = {{"error_kind", e.error_kind()}, {"error_message", e.what()}};
        } catch (const NoCommonVersionError& e) {
            out = {{"error_kind", e.error_kind()}, {"error_message", e.what()}};
        } catch (const std::exception& e) {
            out = {{"error_kind", "client_error"}, {"error_message", e.what()}};
        }
        std::cout << out.dump() << std::endl;
    }
    return 0;
}
