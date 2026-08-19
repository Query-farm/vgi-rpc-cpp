// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include <vgi_rpc/server.h>
#include <vgi_rpc/http_client.h>

int main() {
    vgi_rpc::ServerBuilder builder;
    vgi_rpc::HttpClientConfig client_config;
    client_config.max_request_bytes = 1024;
    return 0;
}
