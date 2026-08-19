// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include <vgi_rpc/server.h>
#include <vgi_rpc/http_client.h>

#include <stop_token>

int main() {
    vgi_rpc::ServerBuilder builder;
    vgi_rpc::HttpClientConfig client_config;
    client_config.max_request_bytes = 1024;
    std::stop_source source;
    vgi_rpc::CallOptions options;
    options.stop_token = source.get_token();
    auto client_builder = vgi_rpc::HttpClient::builder("https://rpc.example.com")
                              .config(client_config)
                              .custom_ca_file("test-ca.pem")
                              .retry_policy(vgi_rpc::RetryPolicy::disabled());
    return 0;
}
