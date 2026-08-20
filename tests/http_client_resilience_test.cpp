// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include <vgi_rpc/arrow_utils.h>
#include <vgi_rpc/http_client.h>
#include <vgi_rpc/metadata.h>
#include <vgi_rpc/wire.h>

#include <arrow/array/builder_primitive.h>
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include <httplib.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509v3.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <future>
#include <memory>
#include <mutex>
#include <random>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace vgi_rpc;

namespace {

constexpr const char* kArrowContentType = "application/vnd.apache.arrow.stream";

void require(bool condition, const std::string& message) {
    if (!condition) throw std::runtime_error(message);
}

std::shared_ptr<arrow::Schema> value_schema() {
    return arrow::schema({arrow::field("value", arrow::int64(), true)});
}

std::shared_ptr<arrow::RecordBatch> value_batch(const std::shared_ptr<arrow::Schema>& schema,
                                                int64_t value) {
    arrow::Int64Builder builder;
    VGI_RPC_THROW_NOT_OK(builder.Append(value));
    return arrow::RecordBatch::Make(schema, 1, {unwrap(builder.Finish())});
}

AnnotatedBatch empty_request() {
    return AnnotatedBatch::data(make_empty_batch(empty_schema()));
}

std::string unary_response(const std::shared_ptr<arrow::Schema>& schema, int64_t value) {
    auto output = unwrap(arrow::io::BufferOutputStream::Create());
    write_ipc_stream(output, schema, {AnnotatedBatch::data(value_batch(schema, value))});
    auto buffer = unwrap(output->Finish());
    return std::string(reinterpret_cast<const char*>(buffer->data()),
                       static_cast<size_t>(buffer->size()));
}

std::string request_id_from_arrow(const httplib::Request& request) {
    auto buffer = arrow::Buffer::FromString(request.body);
    auto input = std::make_shared<arrow::io::BufferReader>(std::move(buffer));
    const auto contents = read_ipc_stream(input);
    if (!contents || contents->batches.size() != 1) return {};
    return get_metadata_value(contents->batches[0].custom_metadata, keys::REQUEST_ID);
}

class PlainFaultServer {
public:
    explicit PlainFaultServer(std::shared_ptr<arrow::Schema> schema)
        : schema_(std::move(schema)), response_(unary_response(schema_, 17)) {
        install_handlers();
        port_ = server_.bind_to_any_port("127.0.0.1");
        if (port_ <= 0) throw std::runtime_error("failed to bind HTTP resilience server");
        thread_ = std::thread([this] { (void)server_.listen_after_bind(); });
        wait_until_running();
    }

    ~PlainFaultServer() {
        server_.stop();
        if (thread_.joinable()) thread_.join();
    }

    std::string origin() const { return "http://127.0.0.1:" + std::to_string(port_); }

    std::atomic<int> ok_requests{0};
    std::atomic<int> redirect_target_requests{0};
    std::atomic<int> retry_requests{0};
    std::atomic<int> slow_requests{0};
    std::atomic<int> capability_requests{0};
    std::atomic<bool> call_headers_valid{false};
    std::atomic<bool> retry_request_ids_valid{true};

private:
    void wait_until_running() {
        for (int attempt = 0; attempt < 1000 && !server_.is_running(); ++attempt) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        if (!server_.is_running()) throw std::runtime_error("HTTP resilience server did not start");
    }

    void arrow_response(httplib::Response& response) {
        response.set_content(response_, kArrowContentType);
    }

    void install_handlers() {
        server_.Options("/health",
                        [this](const httplib::Request& request, httplib::Response& response) {
                            ++capability_requests;
                            if (request.get_header_value("Authorization") != "Bearer dynamic") {
                                response.status = 401;
                                return;
                            }
                            response.status = 204;
                        });
        server_.Options("/large/health", [](const httplib::Request&, httplib::Response& response) {
            response.status = 500;
            response.set_content(std::string(16 * 1024, 'c'), "text/plain");
        });
        server_.Post("/ok", [this](const httplib::Request&, httplib::Response& response) {
            ++ok_requests;
            arrow_response(response);
        });
        server_.Post("/large/ok", [this](const httplib::Request&, httplib::Response& response) {
            ++ok_requests;
            arrow_response(response);
        });
        server_.Post("/redirect", [](const httplib::Request&, httplib::Response& response) {
            response.status = 307;
            response.set_header("Location", "/redirect-target");
            response.set_header("Retry-After", "9");
            response.set_content("do not follow", "text/plain");
        });
        server_.Post("/redirect-target",
                     [this](const httplib::Request&, httplib::Response& response) {
                         ++redirect_target_requests;
                         arrow_response(response);
                     });
        server_.Post("/auth", [](const httplib::Request&, httplib::Response& response) {
            response.status = 401;
            response.set_header("WWW-Authenticate", "Bearer realm=\"vgi\"");
            response.set_header("VGI-Auth-Reason", "expired");
            response.set_header("Retry-After", "7");
            response.set_content(std::string(16 * 1024, 'x'), "text/plain");
        });
        server_.Post("/headers",
                     [this](const httplib::Request& request, httplib::Response& response) {
                         call_headers_valid.store(
                             request.get_header_value("Authorization") == "Bearer dynamic" &&
                             request.get_header_value("X-Layer") == "call" &&
                             request.get_header_value("X-Request-ID") == "caller-logical-id" &&
                             request_id_from_arrow(request) == "caller-logical-id");
                         arrow_response(response);
                     });
        server_.Post(
            "/retry", [this](const httplib::Request& request, httplib::Response& response) {
                const int attempt = retry_requests.fetch_add(1);
                const std::string header_id = request.get_header_value("X-Request-ID");
                const std::string arrow_id = request_id_from_arrow(request);
                {
                    std::lock_guard<std::mutex> lock(retry_mutex_);
                    if (attempt == 0) retry_request_id_ = header_id;
                    retry_request_ids_valid.store(
                        retry_request_ids_valid.load() && header_id == "retry-logical-id" &&
                        arrow_id == header_id && header_id == retry_request_id_);
                }
                if (attempt < 2) {
                    response.status = 503;
                    response.set_header("Retry-After", "1");
                    response.set_content("temporarily unavailable", "text/plain");
                    return;
                }
                arrow_response(response);
            });
        server_.Post("/slow", [this](const httplib::Request&, httplib::Response& response) {
            ++slow_requests;
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
            arrow_response(response);
        });
    }

    std::shared_ptr<arrow::Schema> schema_;
    std::string response_;
    httplib::Server server_;
    std::thread thread_;
    int port_ = 0;
    std::mutex retry_mutex_;
    std::string retry_request_id_;
};

#ifdef CPPHTTPLIB_OPENSSL_SUPPORT
class TlsFixtures {
public:
    TlsFixtures() {
        std::random_device random;
        for (int attempt = 0; attempt < 100; ++attempt) {
            directory_ = std::filesystem::temp_directory_path() /
                         ("vgi-rpc-http-tls-" + std::to_string(random()));
            if (std::filesystem::create_directory(directory_)) break;
        }
        if (directory_.empty() || !std::filesystem::exists(directory_)) {
            throw std::runtime_error("failed to create TLS fixture directory");
        }

        auto ca_key = generate_key();
        auto ca = generate_certificate(ca_key.get(), nullptr, nullptr, "vgi-rpc test CA", 1,
                                       "critical,CA:TRUE", nullptr);
        auto server_key_handle = generate_key();
        auto server =
            generate_certificate(server_key_handle.get(), ca.get(), ca_key.get(), "localhost", 2,
                                 "critical,CA:FALSE", "DNS:localhost,IP:127.0.0.1", "serverAuth");
        auto client_key_handle = generate_key();
        auto client = generate_certificate(client_key_handle.get(), ca.get(), ca_key.get(),
                                           "vgi-rpc test client", 3, "critical,CA:FALSE", nullptr,
                                           "clientAuth");

        ca_file = (directory_ / "ca.pem").string();
        server_certificate_file = (directory_ / "server.pem").string();
        server_private_key_file = (directory_ / "server.key").string();
        client_certificate_file = (directory_ / "client.pem").string();
        client_private_key_file = (directory_ / "client.key").string();
        write_certificate(ca_file, ca.get());
        write_certificate(server_certificate_file, server.get());
        write_private_key(server_private_key_file, server_key_handle.get());
        write_certificate(client_certificate_file, client.get());
        write_private_key(client_private_key_file, client_key_handle.get());
    }

    ~TlsFixtures() { std::filesystem::remove_all(directory_); }

    TlsFixtures(const TlsFixtures&) = delete;
    TlsFixtures& operator=(const TlsFixtures&) = delete;

    std::string ca_file;
    std::string server_certificate_file;
    std::string server_private_key_file;
    std::string client_certificate_file;
    std::string client_private_key_file;

private:
    using Key = std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)>;
    using Certificate = std::unique_ptr<X509, decltype(&X509_free)>;

    static Key generate_key() {
        std::unique_ptr<EVP_PKEY_CTX, decltype(&EVP_PKEY_CTX_free)> context(
            EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr), EVP_PKEY_CTX_free);
        if (!context || EVP_PKEY_keygen_init(context.get()) <= 0 ||
            EVP_PKEY_CTX_set_rsa_keygen_bits(context.get(), 2048) <= 0) {
            throw std::runtime_error("failed to initialize TLS fixture key generation");
        }
        EVP_PKEY* raw = nullptr;
        if (EVP_PKEY_keygen(context.get(), &raw) <= 0) {
            throw std::runtime_error("failed to generate TLS fixture key");
        }
        return Key(raw, EVP_PKEY_free);
    }

    static void add_extension(X509* certificate, X509* issuer, int nid, const char* value) {
        X509V3_CTX context;
        X509V3_set_ctx(&context, issuer, certificate, nullptr, nullptr, 0);
        std::unique_ptr<X509_EXTENSION, decltype(&X509_EXTENSION_free)> extension(
            X509V3_EXT_conf_nid(nullptr, &context, nid, const_cast<char*>(value)),
            X509_EXTENSION_free);
        if (!extension || X509_add_ext(certificate, extension.get(), -1) != 1) {
            throw std::runtime_error("failed to add TLS fixture certificate extension");
        }
    }

    static Certificate generate_certificate(EVP_PKEY* key, X509* issuer, EVP_PKEY* issuer_key,
                                            const char* common_name, long serial,
                                            const char* basic_constraints,
                                            const char* subject_alt_name,
                                            const char* extended_key_usage = nullptr) {
        Certificate certificate(X509_new(), X509_free);
        if (!certificate || X509_set_version(certificate.get(), 2) != 1 ||
            ASN1_INTEGER_set(X509_get_serialNumber(certificate.get()), serial) != 1 ||
            !X509_gmtime_adj(X509_get_notBefore(certificate.get()), -3600) ||
            !X509_gmtime_adj(X509_get_notAfter(certificate.get()), 86400) ||
            X509_set_pubkey(certificate.get(), key) != 1) {
            throw std::runtime_error("failed to initialize TLS fixture certificate");
        }
        X509_NAME* subject = X509_get_subject_name(certificate.get());
        if (!subject ||
            X509_NAME_add_entry_by_txt(subject, "CN", MBSTRING_ASC,
                                       reinterpret_cast<const unsigned char*>(common_name), -1, -1,
                                       0) != 1 ||
            X509_set_issuer_name(certificate.get(),
                                 issuer ? X509_get_subject_name(issuer) : subject) != 1) {
            throw std::runtime_error("failed to name TLS fixture certificate");
        }
        add_extension(certificate.get(), issuer ? issuer : certificate.get(), NID_basic_constraints,
                      basic_constraints);
        if (subject_alt_name) {
            add_extension(certificate.get(), issuer, NID_subject_alt_name, subject_alt_name);
        }
        if (extended_key_usage) {
            add_extension(certificate.get(), issuer, NID_ext_key_usage, extended_key_usage);
        }
        if (X509_sign(certificate.get(), issuer_key ? issuer_key : key, EVP_sha256()) <= 0) {
            throw std::runtime_error("failed to sign TLS fixture certificate");
        }
        return certificate;
    }

    static void write_certificate(const std::string& path, X509* certificate) {
        std::unique_ptr<FILE, decltype(&std::fclose)> output(std::fopen(path.c_str(), "wb"),
                                                             std::fclose);
        if (!output || PEM_write_X509(output.get(), certificate) != 1) {
            throw std::runtime_error("failed to write TLS fixture certificate");
        }
    }

    static void write_private_key(const std::string& path, EVP_PKEY* key) {
        std::unique_ptr<FILE, decltype(&std::fclose)> output(std::fopen(path.c_str(), "wb"),
                                                             std::fclose);
        if (!output ||
            PEM_write_PrivateKey(output.get(), key, nullptr, nullptr, 0, nullptr, nullptr) != 1) {
            throw std::runtime_error("failed to write TLS fixture private key");
        }
    }

    std::filesystem::path directory_;
};

class TlsFaultServer {
public:
    TlsFaultServer(const std::string& certificate, const std::string& private_key,
                   const std::string& client_ca, std::shared_ptr<arrow::Schema> schema)
        : response_(unary_response(schema, 29)),
          server_(certificate.c_str(), private_key.c_str(),
                  client_ca.empty() ? nullptr : client_ca.c_str()) {
        if (!server_.is_valid()) throw std::runtime_error("failed to initialize TLS test server");
        server_.Post("/ok", [this](const httplib::Request&, httplib::Response& response) {
            response.set_content(response_, kArrowContentType);
        });
        port_ = server_.bind_to_any_port("127.0.0.1");
        if (port_ <= 0) throw std::runtime_error("failed to bind TLS test server");
        thread_ = std::thread([this] { (void)server_.listen_after_bind(); });
        for (int attempt = 0; attempt < 1000 && !server_.is_running(); ++attempt) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        if (!server_.is_running()) throw std::runtime_error("TLS test server did not start");
    }

    ~TlsFaultServer() {
        server_.stop();
        if (thread_.joinable()) thread_.join();
    }

    std::string origin() const { return "https://127.0.0.1:" + std::to_string(port_); }

private:
    std::string response_;
    httplib::SSLServer server_;
    std::thread thread_;
    int port_ = 0;
};
#endif

template <typename Function>
HttpClientError require_client_error(Function&& function, const std::string& context) {
    try {
        std::forward<Function>(function)();
    } catch (const HttpClientError& error) {
        return error;
    }
    throw std::runtime_error(context + " did not throw HttpClientError");
}

HttpClientBuilder plain_builder(const std::string& origin) {
    HttpClientConfig config;
    config.prefix = "";
    config.compression_level = std::nullopt;
    config.allow_insecure_credentials = true;
    return HttpClient::builder(origin).config(std::move(config));
}

void test_builder_errors_and_redirects(PlainFaultServer& server,
                                       const std::shared_ptr<arrow::Schema>& schema) {
    RetryPolicy retry = RetryPolicy::disabled();
    auto client = plain_builder(server.origin()).retry_policy(retry).build();

    const auto redirect = require_client_error(
        [&] { (void)client.call("redirect", empty_request(), schema); }, "RPC redirect");
    require(redirect.kind() == HttpClientErrorKind::HTTP_STATUS && redirect.status_code() == 307 &&
                redirect.method() == "redirect" && redirect.retry_after() == "9" &&
                redirect.response_body() == "do not follow",
            "redirect did not produce a complete structured HTTP error");
    require(server.redirect_target_requests.load() == 0, "RPC redirect was followed");

    try {
        (void)client.call("auth", empty_request(), schema);
        throw std::runtime_error("authentication response did not throw");
    } catch (const HttpAuthenticationError& error) {
        require(error.kind() == HttpClientErrorKind::AUTHENTICATION && error.status_code() == 401 &&
                    error.www_authenticate() == "Bearer realm=\"vgi\"" &&
                    error.auth_reason() == "expired" && error.retry_after() == "7",
                "authentication headers were not preserved structurally");
        require(error.response_body().size() == 4096,
                "structured authentication body was not bounded");
    }

    const RetryPolicy defaults;
    require(defaults.max_attempts == 3 && defaults.initial_backoff.count() == 100 &&
                defaults.max_backoff.count() == 10'000 && defaults.multiplier == 2.0 &&
                defaults.jitter == 0.2,
            "RetryPolicy schedule defaults changed unexpectedly");

    HttpClientConfig limited_config;
    limited_config.prefix = "/large";
    limited_config.compression_level = std::nullopt;
    limited_config.max_encoded_response_bytes = 1024;
    limited_config.max_decoded_response_bytes = 1024;
    auto limited = HttpClient::builder(server.origin()).config(limited_config).build();
    const auto capability_limit =
        require_client_error([&] { (void)limited.capabilities(); }, "capability response limit");
    require(capability_limit.kind() == HttpClientErrorKind::LIMIT,
            "oversized capability body did not fail under the encoded limit");
    require(limited.call("ok", empty_request(), schema).batch->num_rows() == 1,
            "client did not recover after an oversized capability response");
}

void test_auth_callback_and_call_options(PlainFaultServer& server,
                                         const std::shared_ptr<arrow::Schema>& schema) {
    HttpClient* reentrant_client = nullptr;
    std::atomic<bool> reentered{false};
    auto builder = plain_builder(server.origin()).header("X-Layer", "static");
    builder.auth_callback([&](const HttpAuthRequest& request) {
        require(!request.request_id.empty(), "auth callback received no request ID");
        if (!reentered.exchange(true)) {
            require(reentrant_client != nullptr, "auth callback ran before client publication");
            (void)reentrant_client->capabilities();
        }
        return std::map<std::string, std::string>{{"Authorization", "Bearer dynamic"},
                                                  {"X-Layer", "auth"}};
    });
    // The PIMPL is deeply copyable; changing the copy must not alter the
    // builder used below.
    auto changed_copy = builder;
    changed_copy.prefix("/unused");
    auto client = builder.build();
    reentrant_client = &client;

    CallOptions options;
    options.request_id = "caller-logical-id";
    options.headers["X-Layer"] = "call";
    const auto result = client.call("headers", empty_request(), schema, options);
    require(result.batch && result.batch->num_rows() == 1 && server.call_headers_valid.load(),
            "per-call headers, auth, or logical request ID were not applied");
    require(server.capability_requests.load() == 1,
            "auth callback did not safely re-enter the client outside its transport lock");

    CallOptions bad;
    bad.request_id = "bad\r\nid";
    bool rejected = false;
    try {
        (void)client.call("ok", empty_request(), schema, bad);
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    require(rejected, "invalid explicit request ID was accepted");

    auto callback_failure = plain_builder(server.origin())
                                .auth_callback([](const HttpAuthRequest&) {
                                    throw std::runtime_error("token refresh failed");
                                    return std::map<std::string, std::string>{};
                                })
                                .build();
    const auto auth_error =
        require_client_error([&] { (void)callback_failure.call("ok", empty_request(), schema); },
                             "credential callback failure");
    require(auth_error.kind() == HttpClientErrorKind::AUTHENTICATION && auth_error.method() == "ok",
            "credential callback failure was not structured as authentication");
}

void test_retry_ids(PlainFaultServer& server, const std::shared_ptr<arrow::Schema>& schema) {
    RetryPolicy policy;
    policy.initial_backoff = std::chrono::milliseconds::zero();
    policy.max_backoff = std::chrono::milliseconds::zero();
    policy.jitter = 0.0;
    policy.retryable_status_codes = {503};
    auto client = plain_builder(server.origin()).retry_policy(policy).build();
    CallOptions options;
    options.request_id = "retry-logical-id";
    options.idempotent = true;
    const auto result = client.call("retry", empty_request(), schema, options);
    require(result.batch && result.batch->num_rows() == 1 && server.retry_requests.load() == 3 &&
                server.retry_request_ids_valid.load(),
            "retry attempts did not preserve the logical request ID");
}

void test_deadline_cancel_and_recovery(PlainFaultServer& server,
                                       const std::shared_ptr<arrow::Schema>& schema) {
    auto client = plain_builder(server.origin()).retry_policy(RetryPolicy::disabled()).build();
    const auto timeout = require_client_error(
        [&] {
            (void)client.call("slow", empty_request(), schema,
                              CallOptions::with_timeout(std::chrono::milliseconds(30)));
        },
        "per-call deadline");
    require(timeout.kind() == HttpClientErrorKind::TIMEOUT,
            "per-call deadline was not classified as a timeout (kind=" +
                std::to_string(static_cast<int>(timeout.kind())) + ", message=" + timeout.what() +
                ")");
    auto after_timeout = client.call("ok", empty_request(), schema);
    require(after_timeout.batch && after_timeout.batch->num_rows() == 1,
            "same client did not recover after a deadline");

    std::stop_source source;
    CallOptions options;
    options.stop_token = source.get_token();
    const int before = server.slow_requests.load();
    auto pending = std::async(std::launch::async, [&] {
        try {
            (void)client.call("slow", empty_request(), schema, options);
        } catch (const HttpClientError& error) {
            return error.kind();
        }
        return HttpClientErrorKind::PROTOCOL;
    });
    for (int attempt = 0; attempt < 1000 && server.slow_requests.load() == before; ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    require(server.slow_requests.load() > before, "cancellation test request never reached server");
    source.request_stop();
    require(pending.get() == HttpClientErrorKind::CANCELLED,
            "std::stop_token cancellation was not classified as cancelled");
    auto after_cancel = client.call("ok", empty_request(), schema);
    require(after_cancel.batch && after_cancel.batch->num_rows() == 1,
            "same client did not recover after std::stop_token cancellation");
}

void test_tls(const std::shared_ptr<arrow::Schema>& schema) {
#ifdef CPPHTTPLIB_OPENSSL_SUPPORT
    const TlsFixtures fixture;

    TlsFaultServer server(fixture.server_certificate_file, fixture.server_private_key_file, {},
                          schema);
    const auto system_trust = require_client_error(
        [&] {
            auto client = HttpClient::builder(server.origin())
                              .prefix("")
                              .compression_level(std::nullopt)
                              .retry_policy(RetryPolicy::disabled())
                              .build();
            (void)client.call("ok", empty_request(), schema);
        },
        "untrusted server certificate");
    require(system_trust.kind() == HttpClientErrorKind::TLS,
            "default system trust did not reject the private test CA");

    auto trusted = HttpClient::builder(server.origin())
                       .prefix("")
                       .compression_level(std::nullopt)
                       .custom_ca_file(fixture.ca_file)
                       .build();
    require(trusted.call("ok", empty_request(), schema).batch->num_rows() == 1,
            "custom CA did not establish verified HTTPS");

    auto insecure = HttpClient::builder(server.origin())
                        .prefix("")
                        .compression_level(std::nullopt)
                        .dangerous_disable_tls_verification_for_testing()
                        .build();
    require(insecure.call("ok", empty_request(), schema).batch->num_rows() == 1,
            "explicit test-only insecure TLS mode did not connect");

    TlsFaultServer mtls_server(fixture.server_certificate_file, fixture.server_private_key_file,
                               fixture.ca_file, schema);
    const auto missing_client_cert = require_client_error(
        [&] {
            auto client = HttpClient::builder(mtls_server.origin())
                              .prefix("")
                              .compression_level(std::nullopt)
                              .custom_ca_file(fixture.ca_file)
                              .retry_policy(RetryPolicy::disabled())
                              .build();
            (void)client.call("ok", empty_request(), schema);
        },
        "mTLS without a client certificate");
    require(missing_client_cert.kind() == HttpClientErrorKind::TLS,
            "mTLS rejection was not classified as TLS (kind=" +
                std::to_string(static_cast<int>(missing_client_cert.kind())) +
                ", message=" + missing_client_cert.what() + ")");

    auto mtls =
        HttpClient::builder(mtls_server.origin())
            .prefix("")
            .compression_level(std::nullopt)
            .custom_ca_file(fixture.ca_file)
            .client_certificate(fixture.client_certificate_file, fixture.client_private_key_file)
            .build();
    require(mtls.call("ok", empty_request(), schema).batch->num_rows() == 1,
            "verified mTLS request failed");
#else
    (void)schema;
    throw std::runtime_error("HTTP client was built without OpenSSL support");
#endif
}

}  // namespace

int main() {
    try {
        const auto schema = value_schema();
        PlainFaultServer server(schema);
        test_builder_errors_and_redirects(server, schema);
        test_auth_callback_and_call_options(server, schema);
        test_retry_ids(server, schema);
        test_deadline_cancel_and_recovery(server, schema);
        test_tls(schema);
    } catch (const std::exception& error) {
        std::cerr << "native HTTP resilience regression failed: " << error.what() << "\n";
        return 1;
    }
    return 0;
}
