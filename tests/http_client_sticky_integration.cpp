// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include <vgi_rpc/arrow_utils.h>
#include <vgi_rpc/http_client.h>
#include <vgi_rpc/metadata.h>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include <httplib.h>

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>

#ifndef _WIN32
#include <csignal>
#include <poll.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

using namespace vgi_rpc;

namespace {

void require(bool condition, const std::string& message) {
    if (!condition) throw std::runtime_error(message);
}

AnnotatedBatch empty_request(const std::shared_ptr<arrow::Schema>& schema) {
    return AnnotatedBatch::data(make_empty_batch(schema));
}

AnnotatedBatch int_request(const std::shared_ptr<arrow::Schema>& schema, int64_t value) {
    require(
        schema && schema->num_fields() == 1 && schema->field(0)->type()->id() == arrow::Type::INT64,
        "sticky worker parameter schema is not one int64 field");
    arrow::Int64Builder builder;
    VGI_RPC_THROW_NOT_OK(builder.Append(value));
    return AnnotatedBatch::data(arrow::RecordBatch::Make(schema, 1, {unwrap(builder.Finish())}));
}

int64_t int_result(const AnnotatedBatch& value) {
    require(value.batch && value.batch->num_columns() == 1 && value.batch->num_rows() == 1,
            "sticky worker result has an invalid Arrow shape");
    auto result = std::dynamic_pointer_cast<arrow::Int64Array>(value.batch->column(0));
    require(result && !result->IsNull(0), "sticky worker result is not a non-null int64");
    return result->Value(0);
}

AnnotatedBatch binary_request(const std::shared_ptr<arrow::Schema>& schema,
                              const std::string& value) {
    require(schema && schema->num_fields() == 1 &&
                schema->field(0)->type()->id() == arrow::Type::BINARY,
            "external worker parameter schema is not one binary field");
    arrow::BinaryBuilder builder;
    VGI_RPC_THROW_NOT_OK(builder.Append(value));
    return AnnotatedBatch::data(arrow::RecordBatch::Make(schema, 1, {unwrap(builder.Finish())}));
}

std::string binary_result(const AnnotatedBatch& value) {
    require(value.batch && value.batch->num_columns() == 1 && value.batch->num_rows() == 1,
            "external worker result has an invalid Arrow shape");
    auto result = std::dynamic_pointer_cast<arrow::BinaryArray>(value.batch->column(0));
    require(result && !result->IsNull(0), "external worker result is not non-null binary");
    return result->GetString(0);
}

std::shared_ptr<arrow::Schema> producer_schema() {
    return arrow::schema({arrow::field("index", arrow::int64(), false),
                          arrow::field("payload", arrow::binary(), false)});
}

bool has_header_case_insensitive(const std::map<std::string, std::string>& headers,
                                 std::string wanted) {
    auto lower = [](std::string value) {
        std::transform(value.begin(), value.end(), value.begin(), [](unsigned char character) {
            return static_cast<char>(std::tolower(character));
        });
        return value;
    };
    wanted = lower(std::move(wanted));
    return std::any_of(headers.begin(), headers.end(), [&](const auto& item) {
        return lower(item.first) == wanted && !item.second.empty();
    });
}

#ifndef _WIN32
class PythonWorker {
public:
    PythonWorker() {
        int output[2];
        if (::pipe(output) != 0) throw std::runtime_error("cannot create worker discovery pipe");
        pid_ = ::fork();
        if (pid_ < 0) throw std::runtime_error("cannot fork Python worker");
        if (pid_ == 0) {
            ::close(output[0]);
            ::dup2(output[1], STDOUT_FILENO);
            ::close(output[1]);
            const char* python = std::getenv("VGI_RPC_PYTHON");
            if (!python || !*python) python = "python3";
            ::execlp(python, python, "-m", "vgi_rpc.conformance.client_worker", "--http", "0",
                     "--prefix", "/vgi", "--sticky", "--external", "--external-threshold", "4096",
                     static_cast<char*>(nullptr));
            _exit(127);
        }
        ::close(output[1]);
        pollfd ready{output[0], POLLIN, 0};
        if (::poll(&ready, 1, 10000) <= 0) {
            ::close(output[0]);
            stop();
            return;
        }
        FILE* stream = ::fdopen(output[0], "r");
        char line[128]{};
        if (!stream || !std::fgets(line, sizeof(line), stream)) {
            if (stream) std::fclose(stream);
            stop();
            return;
        }
        std::fclose(stream);
        const std::string discovery(line);
        if (discovery.rfind("PORT:", 0) != 0) {
            stop();
            return;
        }
        port_ = std::stoi(discovery.substr(5));
        for (int attempt = 0; attempt < 100; ++attempt) {
            httplib::Client probe("127.0.0.1", port_);
            probe.set_connection_timeout(0, 100000);
            if (const auto response = probe.Get("/vgi/health"); response) return;
            ::usleep(50000);
        }
        stop();
        port_ = 0;
    }

    ~PythonWorker() { stop(); }
    int port() const noexcept { return port_; }

private:
    void stop() noexcept {
        if (pid_ <= 0) return;
        ::kill(pid_, SIGTERM);
        int status = 0;
        for (int attempt = 0; attempt < 100; ++attempt) {
            if (::waitpid(pid_, &status, WNOHANG) == pid_) {
                pid_ = -1;
                return;
            }
            ::usleep(50000);
        }
        ::kill(pid_, SIGKILL);
        (void)::waitpid(pid_, &status, 0);
        pid_ = -1;
    }

    pid_t pid_ = -1;
    int port_ = 0;
};
#endif

}  // namespace

int main() {
#ifdef _WIN32
    std::cerr << "native HTTP sticky conformance worker launcher is POSIX-only\n";
    return 77;
#else
    try {
        PythonWorker worker;
        if (worker.port() == 0) {
            std::cerr << "Python vgi_rpc.conformance.client_worker is unavailable\n";
            return 77;
        }

        ClientExternalHttpOptions external_options;
        external_options.url_policy = ExternalUrlPolicy::LOOPBACK_HTTP_TEST;
        auto client = HttpClient::builder("http://127.0.0.1:" + std::to_string(worker.port()))
                          .prefix("/vgi")
                          .external_http_options(external_options)
                          .build();
        const auto capabilities = client.capabilities();
        require(capabilities.sticky_enabled, "worker did not advertise sticky support");
        require(capabilities.sticky_default_ttl == 60,
                "worker did not advertise its 60-second sticky TTL");
        require(std::find(capabilities.sticky_echo_headers.begin(),
                          capabilities.sticky_echo_headers.end(),
                          "X-VGI-Worker-Affinity") != capabilities.sticky_echo_headers.end(),
                "worker did not advertise its routing echo header");

        const auto description = client.describe();
        const auto* open = description.method("open_client_session");
        const auto* increment = description.method("increment_client_session");
        const auto* close = description.method("close_client_session");
        const auto* producer = description.method("producer_sequence");
        const auto* echo_bytes = description.method("echo_bytes");
        const auto* large_response = description.method("large_response");
        require(open && increment && close && producer && echo_bytes && large_response,
                "sticky worker description is missing required methods");

        auto external_disabled =
            HttpClient::builder("http://127.0.0.1:" + std::to_string(worker.port()))
                .prefix("/vgi")
                .disable_external_locations()
                .build();
        bool disabled_urls_rejected = false;
        try {
            (void)external_disabled.request_upload_urls(1);
        } catch (const HttpClientError& error) {
            disabled_urls_rejected = error.kind() == HttpClientErrorKind::LIMIT;
        }
        require(disabled_urls_rejected,
                "disabled external support returned unvalidated upload URLs");

        auto session = client.with_session_token();
        require(int_result(session.call("open_client_session", int_request(open->params_schema, 10),
                                        open->result_schema)) == 10,
                "sticky session did not open at 10");
        const auto saved_token = session.current_session_token();
        const auto saved_echo = session.current_echo_headers();
        require(saved_token.has_value(), "session-opening response did not return a token");
        require(has_header_case_insensitive(saved_echo, "X-VGI-Worker-Affinity"),
                "session-opening response did not capture its routing echo header");
        const auto upload_urls = session.request_upload_urls(2);
        require(upload_urls.size() == 2 && !upload_urls[0].upload_url.empty() &&
                    !upload_urls[0].download_url.empty() && upload_urls[0].expires_at_us,
                "sticky upload-URL control request returned invalid URL records");
        bool invalid_count_rejected = false;
        try {
            (void)session.request_upload_urls(0);
        } catch (const std::invalid_argument&) {
            invalid_count_rejected = true;
        }
        require(invalid_count_rejected, "upload-URL control accepted a zero count");

        std::string external_payload(32768, '\0');
        uint32_t entropy = 0x9e3779b9U;
        for (char& byte : external_payload) {
            entropy ^= entropy << 13;
            entropy ^= entropy >> 17;
            entropy ^= entropy << 5;
            byte = static_cast<char>(entropy & 0xffU);
        }
        auto reactive =
            session.call("echo_bytes", binary_request(echo_bytes->params_schema, external_payload),
                         echo_bytes->result_schema);
        require(binary_result(reactive) == external_payload,
                "sticky reactive request externalization corrupted its payload");
        auto fetched =
            session.call("large_response", int_request(large_response->params_schema, 32768),
                         large_response->result_schema);
        require(binary_result(fetched).size() == 32768, "sticky external response was not fetched");
        auto proactive =
            session.call("echo_bytes", binary_request(echo_bytes->params_schema, external_payload),
                         echo_bytes->result_schema);
        require(binary_result(proactive) == external_payload,
                "sticky proactive request externalization corrupted its payload");
        require(int_result(session.call("increment_client_session",
                                        int_request(increment->params_schema, 5),
                                        increment->result_schema)) == 15,
                "sticky session did not increment to 15");

        arrow::Int64Builder count_builder;
        arrow::Int64Builder payload_builder;
        VGI_RPC_THROW_NOT_OK(count_builder.Append(2));
        VGI_RPC_THROW_NOT_OK(payload_builder.Append(4));
        auto producer_request = AnnotatedBatch::data(arrow::RecordBatch::Make(
            producer->params_schema, 1,
            {unwrap(count_builder.Finish()), unwrap(payload_builder.Finish())}));
        auto stream =
            session.open_producer("producer_sequence", producer_request, producer_schema());
        require(stream.tick().has_value() && stream.tick().has_value() && !stream.tick(),
                "sticky session did not propagate through every producer turn");

        require(int_result(session.call("increment_client_session",
                                        int_request(increment->params_schema, -2),
                                        increment->result_schema)) == 13,
                "sticky session did not decrement to 13");
        require(int_result(session.call("close_client_session", empty_request(close->params_schema),
                                        close->result_schema)) == 13,
                "sticky session did not close at 13");
        require(!session.current_session_token() && session.current_echo_headers().empty(),
                "VGI-Session-Close did not clear token and routing echoes");
        session.close();
        session.close();

        bool saw_session_lost = false;
        try {
            auto stale = client.with_session_token(saved_token, saved_echo);
            (void)stale.call("increment_client_session", int_request(increment->params_schema, 1),
                             increment->result_schema);
        } catch (const HttpSessionLostError& error) {
            saw_session_lost = error.exception_type() == "SessionLostError";
        }
        require(saw_session_lost, "stale token did not surface typed HttpSessionLostError");

        std::optional<std::string> deleted_token;
        std::map<std::string, std::string> deleted_echo;
        {
            auto scoped = client.with_session_token();
            (void)scoped.call("open_client_session", int_request(open->params_schema, 20),
                              open->result_schema);
            deleted_token = scoped.current_session_token();
            deleted_echo = scoped.current_echo_headers();
        }
        require(deleted_token.has_value(), "destructor teardown test did not open a session");
        saw_session_lost = false;
        try {
            auto stale = client.with_session_token(deleted_token, deleted_echo);
            (void)stale.call("increment_client_session", int_request(increment->params_schema, 1),
                             increment->result_schema);
        } catch (const HttpSessionLostError&) {
            saw_session_lost = true;
        }
        require(saw_session_lost, "session view destructor did not DELETE its live token");

        std::optional<std::string> detached_token;
        std::map<std::string, std::string> detached_echo;
        {
            auto detached = client.with_session_token();
            (void)detached.call("open_client_session", int_request(open->params_schema, 30),
                                open->result_schema);
            detached_echo = detached.current_echo_headers();
            detached_token = detached.detach();
        }
        require(detached_token.has_value(), "detach did not return its live token");
        auto resumed = client.with_session_token(detached_token, detached_echo);
        require(int_result(resumed.call("increment_client_session",
                                        int_request(increment->params_schema, 1),
                                        increment->result_schema)) == 31,
                "detached sticky session could not be resumed");
        resumed.close();
        resumed.close();
    } catch (const std::exception& error) {
        std::cerr << "native HTTP sticky conformance failed: " << error.what() << "\n";
        return 1;
    }
    return 0;
#endif
}
