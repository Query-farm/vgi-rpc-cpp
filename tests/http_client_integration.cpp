// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include <vgi_rpc/arrow_utils.h>
#include <vgi_rpc/http_client.h>
#include <vgi_rpc/metadata.h>

#include <arrow/array.h>
#include <arrow/array/builder_decimal.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/array/builder_binary.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/decimal.h>

#include <httplib.h>

#include <cstdlib>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#ifndef _WIN32
#include <csignal>
#include <poll.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

using namespace vgi_rpc;

namespace {

std::shared_ptr<arrow::Schema> typed_schema() {
    auto nullable_item = [](std::shared_ptr<arrow::DataType> type) {
        return arrow::field("item", std::move(type), /*nullable=*/true);
    };
    return arrow::schema({
        arrow::field("nullable_float", arrow::float64(), true),
        arrow::field("tags", arrow::list(nullable_item(arrow::utf8())), true),
        arrow::field("category", arrow::dictionary(arrow::int16(), arrow::utf8()), true),
        arrow::field("event_time", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"), true),
        arrow::field("amount", arrow::decimal128(18, 4), true),
        arrow::field(
            "nested",
            arrow::struct_(
                {arrow::field("name", arrow::utf8(), true),
                 arrow::field("scores", arrow::list(nullable_item(arrow::int32())), true)}),
            true),
    });
}

std::shared_ptr<arrow::Schema> producer_schema() {
    return arrow::schema({arrow::field("index", arrow::int64(), false),
                          arrow::field("payload", arrow::binary(), false)});
}

AnnotatedBatch producer_request(int64_t count, int64_t payload_bytes) {
    arrow::Int64Builder count_builder;
    arrow::Int64Builder payload_builder;
    VGI_RPC_THROW_NOT_OK(count_builder.Append(count));
    VGI_RPC_THROW_NOT_OK(payload_builder.Append(payload_bytes));
    auto schema = arrow::schema({arrow::field("count", arrow::int64(), false),
                                 arrow::field("payload_bytes", arrow::int64(), false)});
    return AnnotatedBatch::data(arrow::RecordBatch::Make(
        schema, 1, {unwrap(count_builder.Finish()), unwrap(payload_builder.Finish())}));
}

int64_t producer_index(const AnnotatedBatch& value) {
    return std::static_pointer_cast<arrow::Int64Array>(value.batch->column(0))->Value(0);
}

AnnotatedBatch int_request(const std::string& name, int64_t value) {
    arrow::Int64Builder builder;
    VGI_RPC_THROW_NOT_OK(builder.Append(value));
    auto schema = arrow::schema({arrow::field(name, arrow::int64(), false)});
    return AnnotatedBatch::data(arrow::RecordBatch::Make(schema, 1, {unwrap(builder.Finish())}));
}

AnnotatedBatch binary_request(const std::string& name, const std::string& value) {
    arrow::BinaryBuilder builder;
    VGI_RPC_THROW_NOT_OK(builder.Append(value));
    auto schema = arrow::schema({arrow::field(name, arrow::binary(), false)});
    return AnnotatedBatch::data(arrow::RecordBatch::Make(schema, 1, {unwrap(builder.Finish())}));
}

std::string binary_result(const AnnotatedBatch& value) {
    auto array = std::dynamic_pointer_cast<arrow::BinaryArray>(value.batch->column(0));
    if (!array || value.batch->num_rows() != 1 || array->IsNull(0)) {
        throw std::runtime_error("binary RPC result has an invalid Arrow shape");
    }
    return array->GetString(0);
}

std::shared_ptr<arrow::RecordBatch> null_batch(const std::shared_ptr<arrow::Schema>& schema,
                                               int64_t rows) {
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (const auto& field : schema->fields()) {
        arrays.push_back(unwrap(arrow::MakeArrayOfNull(field->type(), rows)));
    }
    return arrow::RecordBatch::Make(schema, rows, std::move(arrays));
}

std::shared_ptr<arrow::RecordBatch> populated_batch(const std::shared_ptr<arrow::Schema>& schema) {
    arrow::DoubleBuilder float_builder;
    VGI_RPC_THROW_NOT_OK(float_builder.Append(1.5));

    auto tag_values = std::make_shared<arrow::StringBuilder>();
    arrow::ListBuilder tags_builder(arrow::default_memory_pool(), tag_values,
                                    schema->field(1)->type());
    VGI_RPC_THROW_NOT_OK(tags_builder.Append());
    VGI_RPC_THROW_NOT_OK(tag_values->Append("alpha"));
    VGI_RPC_THROW_NOT_OK(tag_values->AppendNull());
    VGI_RPC_THROW_NOT_OK(tag_values->Append("omega"));

    arrow::Int16Builder index_builder;
    VGI_RPC_THROW_NOT_OK(index_builder.Append(0));
    arrow::StringBuilder dictionary_builder;
    VGI_RPC_THROW_NOT_OK(dictionary_builder.Append("blue"));
    auto category = unwrap(arrow::DictionaryArray::FromArrays(schema->field(2)->type(),
                                                              unwrap(index_builder.Finish()),
                                                              unwrap(dictionary_builder.Finish())));

    arrow::TimestampBuilder time_builder(schema->field(3)->type(), arrow::default_memory_pool());
    VGI_RPC_THROW_NOT_OK(time_builder.Append(1700000000123456LL));

    arrow::Decimal128Builder amount_builder(schema->field(4)->type(), arrow::default_memory_pool());
    VGI_RPC_THROW_NOT_OK(amount_builder.Append(unwrap(arrow::Decimal128::FromString("123.4500"))));

    arrow::StringBuilder name_builder;
    VGI_RPC_THROW_NOT_OK(name_builder.Append("node"));
    auto score_values = std::make_shared<arrow::Int32Builder>();
    auto nested_type = std::static_pointer_cast<arrow::StructType>(schema->field(5)->type());
    arrow::ListBuilder scores_builder(arrow::default_memory_pool(), score_values,
                                      nested_type->field(1)->type());
    VGI_RPC_THROW_NOT_OK(scores_builder.Append());
    VGI_RPC_THROW_NOT_OK(score_values->Append(7));
    VGI_RPC_THROW_NOT_OK(score_values->AppendNull());
    VGI_RPC_THROW_NOT_OK(score_values->Append(9));
    auto nested = unwrap(arrow::StructArray::Make(
        {unwrap(name_builder.Finish()), unwrap(scores_builder.Finish())}, nested_type->fields()));

    return arrow::RecordBatch::Make(
        schema, 1,
        {unwrap(float_builder.Finish()), unwrap(tags_builder.Finish()), std::move(category),
         unwrap(time_builder.Finish()), unwrap(amount_builder.Finish()), std::move(nested)});
}

AnnotatedBatch init_request() {
    return AnnotatedBatch::data(make_empty_batch(empty_schema()));
}

void require(bool condition, const std::string& message) {
    if (!condition) throw std::runtime_error(message);
}

AnnotatedBatch exchange_and_require_equal(HttpExchangeSession& session,
                                          const std::shared_ptr<arrow::Schema>& schema,
                                          const std::shared_ptr<arrow::RecordBatch>& input,
                                          bool taint_reserved = false) {
    AnnotatedBatch annotated = AnnotatedBatch::data(input);
    if (taint_reserved) {
        auto metadata = std::make_shared<arrow::KeyValueMetadata>();
        metadata->Append(keys::METHOD, "wrong_method");
        metadata->Append(keys::REQUEST_VERSION, "wrong_version");
        metadata->Append(keys::STATE_B64, "attacker_cursor");
        metadata->Append(keys::CALL_STATE_B64, "attacker_call");
        metadata->Append(keys::CANCEL, "1");
        annotated.custom_metadata = std::move(metadata);
    }
    auto output = session.exchange(annotated);
    require(output.batch != nullptr, "exchange returned a null batch");
    require(output.batch->schema()->Equals(*schema, true), "exchange changed declared schema");
    require(output.batch->Equals(*input, true), "exchange changed batch data");
    return output;
}

#ifndef _WIN32
class PythonWorker {
public:
    explicit PythonWorker(std::optional<int> producer_turn_bytes = std::nullopt,
                          bool external = false) {
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
            if (external) {
                ::execlp(python, python, "-m", "vgi_rpc.conformance.client_worker", "--http", "0",
                         "--external", "--external-threshold", "4096", static_cast<char*>(nullptr));
            } else if (producer_turn_bytes) {
                const std::string turn_bytes = std::to_string(*producer_turn_bytes);
                ::execlp(python, python, "-m", "vgi_rpc.conformance.client_worker", "--http", "0",
                         "--producer-turn-bytes", turn_bytes.c_str(), static_cast<char*>(nullptr));
            } else {
                ::execlp(python, python, "-m", "vgi_rpc.conformance.client_worker", "--http", "0",
                         static_cast<char*>(nullptr));
            }
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

        // PORT is printed immediately after bind.  On slower runners the
        // server thread may not have entered its accept loop yet, so make the
        // readiness contract explicit before the first one-shot RPC.
        bool accepting = false;
        for (int i = 0; i < 100; ++i) {
            httplib::Client probe("127.0.0.1", port_);
            probe.set_connection_timeout(0, 100000);
            if (const auto response = probe.Get("/health"); response) {
                accepting = true;
                break;
            }
            ::usleep(50000);
        }
        if (!accepting) {
            stop();
            port_ = 0;
        }
    }

    ~PythonWorker() { stop(); }
    int port() const noexcept { return port_; }

private:
    void stop() noexcept {
        if (pid_ <= 0) return;
        ::kill(pid_, SIGTERM);
        int status = 0;
        for (int i = 0; i < 100; ++i) {
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
    std::cerr << "native HTTP client conformance worker launcher is POSIX-only\n";
    return 77;
#else
    try {
        PythonWorker worker;
        if (worker.port() == 0) {
            std::cerr << "Python vgi_rpc.conformance.client_worker is unavailable\n";
            return 77;
        }

        HttpClientConfig config;
        config.prefix = "";
        config.max_request_bytes = 8 * 1024 * 1024;
        config.max_response_bytes = 8 * 1024 * 1024;
        auto client = HttpClient::builder("http://127.0.0.1:" + std::to_string(worker.port()))
                          .config(config)
                          .build();
        const auto schema = typed_schema();

        const auto description = client.describe();
        require(description.method("typed_exchange") != nullptr,
                "native HTTP describe did not parse the Python worker protocol");

        const auto producer_output = producer_schema();
        auto producer =
            client.open_producer("producer_sequence", producer_request(3, 4), producer_output);
        auto first = producer.next_with_token();
        require(first && producer_index(first->value) == 0 && !first->resume_token.empty(),
                "producer init batch or resume token was lost");
        producer.close();
        auto resumed =
            client.resume_stream("producer_sequence", first->resume_token, producer_output);
        auto second = resumed.tick();
        require(second && producer_index(*second) == 1,
                "producer did not resume at the exported token");
        auto third = resumed.tick();
        require(third && producer_index(*third) == 2,
                "producer continuation did not rotate its cursor");
        require(!resumed.tick() && resumed.finished(),
                "producer did not terminate locally after its terminal response");

        auto zero =
            client.open_producer("producer_zero_row_then_value", init_request(), producer_output);
        auto zero_batch = zero.tick();
        require(zero_batch && zero_batch->batch->num_rows() == 0,
                "metadata-free zero-row producer data was swallowed as control");
        auto after_zero = zero.tick();
        require(after_zero && producer_index(*after_zero) == 7,
                "producer did not continue after zero-row application data");
        require(!zero.tick(), "zero-row producer did not terminate");

        auto emitted =
            client.open_producer("producer_emit_and_finish", init_request(), producer_output);
        auto terminal_value = emitted.tick();
        require(terminal_value && producer_index(*terminal_value) == 99 && !emitted.tick(),
                "emit-and-finish producer lost its terminal value");
        auto empty = client.open_producer("producer_empty", init_request(), producer_output);
        require(!empty.tick() && empty.finished(),
                "empty producer did not terminate during initialization");

        {
            // The shared HTTP budget contract has a 64 KiB floor. Keep the
            // fixture at that floor while still forcing the producer to span
            // multiple lock-step responses.
            PythonWorker capped_worker(64 * 1024);
            require(capped_worker.port() != 0, "capped producer conformance worker is unavailable");
            auto lockstep_client =
                HttpClient::builder("http://127.0.0.1:" + std::to_string(capped_worker.port()))
                    .config(config)
                    .build();
            auto lockstep = lockstep_client.open_producer(
                "producer_sequence", producer_request(100, 1024), producer_output);
            for (int64_t expected = 0; expected < 100; ++expected) {
                auto value = lockstep.tick();
                require(value && producer_index(*value) == expected,
                        "capped lock-step producer dropped or reordered a batch");
            }
            require(!lockstep.tick() && lockstep.finished(),
                    "capped lock-step producer did not reach its terminal response");
        }

        {
            PythonWorker external_worker(std::nullopt, true);
            require(external_worker.port() != 0,
                    "external-location conformance worker is unavailable");
            ClientExternalHttpOptions external_options;
            external_options.url_policy = ExternalUrlPolicy::LOOPBACK_HTTP_TEST;
            auto external_client =
                HttpClient::builder("http://127.0.0.1:" + std::to_string(external_worker.port()))
                    .config(config)
                    .external_http_options(external_options)
                    .build();
            const auto upload_urls = external_client.request_upload_urls(2);
            require(upload_urls.size() == 2 && !upload_urls[0].upload_url.empty() &&
                        !upload_urls[0].download_url.empty() &&
                        upload_urls[0].upload_url != upload_urls[0].download_url,
                    "public upload URL API lost its method-bound pair");
            bool rejected_bad_count = false;
            try {
                (void)external_client.request_upload_urls(0);
            } catch (const std::invalid_argument&) {
                rejected_bad_count = true;
            }
            require(rejected_bad_count, "public upload URL API accepted count=0");
            const auto bytes_schema =
                arrow::schema({arrow::field("result", arrow::binary(), false)});
            std::string expected(32768, '\0');
            uint32_t entropy = 0x9e3779b9U;
            for (char& byte : expected) {
                entropy ^= entropy << 13;
                entropy ^= entropy >> 17;
                entropy ^= entropy << 5;
                byte = static_cast<char>(entropy & 0xffU);
            }
            // Start with the oversized request so the client must recover
            // from the server's pre-dispatch 413, harvest capabilities, vend
            // a URL pair, upload, and retry exactly once.
            auto reactive_echo =
                external_client.call("echo_bytes", binary_request("value", expected), bytes_schema);
            require(binary_result(reactive_echo) == expected,
                    "413-triggered request externalization did not recover");

            auto large =
                external_client.call("large_response", int_request("size", 32768), bytes_schema);
            const auto fetched = binary_result(large);
            require(fetched.size() == 32768,
                    "externalized response was not resolved to application data");
            for (size_t index = 0; index < fetched.size(); ++index) {
                require(static_cast<unsigned char>(fetched[index]) == index % 251,
                        "externalized response bytes were corrupted");
            }

            // Cached capabilities make the next oversized body externalize
            // proactively, without a second rejected inline attempt.
            auto echoed =
                external_client.call("echo_bytes", binary_request("value", expected), bytes_schema);
            require(binary_result(echoed) == expected,
                    "oversized request was not externalized and echoed exactly");

            auto disabled =
                HttpClient::builder("http://127.0.0.1:" + std::to_string(external_worker.port()))
                    .config(config)
                    .disable_external_locations()
                    .build();
            bool failed_closed = false;
            try {
                (void)disabled.call("large_response", int_request("size", 32768), bytes_schema);
            } catch (const HttpClientError& error) {
                failed_closed =
                    std::string(error.what()).find("resolution is disabled") != std::string::npos;
            }
            require(failed_closed,
                    "disabled external-location resolution returned a pointer as data");
        }

        auto session = client.open_exchange("typed_exchange", init_request(), schema, schema);
        // Caller-supplied protocol metadata cannot override the client's
        // method, version, cursor, call token, or cancellation state.
        (void)exchange_and_require_equal(session, schema, null_batch(schema, 1), true);
        (void)exchange_and_require_equal(session, schema, null_batch(schema, 0));
        const auto populated = populated_batch(schema);
        const auto retained = exchange_and_require_equal(session, schema, populated);
        // The returned arrays must own their IPC backing after the response
        // parser is gone and after a later request reuses the same client.
        (void)exchange_and_require_equal(session, schema, null_batch(schema, 1));
        require(retained.batch->Equals(*populated, true),
                "returned batch did not retain its response buffer ownership");
        session.close();
        session.close();

        // The strict Python middleware rejects this body before dispatch.  A
        // subsequent fresh session on the same native client must still work.
        const auto wrong_schema = arrow::schema({arrow::field("wrong", arrow::int64(), true)});
        auto bad = client.open_exchange("typed_exchange", init_request(), wrong_schema, schema);
        bool saw_bad_schema = false;
        try {
            (void)bad.exchange(AnnotatedBatch::data(null_batch(wrong_schema, 1)));
        } catch (const HttpClientError& error) {
            saw_bad_schema = error.http_status() == 400;
        }
        require(saw_bad_schema, "strict bad-schema response was not surfaced as HTTP 400");
        require(!bad.active(), "failed exchange retained an ambiguous continuation token");
        bool resent_poisoned_token = false;
        try {
            (void)bad.exchange(AnnotatedBatch::data(null_batch(wrong_schema, 1)));
            resent_poisoned_token = true;
        } catch (const HttpClientError&) {
        }
        require(!resent_poisoned_token, "failed exchange allowed its old token to be resent");
        bad.close();
        auto recovered = client.open_exchange("typed_exchange", init_request(), schema, schema);
        (void)exchange_and_require_equal(recovered, schema, null_batch(schema, 1));
        recovered.cancel();

        bool saw_rpc_error = false;
        try {
            (void)client.open_exchange("missing_exchange", init_request(), schema, schema);
        } catch (const RpcRemoteError&) {
            saw_rpc_error = true;
        }
        require(saw_rpc_error, "HTTP 200 RPC exception envelope was not decoded");
    } catch (const std::exception& error) {
        std::cerr << "native HTTP client conformance failed: " << error.what() << "\n";
        return 1;
    }
    return 0;
#endif
}
