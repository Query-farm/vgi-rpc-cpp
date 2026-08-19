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
