#pragma once

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <arrow/type.h>

#include "vgi_rpc/call_context.h"
#include "vgi_rpc/export.h"
#include "vgi_rpc/request.h"
#include "vgi_rpc/result.h"
#include "vgi_rpc/stream.h"

namespace vgi_rpc {

enum class MethodType {
    UNARY,
    STREAM,
};

struct MethodInfo {
    std::string name;
    MethodType method_type;
    std::shared_ptr<arrow::Schema> params_schema;
    std::shared_ptr<arrow::Schema> result_schema;
    std::function<Result(const Request&, CallContext&)> handler;
    std::string doc;
    bool has_return = true;

    // For streaming
    std::shared_ptr<arrow::Schema> input_schema;   // nullptr for unary
    std::shared_ptr<arrow::Schema> output_schema;   // nullptr for unary
    std::shared_ptr<arrow::Schema> header_schema;   // nullptr if no header
    std::function<Stream(const Request&, CallContext&)> stream_factory;
};

class Server;

class VGI_RPC_EXPORT ServerBuilder {
public:
    ServerBuilder() = default;

    // Register a unary method
    ServerBuilder& add_unary(
        const std::string& name,
        std::shared_ptr<arrow::Schema> params_schema,
        std::shared_ptr<arrow::Schema> result_schema,
        std::function<Result(const Request&, CallContext&)> handler,
        const std::string& doc = "");

    // Register a void unary method
    ServerBuilder& add_void(
        const std::string& name,
        std::shared_ptr<arrow::Schema> params_schema,
        std::function<void(const Request&, CallContext&)> handler,
        const std::string& doc = "");

    // Register a producer stream method
    ServerBuilder& add_producer(
        const std::string& name,
        std::shared_ptr<arrow::Schema> params_schema,
        std::shared_ptr<arrow::Schema> output_schema,
        std::function<Stream(const Request&, CallContext&)> factory,
        const std::string& doc = "",
        std::shared_ptr<arrow::Schema> header_schema = nullptr);

    // Register an exchange stream method
    ServerBuilder& add_exchange(
        const std::string& name,
        std::shared_ptr<arrow::Schema> params_schema,
        std::shared_ptr<arrow::Schema> input_schema,
        std::shared_ptr<arrow::Schema> output_schema,
        std::function<Stream(const Request&, CallContext&)> factory,
        const std::string& doc = "",
        std::shared_ptr<arrow::Schema> header_schema = nullptr);

    // Enable __describe__ introspection.
    // The describe response is a snapshot captured at build() time.
    ServerBuilder& enable_describe(const std::string& protocol_name = "");

    // Build the server
    std::unique_ptr<Server> build();

private:
    void check_duplicate(const std::string& name) const;

    std::vector<MethodInfo> methods_;
    bool describe_enabled_ = false;
    bool built_ = false;
    std::string protocol_name_;
};

// NOT thread-safe.  Designed for single-threaded pipe-based operation
// (one request at a time on stdin/stdout).
class VGI_RPC_EXPORT Server {
    friend class ServerBuilder;

public:
    void run();

    const std::string& server_id() const noexcept { return server_id_; }
    const std::unordered_map<std::string, MethodInfo>& methods() const noexcept { return methods_; }

    // Returns false on EOF (clean shutdown), true when a request was served.
    bool serve_one(const std::shared_ptr<arrow::io::InputStream>& input,
                   const std::shared_ptr<arrow::io::OutputStream>& output);

private:
    Server(std::unordered_map<std::string, MethodInfo> methods,
           std::string server_id);

    void serve_unary(const MethodInfo& method_info,
                     const Request& request,
                     const std::string& request_id,
                     const std::shared_ptr<arrow::io::OutputStream>& output);

    void serve_stream(const MethodInfo& method_info,
                      const Request& request,
                      const std::string& request_id,
                      const std::shared_ptr<arrow::io::InputStream>& input,
                      const std::shared_ptr<arrow::io::OutputStream>& output);

    std::unordered_map<std::string, MethodInfo> methods_;
    std::string server_id_;
};

}  // namespace vgi_rpc
