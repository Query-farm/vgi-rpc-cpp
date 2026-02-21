#include "vgi_rpc/server.h"
#include "vgi_rpc/metadata.h"
#include "vgi_rpc/describe.h"

namespace vgi_rpc {

// ServerBuilder

void ServerBuilder::check_duplicate(const std::string& name) const {
    for (const auto& m : methods_) {
        if (m.name == name) {
            throw std::logic_error("Duplicate method name: '" + name + "'");
        }
    }
}

ServerBuilder& ServerBuilder::add_unary(
    const std::string& name,
    std::shared_ptr<arrow::Schema> params_schema,
    std::shared_ptr<arrow::Schema> result_schema,
    std::function<Result(const Request&, CallContext&)> handler,
    const std::string& doc) {
    if (!params_schema) throw std::invalid_argument("params_schema must not be null");
    if (!result_schema) throw std::invalid_argument("result_schema must not be null");
    check_duplicate(name);

    MethodInfo info;
    info.name = name;
    info.method_type = MethodType::UNARY;
    info.params_schema = std::move(params_schema);
    info.result_schema = std::move(result_schema);
    info.handler = std::move(handler);
    info.doc = doc;
    info.has_return = true;
    methods_.push_back(std::move(info));
    return *this;
}

ServerBuilder& ServerBuilder::add_void(
    const std::string& name,
    std::shared_ptr<arrow::Schema> params_schema,
    std::function<void(const Request&, CallContext&)> handler,
    const std::string& doc) {
    if (!params_schema) throw std::invalid_argument("params_schema must not be null");
    check_duplicate(name);

    MethodInfo info;
    info.name = name;
    info.method_type = MethodType::UNARY;
    info.params_schema = std::move(params_schema);
    info.result_schema = empty_schema();
    info.has_return = false;
    info.doc = doc;
    info.handler = [h = std::move(handler)](const Request& req, CallContext& ctx) -> Result {
        h(req, ctx);
        return Result::void_result();
    };
    methods_.push_back(std::move(info));
    return *this;
}

ServerBuilder& ServerBuilder::add_producer(
    const std::string& name,
    std::shared_ptr<arrow::Schema> params_schema,
    std::shared_ptr<arrow::Schema> output_schema,
    std::function<Stream(const Request&, CallContext&)> factory,
    const std::string& doc,
    std::shared_ptr<arrow::Schema> header_schema) {
    if (!params_schema) throw std::invalid_argument("params_schema must not be null");
    if (!output_schema) throw std::invalid_argument("output_schema must not be null");
    check_duplicate(name);

    MethodInfo info;
    info.name = name;
    info.method_type = MethodType::STREAM;
    info.params_schema = std::move(params_schema);
    info.result_schema = empty_schema();
    info.input_schema = empty_schema();
    info.output_schema = std::move(output_schema);
    info.header_schema = std::move(header_schema);
    info.has_return = true;
    info.doc = doc;
    info.stream_factory = std::move(factory);
    methods_.push_back(std::move(info));
    return *this;
}

ServerBuilder& ServerBuilder::add_exchange(
    const std::string& name,
    std::shared_ptr<arrow::Schema> params_schema,
    std::shared_ptr<arrow::Schema> input_schema,
    std::shared_ptr<arrow::Schema> output_schema,
    std::function<Stream(const Request&, CallContext&)> factory,
    const std::string& doc,
    std::shared_ptr<arrow::Schema> header_schema) {
    if (!params_schema) throw std::invalid_argument("params_schema must not be null");
    if (!input_schema) throw std::invalid_argument("input_schema must not be null");
    if (!output_schema) throw std::invalid_argument("output_schema must not be null");
    check_duplicate(name);

    MethodInfo info;
    info.name = name;
    info.method_type = MethodType::STREAM;
    info.params_schema = std::move(params_schema);
    info.result_schema = empty_schema();
    info.input_schema = std::move(input_schema);
    info.output_schema = std::move(output_schema);
    info.header_schema = std::move(header_schema);
    info.has_return = true;
    info.doc = doc;
    info.stream_factory = std::move(factory);
    methods_.push_back(std::move(info));
    return *this;
}

ServerBuilder& ServerBuilder::server_id(std::string id) {
    server_id_ = std::move(id);
    return *this;
}

ServerBuilder& ServerBuilder::enable_describe(const std::string& protocol_name) {
    describe_enabled_ = true;
    protocol_name_ = protocol_name;
    return *this;
}

std::unique_ptr<Server> ServerBuilder::build() {
    if (built_) {
        throw std::logic_error("ServerBuilder::build() has already been called");
    }
    built_ = true;

    auto server_id = server_id_.empty() ? random_hex(12) : std::move(server_id_);

    std::unordered_map<std::string, MethodInfo> method_map;
    for (auto& m : methods_) {
        method_map[m.name] = std::move(m);
    }

    if (describe_enabled_) {
        register_describe(method_map, protocol_name_, server_id);
    }

    return std::unique_ptr<Server>(new Server(std::move(method_map), std::move(server_id)));
}

// Server

Server::Server(std::unordered_map<std::string, MethodInfo> methods,
               std::string server_id)
    : methods_(std::move(methods))
    , server_id_(std::move(server_id)) {}

}  // namespace vgi_rpc
