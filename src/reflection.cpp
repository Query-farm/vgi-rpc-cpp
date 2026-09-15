// vgi_rpc.Reflection.v1 -- discovery as an ordinary co-hosted protocol.
//
// Introspection used to be a hardcoded method name, `__describe__`, answered
// from a pre-built batch before dispatch. That made it a thing every port had to
// hand-implement, in a bespoke format, outside the machinery that serves every
// other method -- which is how the ports drifted. Here it is a protocol like any
// other, addressed by the same routing key.
//
// Following gRPC's reflection service and D-Bus's org.freedesktop.DBus, it is
// co-hosted rather than special-cased. Its own major version sits in its name,
// so an incompatible reflection is a routing failure a client can act on rather
// than a mis-parse.
//
// Exempt from the protocol_version gate: this is the protocol a
// version-mismatched client calls to learn *what* mismatched, and gating it
// would deny the client the diagnosis it came for.

#include "vgi_rpc/reflection.h"

#include <algorithm>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include "vgi_rpc/protocol_hash.h"

namespace vgi_rpc {
namespace {

std::shared_ptr<arrow::Field> Utf8Field(const std::string& name) {
  return arrow::field(name, arrow::utf8(), /*nullable=*/false);
}

std::shared_ptr<arrow::Field> BoolField(const std::string& name) {
  return arrow::field(name, arrow::boolean(), /*nullable=*/false);
}

std::shared_ptr<arrow::Field> BinaryField(const std::string& name) {
  return arrow::field(name, arrow::binary(), /*nullable=*/false);
}

/// The ProtocolSummary struct fields, mirroring the reference field for field.
arrow::FieldVector ProtocolSummaryFields() {
  return {Utf8Field("protocol"),
          Utf8Field("protocol_version"),
          Utf8Field("protocol_hash"),
          BoolField("deprecated"),
          Utf8Field("deprecation_message"),
          arrow::field("features",
                       arrow::list(arrow::field("item", arrow::utf8(), /*nullable=*/true)),
                       /*nullable=*/false)};
}

/// The MethodInfo struct fields.
arrow::FieldVector MethodInfoFields() {
  return {Utf8Field("name"),
          Utf8Field("method_type"),
          BoolField("has_return"),
          BoolField("has_header"),
          Utf8Field("stream_kind"),
          BinaryField("params_schema_ipc"),
          BinaryField("result_schema_ipc"),
          BinaryField("header_schema_ipc"),
          Utf8Field("idempotency"),
          BoolField("deprecated"),
          Utf8Field("deprecation_message")};
}

/// Serialize a schema as a complete IPC stream, or empty bytes when absent.
///
/// Empty rather than null: a nullable column costs every port a null check on a
/// value it will only ever treat as absent.
arrow::Result<std::string> SchemaIpc(const std::shared_ptr<arrow::Schema>& schema) {
  if (schema == nullptr) return std::string();
  ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::ipc::SerializeSchema(*schema));
  return std::string(reinterpret_cast<const char*>(buffer->data()),
                     static_cast<size_t>(buffer->size()));
}

/// Serialize one batch as a complete IPC stream.
arrow::Result<std::string> BatchToIpc(const std::shared_ptr<arrow::RecordBatch>& batch) {
  ARROW_ASSIGN_OR_RAISE(auto sink, arrow::io::BufferOutputStream::Create());
  ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeStreamWriter(sink, batch->schema()));
  ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
  ARROW_RETURN_NOT_OK(writer->Close());
  ARROW_ASSIGN_OR_RAISE(auto buffer, sink->Finish());
  return std::string(reinterpret_cast<const char*>(buffer->data()),
                     static_cast<size_t>(buffer->size()));
}

/// A one-row string array.
std::shared_ptr<arrow::Array> OneString(const std::string& v) {
  arrow::StringBuilder b;
  ARROW_UNUSED(b.Append(v));
  std::shared_ptr<arrow::Array> out;
  ARROW_UNUSED(b.Finish(&out));
  return out;
}

/// A one-row boolean array.
std::shared_ptr<arrow::Array> OneBool(bool v) {
  arrow::BooleanBuilder b;
  ARROW_UNUSED(b.Append(v));
  std::shared_ptr<arrow::Array> out;
  ARROW_UNUSED(b.Finish(&out));
  return out;
}

/// A one-row list holding every element of `values`.
arrow::Result<std::shared_ptr<arrow::Array>> OneRowList(
    const std::shared_ptr<arrow::StructArray>& values, const arrow::FieldVector& fields) {
  arrow::Int32Builder offsets;
  ARROW_RETURN_NOT_OK(offsets.Append(0));
  ARROW_RETURN_NOT_OK(offsets.Append(static_cast<int32_t>(values->length())));
  ARROW_ASSIGN_OR_RAISE(auto offset_array, offsets.Finish());
  auto item = arrow::field("item", arrow::struct_(fields), /*nullable=*/true);
  return std::make_shared<arrow::ListArray>(
      arrow::list(item), 1, std::static_pointer_cast<arrow::Int32Array>(offset_array)->values(),
      values);
}

/// A one-row list of strings, empty.
arrow::Result<std::shared_ptr<arrow::Array>> EmptyStringList() {
  arrow::ListBuilder b(arrow::default_memory_pool(), std::make_shared<arrow::StringBuilder>(),
                       arrow::list(arrow::field("item", arrow::utf8(), true)));
  ARROW_RETURN_NOT_OK(b.Append());
  std::shared_ptr<arrow::Array> out;
  ARROW_RETURN_NOT_OK(b.Finish(&out));
  return out;
}

/// Assemble a one-row batch from already-built columns.
arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeOneRowBatch(
    const std::shared_ptr<arrow::Schema>& schema, arrow::ArrayVector columns) {
  return arrow::RecordBatch::Make(schema, 1, std::move(columns));
}

}  // namespace

/// Whether a method belongs to the application protocol rather than the framework.
///
/// `__transport_options__` is registered in this port's method table like any
/// other handler, but it is *server-level* surface -- a reserved name that
/// belongs to no protocol. Describing it as part of the application protocol
/// would make this port's description, and therefore its hash, disagree with
/// every other port, none of which has it in a protocol's method table at all.

bool IsApplicationMethod(const std::string& name) {
  return name.rfind("__", 0) != 0;
}

bool UnaryHasReturn(const MethodInfo& info) {
  // A stream's result schema is the (empty) protocol-level return, not something
  // the caller receives, and a void unary method carries no fields -- so neither
  // the null check nor the method type alone is the question being asked.
  return info.method_type == MethodType::UNARY && info.has_return &&
         info.result_schema != nullptr && info.result_schema->num_fields() > 0;
}

std::string StreamKindFor(const MethodInfo& info) {
  if (info.method_type == MethodType::UNARY) return "";
  return info.is_exchange ? "exchange" : "producer";
}

arrow::Result<std::string> BindingHash(
    const std::string& name, const std::unordered_map<std::string, MethodInfo>& methods) {
  std::vector<HashMethod> entries;
  entries.reserve(methods.size());
  for (const auto& [_, info] : methods) {
    if (!IsApplicationMethod(info.name)) continue;
    entries.push_back(HashMethod{info.name,
                                 info.method_type == MethodType::UNARY ? "unary" : "stream",
                                 UnaryHasReturn(info),
                                 info.header_schema != nullptr,
                                 info.params_schema,
                                 info.result_schema,
                                 info.header_schema});
  }
  return ComputeProtocolHash(name, std::move(entries));
}

arrow::Result<std::string> BuildProtocolList(const std::string& server_id,
                                             const std::string& server_version,
                                             const std::string& request_version,
                                             const std::vector<ProtocolSummary>& protocols) {
  auto summary_fields = ProtocolSummaryFields();
  auto schema = arrow::schema(
      {Utf8Field("server_id"), Utf8Field("server_version"), Utf8Field("request_version"),
       arrow::field("protocols",
                    arrow::list(arrow::field("item", arrow::struct_(summary_fields),
                                             /*nullable=*/true)),
                    /*nullable=*/false)});

  arrow::StringBuilder protocol_b, version_b, hash_b, deprecation_b;
  arrow::BooleanBuilder deprecated_b;
  auto features_b = std::make_shared<arrow::ListBuilder>(
      arrow::default_memory_pool(), std::make_shared<arrow::StringBuilder>(),
      arrow::list(arrow::field("item", arrow::utf8(), true)));
  for (const auto& p : protocols) {
    ARROW_RETURN_NOT_OK(protocol_b.Append(p.protocol));
    ARROW_RETURN_NOT_OK(version_b.Append(p.version));
    ARROW_RETURN_NOT_OK(hash_b.Append(p.hash));
    ARROW_RETURN_NOT_OK(deprecated_b.Append(false));
    ARROW_RETURN_NOT_OK(deprecation_b.Append(""));
    // An empty but present feature list: additive capabilities announce here
    // rather than consuming version numbers.
    ARROW_RETURN_NOT_OK(features_b->Append());
  }
  ARROW_ASSIGN_OR_RAISE(auto protocol_a, protocol_b.Finish());
  ARROW_ASSIGN_OR_RAISE(auto version_a, version_b.Finish());
  ARROW_ASSIGN_OR_RAISE(auto hash_a, hash_b.Finish());
  ARROW_ASSIGN_OR_RAISE(auto deprecated_a, deprecated_b.Finish());
  ARROW_ASSIGN_OR_RAISE(auto deprecation_a, deprecation_b.Finish());
  ARROW_ASSIGN_OR_RAISE(auto features_a, features_b->Finish());

  auto summary_struct = std::make_shared<arrow::StructArray>(
      arrow::struct_(summary_fields), static_cast<int64_t>(protocols.size()),
      arrow::ArrayVector{protocol_a, version_a, hash_a, deprecated_a, deprecation_a, features_a});

  ARROW_ASSIGN_OR_RAISE(auto protocols_list, OneRowList(summary_struct, summary_fields));
  ARROW_ASSIGN_OR_RAISE(auto batch,
                        MakeOneRowBatch(schema, {OneString(server_id), OneString(server_version),
                                                 OneString(request_version), protocols_list}));
  return BatchToIpc(batch);
}

arrow::Result<std::string> BuildServiceDescription(
    const std::string& protocol, const std::string& version, const std::string& hash,
    const std::unordered_map<std::string, MethodInfo>& methods) {
  auto summary_fields = ProtocolSummaryFields();
  auto method_fields = MethodInfoFields();
  arrow::FieldVector schema_fields = summary_fields;
  schema_fields.push_back(arrow::field(
      "methods",
      arrow::list(arrow::field("item", arrow::struct_(method_fields), /*nullable=*/true)),
      /*nullable=*/false));
  auto schema = arrow::schema(schema_fields);

  // Sorted so two ports iterating differently-ordered maps still agree.
  std::vector<const MethodInfo*> ordered;
  ordered.reserve(methods.size());
  for (const auto& [_, info] : methods) {
    if (!IsApplicationMethod(info.name)) continue;
    ordered.push_back(&info);
  }
  std::sort(ordered.begin(), ordered.end(),
            [](const MethodInfo* a, const MethodInfo* b) { return a->name < b->name; });

  arrow::StringBuilder name_b, type_b, kind_b, idem_b, depmsg_b;
  arrow::BooleanBuilder hasret_b, hashdr_b, dep_b;
  arrow::BinaryBuilder params_b, result_b, header_b;
  for (const MethodInfo* info : ordered) {
    ARROW_RETURN_NOT_OK(name_b.Append(info->name));
    ARROW_RETURN_NOT_OK(
        type_b.Append(info->method_type == MethodType::UNARY ? "unary" : "stream"));
    ARROW_RETURN_NOT_OK(hasret_b.Append(UnaryHasReturn(*info)));
    ARROW_RETURN_NOT_OK(hashdr_b.Append(info->header_schema != nullptr));
    ARROW_RETURN_NOT_OK(kind_b.Append(StreamKindFor(*info)));
    ARROW_ASSIGN_OR_RAISE(auto params_ipc, SchemaIpc(info->params_schema));
    ARROW_RETURN_NOT_OK(params_b.Append(params_ipc));
    ARROW_ASSIGN_OR_RAISE(auto result_ipc,
                          UnaryHasReturn(*info) ? SchemaIpc(info->result_schema)
                                                : arrow::Result<std::string>(std::string()));
    ARROW_RETURN_NOT_OK(result_b.Append(result_ipc));
    ARROW_ASSIGN_OR_RAISE(auto header_ipc, SchemaIpc(info->header_schema));
    ARROW_RETURN_NOT_OK(header_b.Append(header_ipc));
    ARROW_RETURN_NOT_OK(idem_b.Append("unknown"));
    ARROW_RETURN_NOT_OK(dep_b.Append(false));
    ARROW_RETURN_NOT_OK(depmsg_b.Append(""));
  }

  arrow::ArrayVector method_children;
  for (auto* b : std::vector<arrow::ArrayBuilder*>{&name_b, &type_b, &hasret_b, &hashdr_b, &kind_b,
                                                   &params_b, &result_b, &header_b, &idem_b,
                                                   &dep_b, &depmsg_b}) {
    ARROW_ASSIGN_OR_RAISE(auto arr, b->Finish());
    method_children.push_back(arr);
  }
  auto method_struct = std::make_shared<arrow::StructArray>(
      arrow::struct_(method_fields), static_cast<int64_t>(ordered.size()), method_children);
  ARROW_ASSIGN_OR_RAISE(auto methods_list, OneRowList(method_struct, method_fields));

  ARROW_ASSIGN_OR_RAISE(auto empty_features, EmptyStringList());
  ARROW_ASSIGN_OR_RAISE(
      auto batch,
      MakeOneRowBatch(schema, {OneString(protocol), OneString(version), OneString(hash),
                               OneBool(false), OneString(""), empty_features, methods_list}));
  return BatchToIpc(batch);
}

}  // namespace vgi_rpc
