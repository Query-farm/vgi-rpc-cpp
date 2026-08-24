// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// HTTP transport for the vgi-rpc server (cpp-httplib).  Maps the pipe-based
// wire protocol onto stateless HTTP request/response pairs per
// docs/WIRE_PROTOCOL.md §10, and carries the optional HTTP-only features:
// capability discovery, CORS, standardized 401s, proxy proof, token
// introspection, and sticky sessions.
//
// Stream state is held server-side in a token-keyed registry, which is what
// keeps the C++ stream-state objects — live handles, not serializable values —
// alive across the separate HTTP requests of one stream.  The token itself is
// AEAD-sealed and bound to the worker and the caller's identity, so it is
// opaque to the client and useless at any other worker.  The registries have
// short metadata locks and each live state has its own dispatch lock: one
// stream/session remains lock-step without serializing unrelated RPCs.

#include "vgi_rpc/server.h"
#include "vgi_rpc/arrow_utils.h"
#include "vgi_rpc/crypto.h"
#include "vgi_rpc/errors.h"
#include "vgi_rpc/external.h"
#include "vgi_rpc/log_sink.h"
#include "vgi_rpc/metadata.h"
#include "vgi_rpc/output_collector.h"
#include "vgi_rpc/proxy_proof.h"
#include "vgi_rpc/session.h"
#include "vgi_rpc/wire.h"
#include "request_contract.h"

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/compute/cast.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>

#include <httplib.h>
#include <nlohmann/json.hpp>
#include <zlib.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <ctime>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

namespace vgi_rpc {

namespace {

constexpr const char* ARROW_CONTENT_TYPE = "application/vnd.apache.arrow.stream";
constexpr const char* REQUEST_ID_HEADER = "X-Request-ID";
constexpr const char* RPC_ERROR_HEADER = "X-VGI-RPC-Error";
constexpr const char* AUTH_REASON_HEADER = "VGI-Auth-Reason";
constexpr const char* AUTH_PROXY_REQUIRED_HEADER = "VGI-Auth-Proxy-Required";
constexpr const char* PROOF_HEADER = "VGI-Proxy-Proof";
constexpr const char* PROOF_REQUIRED_HEADER = "VGI-Proxy-Proof-Required";
constexpr const char* SESSION_HEADER = "VGI-Session";
constexpr const char* SESSION_ACCEPT_HEADER = "VGI-Session-Accept";
constexpr const char* SESSION_CLOSE_HEADER = "VGI-Session-Close";
constexpr const char* PRINCIPAL_HEADER = "X-Conformance-Principal";
constexpr const char* AUTH_REASON_REQUEST_HEADER = "X-Conformance-Auth-Reason";

// Fixture constants for the token-introspection group.  The shared suite posts
// these exact values, so they are part of the endpoint's test contract.
constexpr const char* INTROSPECTOR_PRINCIPAL = "conformance-introspector";
constexpr const char* SUBJECT_TOKEN = "conformance-opaque-subject-token";
constexpr const char* SUBJECT_PRINCIPAL = "subject@conformance.example";
constexpr const char* UNAVAILABLE_TOKEN = "conformance-unavailable-token";

// Live stream held across the separate HTTP requests of one stream call.
struct HttpStreamSession {
    std::mutex dispatch_mutex;
    std::shared_ptr<StreamState> state;
    std::shared_ptr<arrow::Schema> output_schema;
    std::shared_ptr<arrow::Schema> input_schema;
    bool is_exchange = false;
    std::string method_name;
    std::string aad;  // identity this stream was opened under
};

std::string buffer_to_string(const std::shared_ptr<arrow::Buffer>& buf) {
    return std::string(reinterpret_cast<const char*>(buf->data()),
                       static_cast<size_t>(buf->size()));
}

template <typename Fn>
std::string build_body(Fn&& fn) {
    auto out = unwrap(arrow::io::BufferOutputStream::Create());
    fn(out);
    return buffer_to_string(unwrap(out->Finish()));
}

std::string error_body(const std::shared_ptr<arrow::Schema>& schema,
                       const std::string& exception_type, const std::string& message,
                       const std::string& server_id, const std::string& request_id,
                       const std::string& error_kind = "") {
    return build_body([&](const std::shared_ptr<arrow::io::OutputStream>& out) {
        auto err =
            Result::error(schema, exception_type, message, server_id, request_id, error_kind);
        write_ipc_stream(out, schema, {err.annotated_batch()});
    });
}

// Same name-set-strict / order+cast-tolerant coercion the pipe transport uses.
std::shared_ptr<arrow::RecordBatch> coerce_input(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                 const std::shared_ptr<arrow::Schema>& target) {
    if (batch->schema()->Equals(*target)) return batch;
    auto mismatch = [&]() {
        return std::logic_error("Input schema mismatch: expected " + target->ToString() + ", got " +
                                batch->schema()->ToString());
    };
    std::set<std::string> bn, tn;
    for (const auto& f : batch->schema()->fields()) bn.insert(f->name());
    for (const auto& f : target->fields()) tn.insert(f->name());
    if (bn != tn) throw mismatch();
    std::vector<std::shared_ptr<arrow::Array>> cols;
    for (const auto& f : target->fields()) {
        auto col = batch->GetColumnByName(f->name());
        if (!col) throw mismatch();
        if (!col->type()->Equals(*f->type())) {
            auto cast_res = arrow::compute::Cast(*col, f->type());
            if (!cast_res.ok()) throw mismatch();
            col = cast_res.ValueUnsafe();
        }
        cols.push_back(std::move(col));
    }
    return arrow::RecordBatch::Make(target, batch->num_rows(), std::move(cols));
}

std::shared_ptr<arrow::KeyValueMetadata> cursor_metadata(const std::string& cursor) {
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(keys::STATE_B64, cursor);
    return md;
}

// The sentinel an /init response carries: both tokens on the one zero-row
// batch.  Only /init mints the call token; a continuation re-mints the cursor
// alone, which is the whole point of splitting them.
std::shared_ptr<arrow::KeyValueMetadata> init_metadata(const std::string& cursor,
                                                       const std::string& call_token) {
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(keys::STATE_B64, cursor);
    md->Append(keys::CALL_STATE_B64, call_token);
    return md;
}

// Drive exactly one producer transition into `writer` for one HTTP tick.
// Returns true when the stream is terminal (finished or errored — no token
// needed), false when the caller should append a continuation token.
// Given one output cycle, either return the pointer metadata that replaces it
// or nullptr to write the batches inline.
using CycleExternalizer = std::function<std::shared_ptr<arrow::KeyValueMetadata>(
    const std::vector<AnnotatedBatch>&, const std::shared_ptr<arrow::Schema>&)>;

bool run_producer_turns(const std::shared_ptr<arrow::ipc::RecordBatchWriter>& writer,
                        const std::shared_ptr<StreamState>& state,
                        const std::shared_ptr<arrow::Schema>& schema, CallContext& ctx,
                        const std::string& server_id, const std::string& request_id, bool* errored,
                        const CycleExternalizer& externalize, const AnnotatedBatch& input) {
    OutputCollector oc(schema, /*producer=*/true, server_id, request_id);
    try {
        state->process(input, oc, ctx);
    } catch (const std::exception& e) {
        auto md = make_error_metadata(exception_type_of(e), e.what(), server_id, request_id,
                                      error_kind_of(e));
        VGI_RPC_THROW_NOT_OK(writer->WriteRecordBatch(*make_empty_batch(schema), md));
        if (errored) *errored = true;
        return true;
    }
    // The pointer replaces the *whole* transition — log batches included —
    // so the client reads them back out of the fetched stream in order.
    std::shared_ptr<arrow::KeyValueMetadata> pointer;
    if (externalize) pointer = externalize(oc.batches(), schema);
    if (pointer) {
        VGI_RPC_THROW_NOT_OK(writer->WriteRecordBatch(*make_empty_batch(schema), pointer));
    } else {
        for (const auto& ab : oc.batches()) {
            VGI_RPC_THROW_NOT_OK(ab.custom_metadata
                                     ? writer->WriteRecordBatch(*ab.batch, ab.custom_metadata)
                                     : writer->WriteRecordBatch(*ab.batch));
        }
    }
    return oc.is_finished();
}

// --- Response codec negotiation -------------------------------------------

// Split a comma-separated accept list into lowercase tokens, dropping any
// q-value.  Order is preserved: it is the client's preference order.
std::vector<std::string> parse_accept_encoding(const std::string& raw) {
    std::vector<std::string> out;
    size_t start = 0;
    while (start <= raw.size()) {
        size_t comma = raw.find(',', start);
        if (comma == std::string::npos) comma = raw.size();
        std::string token = raw.substr(start, comma - start);
        start = comma + 1;
        if (const size_t semi = token.find(';'); semi != std::string::npos) {
            token = token.substr(0, semi);
        }
        const size_t b = token.find_first_not_of(" \t");
        const size_t e = token.find_last_not_of(" \t");
        if (b == std::string::npos) continue;
        token = token.substr(b, e - b + 1);
        std::transform(token.begin(), token.end(), token.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        if (!token.empty()) out.push_back(token);
    }
    return out;
}

bool accepts(const std::vector<std::string>& list, const std::string& codec) {
    return std::find(list.begin(), list.end(), codec) != list.end();
}

// Which codec to answer with, and on which header to stamp it.
struct CodecChoice {
    std::string codec;  // empty = send identity
    bool standard_header = true;
};

// X-VGI-Accept-Encoding outranks the generic Accept-Encoding.  cpp-httplib
// injects "deflate, gzip, br, zstd" — gzip first — and honouring that order
// instead of VGI's cost a 4.2x slower round trip, which is the whole reason
// the custom header exists.
CodecChoice choose_codec(const httplib::Request& req, bool compression_enabled) {
    CodecChoice choice;
    if (!compression_enabled) return choice;

    const bool has_vgi = req.has_header("X-VGI-Accept-Encoding");
    const auto vgi = parse_accept_encoding(req.get_header_value("X-VGI-Accept-Encoding"));
    const auto standard = parse_accept_encoding(req.get_header_value("Accept-Encoding"));

    // `identity` is a first-class token, not the absence of a preference: a
    // client may explicitly demand an uncompressed body.
    std::vector<std::string> preferred = has_vgi ? vgi : standard;
    if (has_vgi) {
        for (const auto& token : standard) {
            if (!accepts(preferred, token)) preferred.push_back(token);
        }
    }
    for (const auto& token : preferred) {
        if (token == "identity") return choice;
        if (token != "zstd" && token != "gzip") continue;
        choice.codec = token;
        // A codec reachable only through the custom header goes on
        // X-VGI-Content-Encoding: such a client's fetch layer would
        // auto-decode or mangle a standard Content-Encoding it never asked
        // for.
        choice.standard_header = accepts(standard, token);
        return choice;
    }
    return choice;
}

std::optional<std::string> gzip_compress(const std::string& body) {
    z_stream stream{};
    if (deflateInit2(&stream, Z_DEFAULT_COMPRESSION, Z_DEFLATED, MAX_WBITS + 16, 8,
                     Z_DEFAULT_STRATEGY) != Z_OK) {
        return std::nullopt;
    }

    std::string compressed;
    compressed.reserve(std::min<size_t>(body.size(), 64 * 1024));
    std::array<unsigned char, 64 * 1024> chunk{};
    size_t offset = 0;
    int status = Z_OK;
    do {
        const size_t remaining = body.size() - offset;
        const auto input_size =
            static_cast<uInt>(std::min<size_t>(remaining, std::numeric_limits<uInt>::max()));
        stream.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(body.data() + offset));
        stream.avail_in = input_size;
        offset += input_size;
        const int flush = offset == body.size() ? Z_FINISH : Z_NO_FLUSH;

        do {
            stream.next_out = chunk.data();
            stream.avail_out = static_cast<uInt>(chunk.size());
            status = deflate(&stream, flush);
            if (status != Z_OK && status != Z_STREAM_END) {
                deflateEnd(&stream);
                return std::nullopt;
            }
            compressed.append(reinterpret_cast<const char*>(chunk.data()),
                              chunk.size() - stream.avail_out);
        } while (stream.avail_in > 0 || (flush == Z_FINISH && status != Z_STREAM_END));
    } while (status != Z_STREAM_END);

    deflateEnd(&stream);
    return compressed;
}

// --- Worker threads -------------------------------------------------------

// --- CORS -----------------------------------------------------------------

// Request headers a browser must be permitted to send.  A server that allows
// only Content-Type serves plain calls perfectly while silently blocking
// sticky sessions and proxy proof, which is why these are enumerated.
constexpr const char* kAllowedRequestHeaders =
    "content-type, accept, accept-encoding, authorization, x-request-id, "
    "x-vgi-accept-encoding, vgi-session, vgi-session-accept, vgi-proxy-proof, "
    "x-conformance-principal, x-conformance-auth-reason";

// Headers that ride failure and session responses, which OPTIONS /health
// never advertises.  A check derived from advertisements structurally cannot
// reach them, so they are named — each is load-bearing for a browser client
// and silent when missing.
constexpr const char* kAlwaysExposedHeaders =
    "x-vgi-rpc-error, vgi-auth-reason, vgi-auth-proxy-required, x-request-id, "
    "x-vgi-content-encoding, www-authenticate, vgi-session, vgi-session-close";

}  // namespace

const char* auth_reason_name(AuthReason reason) {
    switch (reason) {
        case AuthReason::NONE: return "";
        case AuthReason::MISSING_CREDENTIAL: return "missing_credential";
        case AuthReason::INVALID_CREDENTIAL: return "invalid_credential";
        case AuthReason::EXPIRED_CREDENTIAL: return "expired_credential";
        case AuthReason::INSUFFICIENT_SCOPE: return "insufficient_scope";
        case AuthReason::PROXY_REQUIRED: return "proxy_required";
        case AuthReason::UNAUTHORIZED: return "unauthorized";
    }
    return "unauthorized";
}

std::optional<AuthReason> auth_reason_from_name(const std::string& name) {
    if (name == "missing_credential") return AuthReason::MISSING_CREDENTIAL;
    if (name == "invalid_credential") return AuthReason::INVALID_CREDENTIAL;
    if (name == "expired_credential") return AuthReason::EXPIRED_CREDENTIAL;
    if (name == "insufficient_scope") return AuthReason::INSUFFICIENT_SCOPE;
    if (name == "unauthorized") return AuthReason::UNAUTHORIZED;
    // proxy_required is deliberately not resolvable from a request: §5 derives
    // it from server configuration, and letting a caller summon it would
    // advertise a proxy dependency that does not exist.
    return std::nullopt;
}

// ---------------------------------------------------------------------------
// The server
// ---------------------------------------------------------------------------

// All per-connection state and helpers for one running HTTP server.  A struct
// rather than a pile of lambda captures: the handlers share a dozen pieces of
// configuration and three registries, and threading those through captures was
// what made the previous version hard to extend.
class HttpServer {
public:
    HttpServer(Server& rpc, const HttpConfig& cfg)
        : rpc_(rpc),
          cfg_(cfg),
          sessions_(cfg.token_key, rpc.server_id(), cfg.sticky_default_ttl),
          proof_(cfg.proof_mode, cfg.proof_origin_id, cfg.proof_secrets, cfg.proof_skew_seconds,
                 cfg.proof_replay_cache) {
        if (!cfg.external_storage_url.empty()) {
            // Throws for a scheme this build cannot serve, so an operator who
            // configured a bucket learns at startup rather than on the first
            // payload large enough to externalise.
            ExternalStorageConfig storage_config;
            storage_config.uri = cfg.external_storage_url;
            storage_config.signed_url_ttl_seconds = cfg.signed_url_ttl_seconds;
            storage_config.region = cfg.external_storage_region;
            storage_config.endpoint_url = cfg.external_storage_endpoint;
            storage_config.signing_account = cfg.external_storage_signing_account;
            storage_config.max_fetch_bytes = cfg.max_fetch_bytes;
            storage_config.max_decompressed_bytes = cfg.max_decompressed_fetch_bytes;
            storage_config.max_redirects = cfg.max_external_redirects;
            storage_config.url_validator = cfg.external_url_validator;
            storage_ = make_external_storage(storage_config);
        }
    }

    void run();

private:
    // --- helpers ---
    bool proof_required() const noexcept { return cfg_.proof_mode == ProofMode::REQUIRE; }
    bool externalization() const noexcept { return !cfg_.external_storage_url.empty(); }

    void stamp_common(const httplib::Request& req, httplib::Response& res,
                      const std::string& request_id) const;
    // Set an Arrow-IPC response body, compressing it when the caller asked for
    // a codec we can produce.  Every RPC response goes through here so the
    // negotiation happens in exactly one place.
    void set_arrow_content(const httplib::Request& req, httplib::Response& res,
                           std::string body) const;
    void stamp_capabilities(httplib::Response& res) const;
    void stamp_cors(const httplib::Request& req, httplib::Response& res) const;

    // Strip the configured prefix, if present, from a request path.  Both the
    // prefixed and bare forms are served: the prefix is operator configuration
    // rather than wire contract, and a client pointed at either must work.
    std::string strip_prefix(const std::string& path) const;

    AuthIdentity identify(const httplib::Request& req) const;
    // Returns true when the request was refused; `res` is then complete.
    bool refuse_if_unauthorized(const httplib::Request& req, httplib::Response& res,
                                const std::string& request_id);
    void write_unauthorized(const httplib::Request& req, httplib::Response& res,
                            AuthReason reason) const;

    void handle_health(const httplib::Request& req, httplib::Response& res) const;
    void handle_introspect(const httplib::Request& req, httplib::Response& res,
                           const std::string& request_body);
    void handle_session_delete(const httplib::Request& req, httplib::Response& res);
    void handle_rpc(const httplib::Request& req, httplib::Response& res,
                    const std::string& request_body);

    // Externalize an output cycle when its data batch is over the threshold:
    // upload the whole IPC stream and return a body carrying one zero-row
    // pointer batch in its place.  Returns the original body unchanged when
    // there is no backend, no threshold breach, or nothing to point at.
    std::string maybe_externalize(const std::string& body,
                                  const std::shared_ptr<arrow::Schema>& schema,
                                  int64_t* externalized_bytes) const;

    // Externalize one stream output cycle, returning the pointer metadata that
    // replaces it, or nullptr to leave it inline.
    std::shared_ptr<arrow::KeyValueMetadata> externalize_cycle(
        const std::vector<AnnotatedBatch>& batches, const std::shared_ptr<arrow::Schema>& schema,
        int64_t* externalized_bytes) const;

    // Replace a request pointer batch with the batch it points at.
    std::optional<IpcStreamContents> resolve_request_pointer(const AnnotatedBatch& first) const;

    Server& rpc_;
    HttpConfig cfg_;
    SessionRegistry sessions_;
    ProofVerifier proof_;
    std::unique_ptr<ExternalStorage> storage_;

    std::mutex streams_mutex_;
    std::unordered_map<std::string, std::shared_ptr<HttpStreamSession>> streams_;
};

std::string HttpServer::strip_prefix(const std::string& path) const {
    if (!cfg_.prefix.empty() && path.rfind(cfg_.prefix, 0) == 0) {
        std::string rest = path.substr(cfg_.prefix.size());
        if (rest.empty()) return "/";
        if (rest[0] == '/') return rest;
        // "/vgifoo" is not under the "/vgi" prefix; leave it alone.
    }
    return path;
}

void HttpServer::stamp_capabilities(httplib::Response& res) const {
    if (cfg_.max_response_bytes >= 0) {
        res.set_header("VGI-Max-Response-Bytes", std::to_string(cfg_.max_response_bytes));
    }
    if (cfg_.max_externalized_response_bytes >= 0) {
        res.set_header("VGI-Max-Externalized-Response-Bytes",
                       std::to_string(cfg_.max_externalized_response_bytes));
    }
    if (cfg_.max_request_bytes >= 0) {
        res.set_header("VGI-Max-Request-Bytes", std::to_string(cfg_.max_request_bytes));
    }
    // Always present: an absent header means "unknown", and the client needs
    // to know whether to expect pointer batches at all.
    res.set_header("VGI-Externalization-Enabled", externalization() ? "true" : "false");
    if (externalization()) {
        res.set_header("VGI-Upload-URL-Support", "true");
        // The threshold doubles as the inline-request ceiling: a body over it
        // is what the client externalizes instead, so advertising one number
        // keeps the two sides from disagreeing about where the line is.
        if (cfg_.max_request_bytes < 0) {
            res.set_header("VGI-Max-Request-Bytes", std::to_string(cfg_.externalize_threshold));
        }
        res.set_header("VGI-Max-Upload-Bytes",
                       std::to_string(cfg_.max_externalized_response_bytes >= 0
                                          ? cfg_.max_externalized_response_bytes
                                          : int64_t{1} << 31));
    }
    // Present-but-empty is a server positively stating it speaks no
    // compression; absent would mean a server predating the header, for which
    // a client assumes zstd.  The two are not interchangeable.
    res.set_header("VGI-Supported-Encodings", cfg_.compression ? "zstd, gzip" : "");
    if (cfg_.sticky) {
        res.set_header("VGI-Sticky-Enabled", "true");
        res.set_header("VGI-Sticky-Default-TTL", std::to_string(cfg_.sticky_default_ttl));
        if (!cfg_.sticky_echo_headers.empty()) {
            std::string names;
            for (const auto& [name, _] : cfg_.sticky_echo_headers) {
                if (!names.empty()) names += ", ";
                names += name;
            }
            res.set_header("VGI-Sticky-Echo-Headers", names);
        }
    }
    if (cfg_.token_introspection) {
        res.set_header("VGI-Token-Introspection", "true");
    }
    // Only in require mode — never as "false" in off or allow, which readers
    // would have to special-case.
    if (proof_required()) {
        res.set_header(PROOF_REQUIRED_HEADER, "true");
    }
}

void HttpServer::stamp_cors(const httplib::Request& req, httplib::Response& res) const {
    if (cfg_.cors_origin.empty()) return;  // off means no headers at all
    const std::string origin = req.get_header_value("Origin");
    if (cfg_.cors_origin != "*" && !origin.empty() && origin != cfg_.cors_origin) return;

    res.set_header("Access-Control-Allow-Origin", cfg_.cors_origin == "*" ? "*" : cfg_.cors_origin);
    res.set_header("Vary", "Origin");
    res.set_header("Access-Control-Allow-Methods", "GET, HEAD, POST, DELETE, OPTIONS");
    // Echo what the browser asked for when it asked, so a header this server
    // has not heard of still reaches a method that has.
    const std::string requested = req.get_header_value("Access-Control-Request-Headers");
    res.set_header("Access-Control-Allow-Headers", requested.empty()
                                                       ? kAllowedRequestHeaders
                                                       : requested + ", " + kAllowedRequestHeaders);
    // Derived from the capability headers already stamped on this response
    // rather than restated: whatever this server advertises, a browser can
    // read.  A hand-maintained list is a list that drifts, and the failure is
    // invisible to every test that does not drive a browser.
    std::string expose = kAlwaysExposedHeaders;
    for (const auto& [name, _] : res.headers) {
        std::string lower = name;
        std::transform(lower.begin(), lower.end(), lower.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        if (lower.rfind("vgi-", 0) == 0 || lower.rfind("x-vgi-", 0) == 0) {
            if (expose.find(lower) == std::string::npos) expose += ", " + lower;
        }
    }
    res.set_header("Access-Control-Expose-Headers", expose);
    res.set_header("Access-Control-Max-Age", "600");
    // A caller under COEP require-corp has this response blocked without it,
    // and the server sees a perfectly ordinary success — so it fails silently
    // from the operator's side.
    res.set_header("Cross-Origin-Resource-Policy", "cross-origin");
}

void HttpServer::stamp_common(const httplib::Request& req, httplib::Response& res,
                              const std::string& request_id) const {
    if (!request_id.empty()) res.set_header(REQUEST_ID_HEADER, request_id);
    stamp_capabilities(res);
    stamp_cors(req, res);
}

void HttpServer::set_arrow_content(const httplib::Request& req, httplib::Response& res,
                                   std::string body) const {
    const CodecChoice choice = choose_codec(req, cfg_.compression);
    bool compressed_ok = false;
    if (choice.codec == "zstd" && !body.empty()) {
        const size_t bound = ZSTD_compressBound(body.size());
        std::string compressed(bound, '\0');
        const size_t written = ZSTD_compress(compressed.data(), bound, body.data(), body.size(), 3);
        if (!ZSTD_isError(written)) {
            compressed.resize(written);
            body.swap(compressed);
            compressed_ok = true;
        }
    } else if (choice.codec == "gzip" && !body.empty()) {
        if (auto compressed = gzip_compress(body)) {
            body = std::move(*compressed);
            compressed_ok = true;
        }
    }
    if (compressed_ok) {
        res.set_header(choice.standard_header ? "Content-Encoding" : "X-VGI-Content-Encoding",
                       choice.codec);
    }
    // A compression failure is not a request failure: send the body
    // uncompressed rather than turning a slow response into a broken one.
    // httplib does not auto-compress this content type, so the encoding
    // headers set above are the whole story.
    res.set_content(std::move(body), ARROW_CONTENT_TYPE);
}

// ---------------------------------------------------------------------------
// External locations
// ---------------------------------------------------------------------------

std::string HttpServer::maybe_externalize(const std::string& body,
                                          const std::shared_ptr<arrow::Schema>& schema,
                                          int64_t* externalized_bytes) const {
    if (!storage_ || cfg_.externalize_threshold < 0) return body;
    if (static_cast<int64_t>(body.size()) < cfg_.externalize_threshold) return body;

    // The digest covers the payload *before* compression, so a reader that
    // decompresses and then verifies is checking the same bytes the writer
    // hashed.  Sent optionally, but a reader that sees it must verify.
    auto digest = crypto::sha256(reinterpret_cast<const uint8_t*>(body.data()), body.size());

    std::string payload = body;
    std::string encoding;
    if (cfg_.externalize_compression == "zstd") {
        const size_t bound = ZSTD_compressBound(payload.size());
        std::string compressed(bound, '\0');
        const size_t written =
            ZSTD_compress(compressed.data(), bound, payload.data(), payload.size(), 3);
        if (!ZSTD_isError(written)) {
            compressed.resize(written);
            payload.swap(compressed);
            encoding = "zstd";
        }
    }

    const std::string url = storage_->upload(payload, encoding);
    // Charged against the external cap as the bytes the client will fetch.
    if (externalized_bytes) *externalized_bytes += static_cast<int64_t>(payload.size());

    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(keys::LOCATION, url);
    md->Append("vgi_rpc.location.sha256", crypto::hex_encode(digest.data(), digest.size()));

    // One zero-row pointer batch on the original schema replaces the whole
    // cycle; the client fetches the URL and reads the batches back out of it.
    return build_body([&](const std::shared_ptr<arrow::io::OutputStream>& out) {
        write_ipc_stream(out, schema,
                         {AnnotatedBatch::with_metadata(make_empty_batch(schema), md)});
    });
}

std::shared_ptr<arrow::KeyValueMetadata> HttpServer::externalize_cycle(
    const std::vector<AnnotatedBatch>& batches, const std::shared_ptr<arrow::Schema>& schema,
    int64_t* externalized_bytes) const {
    if (!storage_ || cfg_.externalize_threshold < 0 || batches.empty()) return nullptr;

    const std::string cycle = build_body([&](const std::shared_ptr<arrow::io::OutputStream>& out) {
        write_ipc_stream(out, schema, batches);
    });
    if (static_cast<int64_t>(cycle.size()) < cfg_.externalize_threshold) return nullptr;

    auto digest = crypto::sha256(reinterpret_cast<const uint8_t*>(cycle.data()), cycle.size());
    std::string payload = cycle;
    std::string encoding;
    if (cfg_.externalize_compression == "zstd") {
        const size_t bound = ZSTD_compressBound(payload.size());
        std::string compressed(bound, '\0');
        const size_t written =
            ZSTD_compress(compressed.data(), bound, payload.data(), payload.size(), 3);
        if (!ZSTD_isError(written)) {
            compressed.resize(written);
            payload.swap(compressed);
            encoding = "zstd";
        }
    }

    const std::string url = storage_->upload(payload, encoding);
    if (externalized_bytes) *externalized_bytes += static_cast<int64_t>(payload.size());

    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(keys::LOCATION, url);
    md->Append("vgi_rpc.location.sha256", crypto::hex_encode(digest.data(), digest.size()));
    return md;
}

std::optional<IpcStreamContents> HttpServer::resolve_request_pointer(
    const AnnotatedBatch& first) const {
    if (!storage_ || !first.custom_metadata) return std::nullopt;
    const std::string url = get_metadata_value(first.custom_metadata, keys::LOCATION);
    if (url.empty()) return std::nullopt;

    const std::string fetched = storage_->fetch(url);
    const std::string expected_sha256 =
        get_metadata_value(first.custom_metadata, keys::LOCATION_SHA256);
    if (!expected_sha256.empty()) {
        const auto digest =
            crypto::sha256(reinterpret_cast<const uint8_t*>(fetched.data()), fetched.size());
        const std::string actual_sha256 = crypto::hex_encode(digest.data(), digest.size());
        if (actual_sha256 != expected_sha256) {
            throw std::runtime_error("external location checksum mismatch: expected " +
                                     expected_sha256 + ", got " + actual_sha256);
        }
    }
    auto buf = arrow::Buffer::FromString(fetched);
    auto reader = std::make_shared<arrow::io::BufferReader>(buf);
    auto contents = read_ipc_stream(reader);
    if (!contents || contents->batches.empty()) {
        throw std::runtime_error("externalized request resolved to an empty stream");
    }
    if (contents->batches[0].custom_metadata &&
        contents->batches[0].custom_metadata->FindKey(keys::LOCATION) >= 0) {
        // A pointer that points at another pointer is a loop, not a chain.
        throw std::runtime_error("externalized request redirects to another location");
    }
    return contents;
}

// ---------------------------------------------------------------------------
// Authentication
// ---------------------------------------------------------------------------

AuthIdentity HttpServer::identify(const httplib::Request& req) const {
    AuthIdentity id;
    // Fixture affordance for the sticky principal-binding tests: resolve an
    // identity when one is presented, and reject nothing.  The capability
    // probes run before anything authenticates, so a hook that refused
    // anonymous requests would fail the group at the gate.
    if (cfg_.sticky_header_auth && req.has_header(PRINCIPAL_HEADER)) {
        const std::string principal = req.get_header_value(PRINCIPAL_HEADER);
        if (!principal.empty()) {
            id.authenticated = true;
            id.domain = "conformance";
            id.principal = principal;
        }
    }
    return id;
}

void HttpServer::write_unauthorized(const httplib::Request& req, httplib::Response& res,
                                    AuthReason reason) const {
    res.status = 401;
    res.set_header(AUTH_REASON_HEADER, auth_reason_name(reason));
    // A 401 is per-request and flips to 200 on the next attempt with a
    // credential, so it must never land in a shared cache.
    res.set_header("Cache-Control", "no-store");

    // The proxy note is derived from configuration, not from what failed on
    // this request: a require-mode worker emits the identical note whether the
    // proof was absent, expired, or simply wrong.  That is what lets it
    // coexist with the uniform-rejection rule.
    const bool proxy_note = proof_required();
    if (proxy_note) res.set_header(AUTH_PROXY_REQUIRED_HEADER, "true");

    const std::string accept = req.get_header_value("Accept");
    if (accept.find("text/html") != std::string::npos) {
        std::string html =
            "<!doctype html><html><head><title>401 Unauthorized</title></head><body>"
            "<h1>401 Unauthorized</h1><p>reason: ";
        html += auth_reason_name(reason);
        html += "</p>";
        if (proxy_note) {
            html +=
                "<p>This service only accepts requests that arrive through its configured "
                "reverse proxy, which must set the VGI-Proxy-Proof header. A rejection here "
                "is at least as likely to be a proxy misconfiguration as a bad "
                "credential.</p>";
        }
        html += "</body></html>";
        res.set_content(html, "text/html; charset=utf-8");
        return;
    }

    nlohmann::json body;
    body["error"] = "unauthorized";
    body["reason"] = auth_reason_name(reason);
    body["detail"] = proxy_note
                         // Identical on every 401 this worker produces: naming
                         // which check tripped would turn the rejection into an
                         // oracle for the verifier's state.
                         ? "Request did not arrive through the configured trusted proxy."
                         : "Request was refused by the server's authentication hook.";
    if (proxy_note) {
        body["proxy_hint"] =
            "This service only accepts requests that arrive through its configured reverse "
            "proxy, which must set the VGI-Proxy-Proof header. A rejection here is at least "
            "as likely to be a proxy misconfiguration as a bad credential.";
    }
    res.set_content(body.dump(), "application/json");
}

bool HttpServer::refuse_if_unauthorized(const httplib::Request& req, httplib::Response& res,
                                        const std::string& request_id) {
    // Proxy proof runs first, and its outcome is uniform: every reason in the
    // verifier's table collapses onto proxy_required with the same detail.
    if (cfg_.proof_mode != ProofMode::OFF) {
        const size_t count = req.get_header_value_count(PROOF_HEADER);
        const auto result =
            proof_.verify(req.get_header_value(PROOF_HEADER), static_cast<int>(count));
        if (!result.verified() && cfg_.proof_mode == ProofMode::REQUIRE) {
            // Detail goes to stderr, never to the body.
            std::cerr << "vgi_rpc: proxy proof rejected (" << proof_reason_name(result.reason)
                      << ") request_id=" << request_id << "\n";
            write_unauthorized(req, res, AuthReason::PROXY_REQUIRED);
            return true;
        }
    }

    if (cfg_.reject_all == AuthReason::NONE) return false;

    AuthReason reason = cfg_.reject_all;
    if (cfg_.honour_requested_auth_reason && req.has_header(AUTH_REASON_REQUEST_HEADER)) {
        // Fixture affordance only — see HttpConfig.  A production server must
        // never let a request steer the reason its rejection reports.
        if (auto requested =
                auth_reason_from_name(req.get_header_value(AUTH_REASON_REQUEST_HEADER))) {
            reason = *requested;
        }
    }
    write_unauthorized(req, res, reason);
    return true;
}

// ---------------------------------------------------------------------------
// Framework endpoints
// ---------------------------------------------------------------------------

void HttpServer::handle_health(const httplib::Request& req, httplib::Response& res) const {
    // Exempt from authentication in every mode: health probes come from load
    // balancers and orchestrators directly, not through the proxy.
    stamp_common(req, res, random_hex(16));
    res.status = 200;
    if (req.method == "GET") {
        nlohmann::json body;
        body["status"] = "ok";
        body["server_id"] = rpc_.server_id();
        body["protocol"] = rpc_.protocol_name();
        res.set_content(body.dump(), "application/json");
    }
}

void HttpServer::handle_introspect(const httplib::Request& req, httplib::Response& res,
                                   const std::string& request_body) {
    stamp_common(req, res, random_hex(16));

    if (!cfg_.token_introspection) {
        // Definitive, not transient: a 415 from a generic route would read as
        // "retry later", so a proxy pointed at a worker without the feature
        // would retry forever instead of failing at preflight.
        res.status = 404;
        res.set_content(nlohmann::json{{"error", "not_enabled"}}.dump(), "application/json");
        return;
    }

    // An introspector allowlist with no permissive default: authentication is
    // not the same capability as introspection.  A deployment where any valid
    // credential may introspect lets any user resolve a stolen one to its owner.
    const std::string caller = req.get_header_value(PRINCIPAL_HEADER);
    if (caller != INTROSPECTOR_PRINCIPAL) {
        res.status = 403;
        res.set_content(nlohmann::json{{"error", "forbidden"}}.dump(), "application/json");
        return;
    }

    std::string token;
    try {
        auto body = nlohmann::json::parse(request_body);
        token = body.value("token", "");
    } catch (const std::exception&) {
        res.status = 404;
        res.set_content(nlohmann::json{{"error", "not_found"}}.dump(), "application/json");
        return;
    }

    // A JWS the asker can validate locally must never be vouched for here:
    // routing one through hands a third party a bearer token the asker may
    // itself have rejected.  Shape-checked before any resolution is attempted.
    if (std::count(token.begin(), token.end(), '.') == 2) {
        res.status = 404;
        res.set_content(nlohmann::json{{"error", "not_found"}}.dump(), "application/json");
        return;
    }

    if (token == UNAVAILABLE_TOKEN) {
        // The resolver could not answer.  404 is the one status a caller may
        // negative-cache, so a store blip reported as 404 is remembered as
        // "this credential is bad" for the cache's lifetime.
        res.status = 503;
        res.set_header("Retry-After", "5");
        res.set_content(nlohmann::json{{"error", "unavailable"}}.dump(), "application/json");
        return;
    }

    if (token != SUBJECT_TOKEN) {
        // Unknown, expired and malformed are byte-identical answers; reporting
        // which would confirm that a guessed credential exists.  The credential
        // itself appears nowhere in the response.
        res.status = 404;
        res.set_content(nlohmann::json{{"error", "not_found"}}.dump(), "application/json");
        return;
    }

    nlohmann::json body;
    body["principal"] = SUBJECT_PRINCIPAL;
    body["token_name"] = "conformance";
    body["ttl_seconds"] = 300;
    // Exactly three keys.  A claims field would let this worker choose its
    // caller's tenant routing and policy branch; the asker derives everything
    // it needs from the principal alone.
    res.status = 200;
    res.set_content(body.dump(), "application/json");
}

void HttpServer::handle_session_delete(const httplib::Request& req, httplib::Response& res) {
    stamp_common(req, res, random_hex(16));
    if (!cfg_.sticky) {
        res.status = 404;
        return;
    }
    const AuthIdentity id = identify(req);
    const std::string aad = session_aad(id.domain, id.principal, id.authenticated);
    const bool hit = sessions_.close(req.get_header_value(SESSION_HEADER), aad);
    // 204 on a hit, 200 on every failure — idempotent, so a caller holding a
    // stolen token cannot probe whether a session exists.
    res.status = hit ? 204 : 200;
}

// ---------------------------------------------------------------------------
// RPC dispatch
// ---------------------------------------------------------------------------

void HttpServer::handle_rpc(const httplib::Request& req, httplib::Response& res,
                            const std::string& request_body) {
    std::string request_id = req.get_header_value(REQUEST_ID_HEADER);
    if (request_id.empty()) request_id = random_hex(16);
    stamp_common(req, res, request_id);

    // Authentication precedes method dispatch, so a caller cannot enumerate
    // which methods this worker implements by comparing 401 against 404.
    if (refuse_if_unauthorized(req, res, request_id)) return;

    const std::string ctype = req.get_header_value("Content-Type");
    if (ctype.rfind(ARROW_CONTENT_TYPE, 0) != 0) {
        res.status = 415;
        res.set_content("Unsupported Media Type", "text/plain");
        return;
    }

    // Route: /{method}, /{method}/init, /{method}/exchange.
    std::string path = strip_prefix(req.path);
    if (!path.empty() && path[0] == '/') path.erase(0, 1);
    bool is_init = false, is_exchange_ep = false;
    std::string method_name = path;
    if (path.size() > 5 && path.substr(path.size() - 5) == "/init") {
        is_init = true;
        method_name = path.substr(0, path.size() - 5);
    } else if (path.size() > 9 && path.substr(path.size() - 9) == "/exchange") {
        is_exchange_ep = true;
        method_name = path.substr(0, path.size() - 9);
    }

    auto fail = [&](int status, const char* type, const std::string& msg) {
        res.status = status;
        res.set_header(RPC_ERROR_HEADER, "true");
        set_arrow_content(req, res,
                          error_body(empty_schema(), type, msg, rpc_.server_id(), request_id));
    };

    auto body_buf = arrow::Buffer::Wrap(request_body.data(), request_body.size());
    auto reader = std::make_shared<arrow::io::BufferReader>(body_buf);
    std::optional<IpcStreamContents> contents;
    try {
        contents = read_ipc_stream(reader);
    } catch (const std::exception& e) {
        fail(400, "ProtocolError", std::string("Invalid request IPC: ") + e.what());
        return;
    }
    if (!contents || contents->batches.empty()) {
        fail(400, "ProtocolError", "Empty request body");
        return;
    }
    // Stage 1: a pointer batch is replaced by what it points at, so dispatch
    // sees the parameters the caller meant to send.
    try {
        if (auto resolved = resolve_request_pointer(contents->batches[0])) {
            contents = std::move(resolved);
        }
    } catch (const std::exception& e) {
        fail(200, "ProtocolError",
             std::string("Could not resolve externalized request: ") + e.what());
        return;
    }

    auto& first = contents->batches[0];
    auto& batch = first.batch;
    auto& custom_metadata = first.custom_metadata;

    // Stream /exchange continuations carry the state tokens instead of the
    // request_version metadata, so only the initial request is version-checked.
    // The application protocol version rides the same metadata and is gated on
    // the same requests, for the same reason.
    //
    // The synthetic `__`-prefixed methods are exempt: they are framework
    // surface, and `__describe__` in particular is how a mismatched client
    // finds out what this server speaks.
    if (!is_exchange_ep) {
        const std::string wire_method = get_metadata_value(custom_metadata, keys::METHOD);
        if (wire_method != method_name) {
            fail(400, "ProtocolError",
                 wire_method.empty() ? "Missing 'vgi_rpc.method' in request batch custom_metadata."
                                     : "Method name in request does not match the HTTP route.");
            return;
        }
        auto version = get_metadata_value(custom_metadata, keys::REQUEST_VERSION);
        if (version != REQUEST_VERSION_VALUE) {
            fail(400, "VersionError", "Unsupported or missing request version, expected '1'.");
            return;
        }
        if (method_name.rfind("__", 0) != 0) {
            if (auto reason = rpc_.protocol_version_error(custom_metadata); !reason.empty()) {
                fail(400, "ProtocolVersionError", reason);
                return;
            }
        }
    }

    const AuthIdentity id = identify(req);
    const std::string aad = session_aad(id.domain, id.principal, id.authenticated);

    // Synthetic method: vends upload/download URL pairs so a client can
    // externalize an outgoing batch the server would otherwise refuse at 413.
    // Present only when a backend is configured, so a client that discovers
    // upload-URL support can rely on the route existing.
    if (method_name == "__upload_url__" && is_init) {
        if (!storage_) {
            fail(404, "AttributeError", "Upload URLs are not configured");
            return;
        }
        int64_t count = 1;
        if (auto col = batch->GetColumnByName("count");
            col && col->length() > 0 && col->type()->id() == arrow::Type::INT64) {
            count = std::static_pointer_cast<arrow::Int64Array>(col)->Value(0);
        }
        count = std::clamp<int64_t>(count, 1, 100);

        try {
            const auto pairs = storage_->upload_urls(count);
            auto schema = arrow::schema({
                arrow::field("upload_url", arrow::utf8()),
                arrow::field("download_url", arrow::utf8()),
                arrow::field("expires_at", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC")),
            });
            arrow::StringBuilder up, down;
            arrow::TimestampBuilder exp(arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"),
                                        arrow::default_memory_pool());
            const int64_t expires_us = (static_cast<int64_t>(std::time(nullptr)) + 3600) * 1000000;
            for (const auto& pair : pairs) {
                VGI_RPC_THROW_NOT_OK(up.Append(pair.upload_url));
                VGI_RPC_THROW_NOT_OK(down.Append(pair.download_url));
                VGI_RPC_THROW_NOT_OK(exp.Append(expires_us));
            }
            auto out_batch = arrow::RecordBatch::Make(
                schema, static_cast<int64_t>(pairs.size()),
                {unwrap(up.Finish()), unwrap(down.Finish()), unwrap(exp.Finish())});
            res.status = 200;
            set_arrow_content(req, res,
                              build_body([&](const std::shared_ptr<arrow::io::OutputStream>& out) {
                                  write_ipc_stream(out, schema, {AnnotatedBatch::data(out_batch)});
                              }));
        } catch (const std::exception& e) {
            fail(500, "RuntimeError", std::string("cannot vend upload URLs: ") + e.what());
        }
        return;
    }

    auto it = rpc_.methods().find(method_name);
    if (it == rpc_.methods().end()) {
        fail(404, "AttributeError", "Unknown method: '" + method_name + "'");
        return;
    }
    const auto& method_info = it->second;
    if (!is_exchange_ep) {
        if (const std::string error = parameter_contract_error(batch, method_info.params_schema);
            !error.empty()) {
            fail(400, "ProtocolError", error);
            return;
        }
    }
    Request request(batch, custom_metadata);

    auto log_sink = std::make_shared<LogSink>(rpc_.server_id(), request_id);
    CallContext ctx(log_sink, rpc_.server_id(), request_id, TransportKind::HTTP);

    // Sticky machinery for this request, installed only on HTTP.
    StickySlot sticky;
    std::string resumed_token;
    std::unique_lock<std::recursive_mutex> sticky_dispatch_lock;
    if (cfg_.sticky) {
        sticky.client_accepts = req.get_header_value(SESSION_ACCEPT_HEADER) == "true";
        sticky.draining = sessions_.draining();
        sticky.open = [&](std::shared_ptr<SessionState> state, std::optional<int> ttl) {
            return sessions_.open(std::move(state), aad, ttl);
        };
        sticky.close = [&]() { sessions_.close(resumed_token, aad); };

        resumed_token = req.get_header_value(SESSION_HEADER);
        if (!resumed_token.empty()) {
            std::shared_ptr<SessionState> state;
            std::string session_id;
            const SessionLookup status =
                sessions_.resolve(resumed_token, aad, &state, &session_id, &sticky_dispatch_lock);
            if (status != SessionLookup::OK) {
                // Every cause reports identically — see SessionLookup.
                res.status = 200;
                res.set_header(RPC_ERROR_HEADER, "true");
                set_arrow_content(
                    req, res,
                    error_body(method_info.result_schema, "SessionLostError", "session lost",
                               rpc_.server_id(), request_id, ERROR_KIND_SESSION_LOST));
                return;
            }
            sticky.resolved = std::move(state);
            sticky.session_id = session_id;
        }
        ctx.set_sticky(&sticky);
    }

    // Everything a handler may have asked the transport to do to the response.
    auto apply_sticky = [&]() {
        if (!sticky.minted_token.empty()) {
            res.set_header(SESSION_HEADER, sticky.minted_token);
            // Once only, on the response that opens the session: the client
            // holds the captured map for the session's lifetime, so re-sending
            // them every turn would be noise.
            for (const auto& [name, value] : cfg_.sticky_echo_headers) {
                res.set_header("VGI-Echo-" + name, value);
            }
        }
        if (sticky.closed) res.set_header(SESSION_CLOSE_HEADER, "true");
    };

    // ---- Unary ----
    if (!is_init && !is_exchange_ep) {
        auto buf_out = unwrap(arrow::io::BufferOutputStream::Create());
        const bool errored = rpc_.serve_unary_http(method_info, request, request_id, buf_out, ctx);
        auto rbuf = unwrap(buf_out->Finish());
        apply_sticky();

        std::string body = buffer_to_string(rbuf);
        int64_t externalized = 0;
        if (!errored) {
            try {
                body = maybe_externalize(body, method_info.result_schema, &externalized);
            } catch (const std::exception& e) {
                res.status = 200;
                res.set_header(RPC_ERROR_HEADER, "true");
                set_arrow_content(req, res,
                                  error_body(method_info.result_schema, "RpcError",
                                             std::string("externalization failed: ") + e.what(),
                                             rpc_.server_id(), request_id));
                return;
            }
        }

        // The external cap is hard for every method type: it bounds how much
        // the client will end up fetching for one call, regardless of how the
        // framework chose to deliver it.
        if (cfg_.max_externalized_response_bytes >= 0 &&
            externalized > cfg_.max_externalized_response_bytes) {
            res.status = 200;
            res.set_header(RPC_ERROR_HEADER, "true");
            set_arrow_content(
                req, res,
                error_body(method_info.result_schema, "RpcError",
                           "Externalised payload exceeds max_externalized_response_bytes (" +
                               std::to_string(externalized) + " > " +
                               std::to_string(cfg_.max_externalized_response_bytes) +
                               ") for method '" + method_name + "'",
                           rpc_.server_id(), request_id));
            return;
        }

        // The wire cap counts only what lands on the wire; an externalized
        // payload leaves a pointer batch of a few hundred bytes behind, so it
        // is measured after externalization, not before.
        if (cfg_.max_response_bytes >= 0 &&
            static_cast<int64_t>(body.size()) > cfg_.max_response_bytes) {
            res.status = 200;
            res.set_header(RPC_ERROR_HEADER, "true");
            set_arrow_content(
                req, res,
                error_body(method_info.result_schema, "RpcError",
                           "HTTP body exceeds max_response_bytes (" + std::to_string(body.size()) +
                               " > " + std::to_string(cfg_.max_response_bytes) + ") for method '" +
                               method_name + "'",
                           rpc_.server_id(), request_id));
            return;
        }
        res.status = 200;
        // A failed RPC still answers 200 — the error rides the body, because
        // the call reached the method and the method raised.  This header is
        // the only thing that tells a client a failure from a result.
        if (errored) res.set_header(RPC_ERROR_HEADER, "true");
        set_arrow_content(req, res, std::move(body));
        return;
    }

    // ---- Stream init ----
    if (is_init) {
        Stream stream;
        try {
            stream = method_info.stream_factory(request, ctx);
        } catch (const std::exception& e) {
            res.status = 200;
            res.set_header(RPC_ERROR_HEADER, "true");
            apply_sticky();
            set_arrow_content(req, res,
                              error_body(empty_schema(), exception_type_of(e), e.what(),
                                         rpc_.server_id(), request_id, error_kind_of(e)));
            return;
        }

        const std::string cursor = random_hex(32);
        // The call token names the fixed half of the stream and is minted
        // exactly once; the cursor advances every turn.  Both are opaque to
        // the client, which only echoes them back.
        const std::string call_token = crypto::base64url_encode(
            // NUL-separated, built explicitly: `name + "\x00" + cursor` would
            // append an empty C string and silently drop the separator.
            crypto::aead_seal(cfg_.token_key, method_name + std::string(1, '\0') + cursor, aad));
        auto output_schema = stream.output_schema;
        auto input_schema = stream.input_schema;
        // Asked of the state the factory built, not of how the method was
        // registered. One method can be both: `init` is declared an exchange
        // because a scalar or table-in-out call pushes batches into it, but a
        // table scan's init returns a producer that runs to exhaustion and
        // finishes.
        //
        // Taking the static flag made every table scan an exchange over HTTP,
        // and the first `finish()` was refused with "finish() is not allowed on
        // exchange streams" — a scan working on three transports and failing on
        // the fourth. Deciding on the input schema instead is just as wrong the
        // other way: a scalar whose arguments are all constants also has an
        // empty input schema, and routing *that* as a producer skips the turn
        // semantics and returns no rows.
        const bool is_exchange = method_info.is_exchange &&
                                 dynamic_cast<const ProducerState*>(stream.state.get()) == nullptr;
        bool errored = false;
        int64_t externalized = 0;
        auto externalize = [&](const std::vector<AnnotatedBatch>& batches,
                               const std::shared_ptr<arrow::Schema>& schema) {
            return externalize_cycle(batches, schema, &externalized);
        };

        std::string body = build_body([&](const std::shared_ptr<arrow::io::OutputStream>& out) {
            if (stream.header) {
                auto header_schema = stream.header->schema();
                std::vector<AnnotatedBatch> hb = log_sink->flush(header_schema);
                hb.push_back(AnnotatedBatch::data(stream.header));
                write_ipc_stream(out, header_schema, hb);
            }

            if (is_exchange) {
                std::vector<AnnotatedBatch> ob;
                if (!stream.header) ob = log_sink->flush(output_schema);
                ob.push_back(AnnotatedBatch::with_metadata(make_empty_batch(output_schema),
                                                           init_metadata(cursor, call_token)));
                write_ipc_stream(out, output_schema, ob);
                auto session = std::make_shared<HttpStreamSession>();
                session->state = stream.state;
                session->output_schema = output_schema;
                session->input_schema = input_schema;
                session->is_exchange = true;
                session->method_name = method_name;
                session->aad = aad;
                std::lock_guard<std::mutex> registry_lock(streams_mutex_);
                streams_[cursor] = std::move(session);
            } else {
                auto writer = unwrap(arrow::ipc::MakeStreamWriter(out, output_schema));
                if (!stream.header) {
                    for (auto& lb : log_sink->flush(output_schema)) {
                        VGI_RPC_THROW_NOT_OK(
                            lb.custom_metadata
                                ? writer->WriteRecordBatch(*lb.batch, lb.custom_metadata)
                                : writer->WriteRecordBatch(*lb.batch));
                    }
                }
                const bool finished = run_producer_turns(
                    writer, stream.state, output_schema, ctx, rpc_.server_id(), request_id,
                    &errored, externalize, AnnotatedBatch::data(make_empty_batch(empty_schema())));
                if (!finished) {
                    VGI_RPC_THROW_NOT_OK(writer->WriteRecordBatch(
                        *make_empty_batch(output_schema), init_metadata(cursor, call_token)));
                    auto session = std::make_shared<HttpStreamSession>();
                    session->state = stream.state;
                    session->output_schema = output_schema;
                    session->input_schema = input_schema;
                    session->is_exchange = false;
                    session->method_name = method_name;
                    session->aad = aad;
                    std::lock_guard<std::mutex> registry_lock(streams_mutex_);
                    streams_[cursor] = std::move(session);
                }
                VGI_RPC_THROW_NOT_OK(writer->Close());
            }
        });
        // The external cap is hard even for a producer: by the time anyone
        // could mint a continuation the upload has already happened, so there
        // is nothing for a soft cap to spread the overshoot across.
        if (cfg_.max_externalized_response_bytes >= 0 &&
            externalized > cfg_.max_externalized_response_bytes) {
            {
                std::lock_guard<std::mutex> registry_lock(streams_mutex_);
                streams_.erase(cursor);
            }
            res.status = 200;
            res.set_header(RPC_ERROR_HEADER, "true");
            apply_sticky();
            set_arrow_content(
                req, res,
                error_body(output_schema, "RpcError",
                           "Externalised payload exceeds max_externalized_response_bytes (" +
                               std::to_string(externalized) + " > " +
                               std::to_string(cfg_.max_externalized_response_bytes) +
                               ") for method '" + method_name + "'",
                           rpc_.server_id(), request_id));
            return;
        }
        res.status = 200;
        if (errored) res.set_header(RPC_ERROR_HEADER, "true");
        apply_sticky();
        set_arrow_content(req, res, body);
        return;
    }

    // ---- Stream exchange / producer continuation ----
    auto cursor = get_metadata_value(custom_metadata, keys::STATE_B64);
    std::shared_ptr<HttpStreamSession> sess;
    if (!cursor.empty()) {
        std::lock_guard<std::mutex> registry_lock(streams_mutex_);
        auto sit = streams_.find(cursor);
        if (sit != streams_.end()) sess = sit->second;
    }
    if (!sess) {
        fail(400, "ProtocolError", "Unknown or missing stream state token");
        return;
    }
    std::unique_lock<std::mutex> stream_dispatch_lock(sess->dispatch_mutex);
    // A concurrent terminal/cancel turn may have removed this session while
    // this request waited on its state lock. Revalidate before touching state.
    bool still_live = false;
    {
        std::lock_guard<std::mutex> registry_lock(streams_mutex_);
        auto sit = streams_.find(cursor);
        still_live = sit != streams_.end() && sit->second == sess;
    }
    if (!still_live) {
        fail(400, "ProtocolError", "Unknown or missing stream state token");
        return;
    }
    // A stream is bound to the identity that opened it for the same reason a
    // session is: otherwise a second caller could resume the first's cursor.
    if (sess->aad != aad) {
        fail(400, "ProtocolError", "Unknown or missing stream state token");
        return;
    }

    // With the call-state cache off, a continuation must stand on its own: it
    // has to carry the call token /init handed over, not just the cursor.  A
    // client that echoes only the cursor works perfectly against a warm cache
    // and fails the moment one is not there — a restarted worker, an evicted
    // entry, or a continuation load-balanced to a node that never saw /init.
    // Turning the cache off makes that load-dependent bug deterministic.
    if (!cfg_.call_state_cache) {
        const std::string call_token = get_metadata_value(custom_metadata, keys::CALL_STATE_B64);
        bool resolved = false;
        if (!call_token.empty()) {
            if (auto raw = crypto::base64url_decode(call_token)) {
                if (auto opened = crypto::aead_open(cfg_.token_key, *raw, aad)) {
                    const size_t sep = opened->find('\0');
                    resolved = sep != std::string::npos &&
                               opened->substr(0, sep) == sess->method_name &&
                               opened->substr(sep + 1) == cursor;
                }
            }
        }
        if (!resolved) {
            fail(400, "ProtocolError",
                 "Stream continuation is missing its call token; a client must echo "
                 "vgi_rpc.call_state#b64 alongside vgi_rpc.stream_state#b64 on every turn");
            return;
        }
    }
    auto output_schema = sess->output_schema;

    if (custom_metadata && custom_metadata->FindKey(keys::CANCEL) >= 0) {
        try {
            sess->state->on_cancel(ctx);
        } catch (...) {
        }
        {
            std::lock_guard<std::mutex> registry_lock(streams_mutex_);
            auto sit = streams_.find(cursor);
            if (sit != streams_.end() && sit->second == sess) streams_.erase(sit);
        }
        res.status = 200;
        apply_sticky();
        set_arrow_content(req, res,
                          build_body([&](const std::shared_ptr<arrow::io::OutputStream>& out) {
                              write_ipc_stream(out, output_schema, {});
                          }));
        return;
    }

    auto stream_error = [&](const std::exception& e) {
        {
            std::lock_guard<std::mutex> registry_lock(streams_mutex_);
            auto sit = streams_.find(cursor);
            if (sit != streams_.end() && sit->second == sess) streams_.erase(sit);
        }
        res.status = 200;
        res.set_header(RPC_ERROR_HEADER, "true");
        apply_sticky();
        set_arrow_content(req, res,
                          error_body(output_schema, exception_type_of(e), e.what(),
                                     rpc_.server_id(), request_id, error_kind_of(e)));
    };

    int64_t externalized = 0;
    auto externalize = [&](const std::vector<AnnotatedBatch>& batches,
                           const std::shared_ptr<arrow::Schema>& schema) {
        return externalize_cycle(batches, schema, &externalized);
    };
    // Same hard cap as everywhere else: what the client will end up fetching
    // for one call is bounded regardless of which channel delivered it.
    auto external_cap_exceeded = [&]() {
        if (cfg_.max_externalized_response_bytes < 0) return false;
        if (externalized <= cfg_.max_externalized_response_bytes) return false;
        {
            std::lock_guard<std::mutex> registry_lock(streams_mutex_);
            auto sit = streams_.find(cursor);
            if (sit != streams_.end() && sit->second == sess) streams_.erase(sit);
        }
        res.status = 200;
        res.set_header(RPC_ERROR_HEADER, "true");
        apply_sticky();
        set_arrow_content(
            req, res,
            error_body(output_schema, "RpcError",
                       "Externalised payload exceeds max_externalized_response_bytes (" +
                           std::to_string(externalized) + " > " +
                           std::to_string(cfg_.max_externalized_response_bytes) + ") for method '" +
                           sess->method_name + "'",
                       rpc_.server_id(), request_id));
        return true;
    };

    try {
        if (sess->is_exchange) {
            auto coerced = coerce_input(batch, sess->input_schema);
            OutputCollector oc(output_schema, /*producer=*/false, rpc_.server_id(), request_id);
            sess->state->process(AnnotatedBatch::data(coerced), oc, ctx);
            std::string body = build_body([&](const std::shared_ptr<arrow::io::OutputStream>& out) {
                std::vector<AnnotatedBatch> batches = oc.batches();
                // Only the cursor is re-minted; re-issuing the call token
                // here would be exactly the work the split avoids. Merged,
                // not assigned: whatever the handler attached to its own
                // data batch has to survive the ride.
                for (auto& ab : batches) {
                    if (ab.batch && ab.batch->num_rows() > 0) {
                        ab.merge_metadata(cursor_metadata(cursor));
                    }
                }
                if (batches.empty() || batches.back().batch->num_rows() == 0) {
                    batches.push_back(AnnotatedBatch::with_metadata(make_empty_batch(output_schema),
                                                                    cursor_metadata(cursor)));
                }
                write_ipc_stream(out, output_schema, batches);
            });
            if (cfg_.max_response_bytes >= 0 &&
                static_cast<int64_t>(body.size()) > cfg_.max_response_bytes) {
                res.status = 200;
                res.set_header(RPC_ERROR_HEADER, "true");
                apply_sticky();
                set_arrow_content(req, res,
                                  error_body(output_schema, "RpcError",
                                             "HTTP body exceeds max_response_bytes (" +
                                                 std::to_string(body.size()) + " > " +
                                                 std::to_string(cfg_.max_response_bytes) +
                                                 ") for method '" + sess->method_name + "'",
                                             rpc_.server_id(), request_id));
                return;
            }
            res.status = 200;
            apply_sticky();
            set_arrow_content(req, res, body);
        } else {
            bool finished = false;
            bool errored = false;
            std::string body = build_body([&](const std::shared_ptr<arrow::io::OutputStream>& out) {
                auto writer = unwrap(arrow::ipc::MakeStreamWriter(out, output_schema));
                finished = run_producer_turns(
                    writer, sess->state, output_schema, ctx, rpc_.server_id(), request_id, &errored,
                    externalize, AnnotatedBatch::with_metadata(batch, custom_metadata));
                if (!finished) {
                    VGI_RPC_THROW_NOT_OK(writer->WriteRecordBatch(*make_empty_batch(output_schema),
                                                                  cursor_metadata(cursor)));
                }
                VGI_RPC_THROW_NOT_OK(writer->Close());
            });
            if (external_cap_exceeded()) return;
            if (finished) {
                std::lock_guard<std::mutex> registry_lock(streams_mutex_);
                auto sit = streams_.find(cursor);
                if (sit != streams_.end() && sit->second == sess) streams_.erase(sit);
            }
            res.status = 200;
            if (errored) res.set_header(RPC_ERROR_HEADER, "true");
            apply_sticky();
            set_arrow_content(req, res, body);
        }
    } catch (const std::exception& e) {
        stream_error(e);
    }
}

void HttpServer::run() {
    httplib::Server svr;
    if (cfg_.max_request_bytes >= 0) {
        svr.set_payload_max_length(static_cast<size_t>(cfg_.max_request_bytes));
    }
    // Keep httplib's default multi-threaded pool. Independent calls dispatch in
    // parallel; the HTTP state registries serialize only turns that address the
    // same stream or sticky session.

    // Without this, an escaping exception drops the connection, which a client
    // reads as "server disconnected" — indistinguishable from a crash and
    // impossible to diagnose from the outside.  A 500 with the reason is worse
    // than a correct answer and much better than silence.
    svr.set_exception_handler(
        [](const httplib::Request& req, httplib::Response& res, std::exception_ptr ep) {
            std::string what = "unknown error";
            try {
                std::rethrow_exception(ep);
            } catch (const std::exception& e) {
                what = e.what();
            } catch (...) {
            }
            std::cerr << "vgi_rpc: unhandled exception serving " << req.method << " " << req.path
                      << ": " << what << "\n";
            res.status = 500;
            res.set_content(nlohmann::json{{"error", "internal"}, {"detail", what}}.dump(),
                            "application/json");
        });

    // Regular handlers have already consumed their request body by the time
    // this hook runs.  The RPC content-reader route below performs the same
    // notification explicitly because cpp-httplib bypasses pre_request for
    // content-reader handlers.
    svr.set_pre_request_handler([this](const httplib::Request&, httplib::Response&) {
        rpc_.notify_serve_start(TransportKind::HTTP);
        return httplib::Server::HandlerResponse::Unhandled;
    });

    auto health = [this](const httplib::Request& req, httplib::Response& res) {
        handle_health(req, res);
    };
    for (const std::string& p : {std::string("/health"), cfg_.prefix + "/health"}) {
        svr.Get(p, health);
        svr.Options(p, health);
    }

    // Always routed, even when disabled: a caller must get a definitive answer
    // rather than whatever a generic route happens to produce.
    // cpp-httplib keeps ordinary POST handlers and ContentReader POST handlers
    // in separate tables, and consults the latter first.  Register framework
    // POST endpoints as ContentReader handlers too, otherwise the catch-all RPC
    // route below wins and rejects their JSON/empty bodies as non-Arrow media.
    auto introspect = [this](const httplib::Request& req, httplib::Response& res,
                             const httplib::ContentReader& reader) {
        std::exception_ptr serve_start_error;
        try {
            rpc_.notify_serve_start(TransportKind::HTTP);
        } catch (...) {
            serve_start_error = std::current_exception();
        }

        const int64_t cap = cfg_.max_request_bytes;
        std::string body;
        bool decoded_too_large = false;
        const bool read = reader([&](const char* data, size_t size) {
            if (cap >= 0 && (size > static_cast<size_t>(cap) ||
                             body.size() > static_cast<size_t>(cap) - size)) {
                decoded_too_large = true;
                return false;
            }
            body.append(data, size);
            return true;
        });
        if (serve_start_error) std::rethrow_exception(serve_start_error);
        if (!read) {
            stamp_common(req, res, random_hex(16));
            if (decoded_too_large || res.status == 413) {
                res.status = 413;
                res.set_content("Request body exceeds VGI-Max-Request-Bytes", "text/plain");
            } else if (res.status < 400) {
                res.status = 400;
                res.set_content("Invalid compressed request body", "text/plain");
            }
            return;
        }
        handle_introspect(req, res, body);
    };
    for (const std::string& p :
         {std::string("/__introspect_token__"), cfg_.prefix + "/__introspect_token__"}) {
        svr.Post(p, introspect);
    }

    if (cfg_.test_drain_endpoint) {
        // Conformance affordance: flip the drain flag over the wire, because
        // the alternative — SIGTERM — kills the worker the test is driving.
        svr.Post("/__test_drain__", [this](const httplib::Request&, httplib::Response& res,
                                           const httplib::ContentReader& reader) {
            std::exception_ptr serve_start_error;
            try {
                rpc_.notify_serve_start(TransportKind::HTTP);
            } catch (...) {
                serve_start_error = std::current_exception();
            }
            const bool read = reader([](const char*, size_t) { return true; });
            if (serve_start_error) std::rethrow_exception(serve_start_error);
            if (!read) {
                if (res.status < 400) res.status = 400;
                return;
            }
            sessions_.drain();
            res.status = 204;
        });
        svr.Delete("/__test_drain__", [this](const httplib::Request&, httplib::Response& res) {
            // Undrain exists only here.  A deployment only ever drains, so the
            // operator API has no reverse; a test suite needs one so a drain
            // case does not poison every case after it.
            sessions_.undrain();
            res.status = 204;
        });
    }

    auto session_delete = [this](const httplib::Request& req, httplib::Response& res) {
        handle_session_delete(req, res);
    };
    for (const std::string& p : {std::string("/__session__"), cfg_.prefix + "/__session__"}) {
        svr.Delete(p, session_delete);
    }

    // CORS preflight for the RPC surface.  Exempt from authentication in every
    // mode — a preflight cannot carry a credential or a proof.
    svr.Options(R"(/(.*))", [this](const httplib::Request& req, httplib::Response& res) {
        stamp_common(req, res, random_hex(16));
        res.status = 204;
    });

    svr.Post(R"(/(.+))", [this](const httplib::Request& req, httplib::Response& res,
                                const httplib::ContentReader& reader) {
        // Keep reading the body even when the startup hook fails.  Otherwise
        // unread bytes poison this keep-alive connection and the retry cannot
        // prove that the listener remains reusable.
        std::exception_ptr serve_start_error;
        try {
            rpc_.notify_serve_start(TransportKind::HTTP);
        } catch (...) {
            serve_start_error = std::current_exception();
        }
        const int64_t cap = cfg_.max_request_bytes >= 0
                                ? cfg_.max_request_bytes
                                : (storage_ ? cfg_.externalize_threshold : -1);
        std::string body;
        bool decoded_too_large = false;
        const bool read = reader([&](const char* data, size_t size) {
            if (cap >= 0 && (size > static_cast<size_t>(cap) ||
                             body.size() > static_cast<size_t>(cap) - size)) {
                decoded_too_large = true;
                return false;
            }
            body.append(data, size);
            return true;
        });
        if (serve_start_error) std::rethrow_exception(serve_start_error);
        if (!read) {
            stamp_common(req, res, random_hex(16));
            if (decoded_too_large || res.status == 413) {
                res.status = 413;
                res.set_content("Request body exceeds VGI-Max-Request-Bytes", "text/plain");
            } else if (res.status < 400) {
                res.status = 400;
                res.set_content("Invalid compressed request body", "text/plain");
            }
            return;
        }
        handle_rpc(req, res, body);
    });

    int bound;
    if (cfg_.port == 0) {
        bound = svr.bind_to_any_port(cfg_.host);
    } else {
        bound = svr.bind_to_port(cfg_.host, cfg_.port) ? cfg_.port : -1;
    }
    if (bound < 0) {
        std::cerr << "vgi_rpc: failed to bind HTTP server to " << cfg_.host << ":" << cfg_.port
                  << "\n";
        return;
    }
    std::cout << "PORT:" << bound << std::endl;
    svr.listen_after_bind();
}

void Server::serve_http(const HttpConfig& config) {
    HttpServer server(*this, config);
    server.run();
}

void Server::serve_http(const std::string& host, int port, int64_t max_response_bytes) {
    HttpConfig cfg;
    cfg.host = host;
    cfg.port = port;
    cfg.max_response_bytes = max_response_bytes;
    serve_http(cfg);
}

}  // namespace vgi_rpc
