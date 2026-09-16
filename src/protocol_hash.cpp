#include "vgi_rpc/protocol_hash.h"

#include <algorithm>
#include <cstdio>

#include "vgi_rpc/crypto.h"
#include "vgi_rpc/type_tokens.h"

namespace vgi_rpc {
namespace {

/// Encode a string the way RFC 8785 requires.
///
/// Short escapes where JCS mandates them, `\uXXXX` only for the remaining
/// control characters, and every other byte emitted as itself -- notably *not*
/// ASCII-escaped, which is where a JSON library's defaults would silently
/// diverge from the other ports. Written by hand for exactly that reason.
std::string JsonString(const std::string& value) {
    std::string out = "\"";
    for (unsigned char c : value) {
        switch (c) {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\b': out += "\\b"; break;
            case '\f': out += "\\f"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (c < 0x20) {
                    char buf[7];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", c);
                    out += buf;
                } else {
                    out += static_cast<char>(c);
                }
        }
    }
    return out + "\"";
}

arrow::Result<std::string> FieldTokensJson(const std::shared_ptr<arrow::Schema>& schema) {
    ARROW_ASSIGN_OR_RAISE(auto tokens, SchemaTokens(schema.get()));
    std::string out = "[";
    for (size_t i = 0; i < tokens.size(); ++i) {
        if (i > 0) out += ",";
        out += "{\"name\":" + JsonString(tokens[i].name) +
               ",\"nullable\":" + (tokens[i].nullable ? "true" : "false") +
               ",\"type\":" + JsonString(tokens[i].type) + "}";
    }
    return out + "]";
}

/// One method entry, with its keys emitted in sorted order.
///
/// Canonical JSON requires sorted keys, and the order here is load bearing
/// rather than cosmetic: emitting `header` after `params` would change the
/// digest for every protocol that has a stream header, which is exactly the
/// class of bug the canonical form exists to prevent.
arrow::Result<std::string> MethodEntry(const HashMethod& m) {
    std::string out = "{\"has_header\":";
    out += m.has_header ? "true" : "false";
    out += ",\"has_return\":";
    out += m.has_return ? "true" : "false";
    // Absent and empty are different: a method returning nothing is not a method
    // returning an empty struct, and they must not hash alike.
    if (m.has_header) {
        ARROW_ASSIGN_OR_RAISE(auto header, FieldTokensJson(m.header_schema));
        out += ",\"header\":" + header;
    }
    out += ",\"name\":" + JsonString(m.name);
    ARROW_ASSIGN_OR_RAISE(auto params, FieldTokensJson(m.params_schema));
    out += ",\"params\":" + params;
    if (m.has_return) {
        ARROW_ASSIGN_OR_RAISE(auto result, FieldTokensJson(m.result_schema));
        out += ",\"result\":" + result;
    }
    out += ",\"type\":" + JsonString(m.method_type);
    return out + "}";
}

}  // namespace

arrow::Result<std::string> CanonicalDescription(const std::string& protocol_name,
                                                std::vector<HashMethod> methods) {
    // Sorted so two ports iterating differently-ordered maps still agree.
    std::sort(methods.begin(), methods.end(),
              [](const HashMethod& a, const HashMethod& b) { return a.name < b.name; });

    std::string out = "{\"methods\":[";
    for (size_t i = 0; i < methods.size(); ++i) {
        if (i > 0) out += ",";
        ARROW_ASSIGN_OR_RAISE(auto entry, MethodEntry(methods[i]));
        out += entry;
    }
    return out + "],\"protocol\":" + JsonString(protocol_name) + "}";
}

arrow::Result<std::string> ComputeProtocolHash(const std::string& protocol_name,
                                               std::vector<HashMethod> methods) {
    ARROW_ASSIGN_OR_RAISE(auto description,
                          CanonicalDescription(protocol_name, std::move(methods)));
    const std::string preimage = std::string(kProtocolHashDomain) + description;
    auto digest =
        crypto::sha256(reinterpret_cast<const uint8_t*>(preimage.data()), preimage.size());
    std::string hex;
    hex.reserve(64);
    static constexpr char kHex[] = "0123456789abcdef";
    for (uint8_t b : digest) {
        hex += kHex[b >> 4];
        hex += kHex[b & 0x0F];
    }
    return hex;
}

}  // namespace vgi_rpc
