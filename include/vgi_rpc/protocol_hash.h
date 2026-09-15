#pragma once

// The protocol hash: a fingerprint of a protocol's wire surface.
//
// A client and a worker agree on a protocol or they do not, and the hash is how
// either side says which one it has without shipping the whole description. For
// that to be worth anything the same protocol must hash the same in every port,
// which the previous definition could not promise: it hashed serialized Arrow
// IPC bytes, and each language's Arrow implementation may legitimately emit
// different bytes for the same logical schema. The docs said so, which made the
// field advisory -- comparable only against itself.
//
// So the preimage is canonical JSON of what Arrow *decodes to*:
//
//     sha256("vgi_rpc.protocol_hash.v1|" + CanonicalDescription(...))
//
// Profile: RFC 8785 (JCS), chosen for its published test vectors. The structure
// is deliberately restricted to objects, arrays, strings and booleans; every
// number is folded into a type token (`decimal128(38,9)`), so JCS's hardest rule
// -- number canonicalisation, and the likeliest place for six ports to diverge
// -- never applies. Keep it that way.
//
// Not in the preimage: server identity, docstrings, parameter defaults,
// language-specific type names, the framework's own request/describe versions,
// and whether a stream is an exchange. That last is an *implementation*
// property, not visible on the protocol definition, so one port can determine it
// and another cannot -- and a field one port knows and another does not cannot
// be part of a cross-language contract. It still reaches clients as
// `stream_kind` on the description, where "unknown" is a sayable answer; a hash
// has no such option.

#include <memory>
#include <string>
#include <vector>

#include <arrow/result.h>
#include <arrow/type.h>

#include "vgi_rpc/export.h"

namespace vgi_rpc {

/// Domain separator. Moves only when the hash definition moves, never when a
/// protocol changes -- that is what the hash itself is for.
inline constexpr const char* kProtocolHashDomain = "vgi_rpc.protocol_hash.v1|";

/// One method's input to `ComputeProtocolHash`.
///
/// Takes decoded schemas rather than serialized IPC: the hash is over structure,
/// and accepting bytes would invite a caller to pass whatever its encoder
/// produced.
struct VGI_RPC_EXPORT HashMethod {
  std::string name;
  /// "unary" or "stream".
  std::string method_type;
  bool has_return = false;
  bool has_header = false;
  std::shared_ptr<arrow::Schema> params_schema;
  /// Ignored when `has_return` is false.
  std::shared_ptr<arrow::Schema> result_schema;
  /// Ignored when `has_header` is false.
  std::shared_ptr<arrow::Schema> header_schema;
};

/// Build the canonical preimage for one protocol.
///
/// Exposed because a hash mismatch between ports is otherwise one bit of
/// information. With the preimage in hand a failing port diffs two JSON
/// documents and sees which method, field or type token it spells differently.
VGI_RPC_EXPORT arrow::Result<std::string> CanonicalDescription(
    const std::string& protocol_name, std::vector<HashMethod> methods);

/// Return the SHA-256 hex digest of a protocol's canonical description.
///
/// Identical in every port for the same protocol -- which is a property
/// conformance can assert, and could not before.
VGI_RPC_EXPORT arrow::Result<std::string> ComputeProtocolHash(
    const std::string& protocol_name, std::vector<HashMethod> methods);

}  // namespace vgi_rpc
