#pragma once

// Canonical text tokens for Arrow types, for the protocol hash preimage.
//
// The protocol hash is taken over what Arrow *decodes to*, not over what an
// encoder emits: each language's Arrow implementation may legitimately produce
// different bytes for the same logical schema, so a hash over serialized IPC is
// not a cross-language contract. The preimage is canonical JSON (RFC 8785) of
// the decoded description, and these tokens are how a type appears inside it.
//
// JSON solves framing, escaping and key ordering. It does not solve spelling --
// two ports can agree on every JCS rule and still disagree on whether a
// microsecond timestamp is `timestamp[us]` or `timestamp(us)`, which is a
// silent hash divergence. So the vocabulary is enumerated exhaustively and
// `TypeToken` is total: an unrecognised type is an error rather than a fallback
// to `DataType::ToString()`, whose output is an arrow-cpp implementation detail
// that differs from every other port.
//
// Grammar: a token is lowercase ASCII. Parameters go in parentheses, children
// in angle brackets. A child is `name:token` when non-nullable and `name?:token`
// when nullable -- child nullability is part of the type in Arrow, and two
// schemas differing only there are different schemas. Numeric parameters are
// folded into the token (`decimal128(38,9)`) so the preimage contains no JSON
// numbers and RFC 8785's hardest rule, number canonicalisation, never applies.
// Keep it that way.
//
// What is normalised: Arrow's own type equality ignores the *name* of a list's
// child field and of a map's key/value fields. Those names are normalised,
// because keeping them would give two ports different hashes for a protocol
// Arrow itself calls identical. Everything Arrow does treat as part of the type
// is kept: child nullability, struct field names, union child names and type
// codes, dictionary index/value types and orderedness, and map keys_sorted.

#include <string>
#include <vector>

#include <arrow/result.h>
#include <arrow/type.h>

#include "vgi_rpc/export.h"

namespace vgi_rpc {

/// One top-level schema field as it appears in the hash preimage.
///
/// Strings and booleans only, so the preimage carries no JSON numbers.
struct VGI_RPC_EXPORT FieldToken {
    std::string name;
    bool nullable;
    std::string type;
};

/// Return the canonical token for `field`'s type.
///
/// Returns a status rather than falling back to `ToString()`: a port that
/// silently spelled an unknown type its own way would produce a protocol hash
/// that disagrees with every other port, and the disagreement would surface as
/// an unexplained mismatch at a client rather than as an error here.
VGI_RPC_EXPORT arrow::Result<std::string> TypeToken(const arrow::Field& field);

/// Describe a schema's fields in declaration order, which is significant.
VGI_RPC_EXPORT arrow::Result<std::vector<FieldToken>> SchemaTokens(const arrow::Schema* schema);

}  // namespace vgi_rpc
