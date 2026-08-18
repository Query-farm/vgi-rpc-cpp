// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/// Exceptions that carry a machine-readable `vgi_rpc.error_kind` onto the wire.
///
/// Ordinary handler exceptions map to an error_type by their C++ class
/// (invalid_argument -> ValueError and so on).  These carry something a client
/// is expected to branch on rather than display, so the kind travels
/// explicitly instead of being inferred.
#pragma once

#include <stdexcept>
#include <string>

#include "vgi_rpc/export.h"
#include "vgi_rpc/metadata.h"

namespace vgi_rpc {

// Base for exceptions whose class is part of the wire contract.
class VGI_RPC_EXPORT KindedError : public std::runtime_error {
public:
    KindedError(std::string kind, std::string exception_type, const std::string& what)
        : std::runtime_error(what),
          kind_(std::move(kind)),
          exception_type_(std::move(exception_type)) {}

    const std::string& kind() const noexcept { return kind_; }
    const std::string& exception_type() const noexcept { return exception_type_; }

private:
    std::string kind_;
    std::string exception_type_;
};

// Map a handler exception onto the error_type the wire carries.  Ordering
// matters: invalid_argument and out_of_range are both logic_error subclasses,
// so the specific cases must be tested before the general one.
VGI_RPC_EXPORT std::string exception_type_of(const std::exception& e);

// The machine-readable error_kind, or "" for an ordinary exception.
VGI_RPC_EXPORT std::string error_kind_of(const std::exception& e);

// A sticky session could not be resolved.  Every cause — a token that will not
// decrypt, one sealed for another principal, one minted by another worker, an
// entry that aged out — raises this same error with this same message, so the
// endpoint cannot be used to probe whose sessions exist.
class VGI_RPC_EXPORT SessionLostError : public KindedError {
public:
    explicit SessionLostError(const std::string& what = "session lost")
        : KindedError(ERROR_KIND_SESSION_LOST, "SessionLostError", what) {}
};

// The worker is draining and will not open new sessions.  Distinct from
// session_lost: the client's token is fine, the server is going away.
class VGI_RPC_EXPORT ServerDrainingError : public KindedError {
public:
    explicit ServerDrainingError(const std::string& what = "server draining")
        : KindedError(ERROR_KIND_SERVER_DRAINING, "ServerDrainingError", what) {}
};

}  // namespace vgi_rpc
