#pragma once

#include <stdexcept>
#include <string>

#include <arrow/result.h>
#include <arrow/status.h>

namespace vgi_rpc {

// Unwrap an arrow::Result<T>, throwing std::runtime_error on failure.
template <typename T>
T unwrap(arrow::Result<T>&& result) {
    if (!result.ok()) {
        throw std::runtime_error(result.status().ToString());
    }
    return std::move(result).ValueUnsafe();
}

// Unwrap with a context message prepended on failure.
template <typename T>
T unwrap(arrow::Result<T>&& result, const char* context) {
    if (!result.ok()) {
        throw std::runtime_error(std::string(context) + ": " +
                                 result.status().ToString());
    }
    return std::move(result).ValueUnsafe();
}

}  // namespace vgi_rpc

// Check an arrow::Status expression and throw std::runtime_error on failure.
#define VGI_RPC_THROW_NOT_OK(expr)                             \
    do {                                                       \
        ::arrow::Status _vgi_rpc_status = (expr);              \
        if (!_vgi_rpc_status.ok()) {                           \
            throw std::runtime_error(_vgi_rpc_status.ToString()); \
        }                                                      \
    } while (0)
