# Errors

`#include <vgi_rpc/errors.h>`

Every error batch carries the three layers of the vgi-rpc error model
(WIRE_PROTOCOL §8), adopted from gRPC's `google.rpc.Status`:

| Layer | Wire key | Set |
|---|---|---|
| Code | `vgi_rpc.error_code` | Closed: `vgi_rpc::Code`, gRPC's sixteen non-`OK` codes, sent by name |
| Reason | `vgi_rpc.error_kind` | Open; what a client branches on |
| Details | `vgi_rpc.error_details` | JSON array from a fixed catalog, at most 4 KiB |

All three are mirrored in `log_extra`, and failed access-log records carry
`error_code`.

## Raising errors

An ordinary exception from a handler reaches the client with its mapped
`error_type` (`std::invalid_argument` → `ValueError`, …) and the code
`UNKNOWN`. To choose the code, the reason and the details, throw
`StatusError`:

```cpp
throw vgi_rpc::StatusError("report is being rebuilt", vgi_rpc::Code::UNAVAILABLE,
                           "report_rebuilding", {vgi_rpc::RetryInfo{30}});
```

Details are validated when the error is built: each type at most once, and
nothing invented under the reserved `vgi_rpc.` prefix. A protocol-defined
detail type is named under the protocol's own name and passed through the
`nlohmann::json` constructor. A details array over 4096 UTF-8 bytes is
dropped **whole** on emission; the code and kind are still sent.

The catalog: `ErrorInfo`, `RetryInfo`, `BadRequest`, `PreconditionFailure`,
`QuotaFailure`, `ResourceInfo`, `Help`, `LocalizedMessage`.

Framework errors carry their codes already: `method_not_implemented` and
`protocol_not_supported` → `UNIMPLEMENTED`, `protocol_not_specified` →
`INVALID_ARGUMENT`, `protocol_version_mismatch` → `FAILED_PRECONDITION` (with
a `PreconditionFailure` naming the protocol), `session_lost` → `ABORTED`,
`server_draining` → `UNAVAILABLE` (with `RetryInfo`), and the identity kinds
as in WIRE_PROTOCOL §16.

## Transient authentication failures

`AuthUnavailableError(detail, retry_after)` is this port's "could not find
out" error. The HTTP transport answers it with `503` and that
`Retry-After` (`PeerIdentityUnavailable` derives from it). Thrown from a
`vgi_rpc.Identity.v1` hook (`resolve_token` / `mint_grant`), it is
translated to `identity_unavailable` keeping the same retry hint as
`RetryInfo`. A hook that calls the same store an authenticator calls should
throw it. Do not throw `std::invalid_argument` for an outage: that is a
`ValueError` on the wire, and callers read it as "your input was wrong".

## Reading errors (clients)

`RpcException` (raw transports) and `RpcRemoteError` (HTTP) both derive from
`RemoteStatus`:

```cpp
try {
    client.call("fetch", params);
} catch (const vgi_rpc::RpcException& e) {
    if (e.is_retryable()) {
        auto delay = e.retry_info() ? e.retry_info()->retry_delay_seconds : 1.0;
        // schedule a retry after `delay`
    }
    if (e.error_kind() == "report_rebuilding") { /* ... */ }
}
```

- `error_code()` is the code's name, or `""` when the server predates the
  model; `code()` reads both `""` and an unrecognised value as
  `Code::UNKNOWN`.
- `error_details()` is the array as received, with unknown types included.
  `details()` and the typed accessors (`error_info()`, `retry_info()`,
  `bad_request()`, `precondition_failure()`, `quota_failure()`,
  `resource_info()`, `help()`, `localized_message()`) skip types and malformed
  entries they do not understand.
- `is_retryable()` is true for `UNAVAILABLE`, and for `RESOURCE_EXHAUSTED`
  when it carries `RetryInfo`. It classifies the error and does nothing else:
  the client never retries an RPC error on its own, because a method may not
  be idempotent.
- `remote_traceback()` is the server's traceback when it sent one (by
  default every server does, on every transport).
