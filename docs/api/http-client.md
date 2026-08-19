# HTTP client

`HttpClient` is the native C++ client for unary calls and typed bidirectional
exchanges over VGI-RPC's HTTP transport.

```cpp
#include <vgi_rpc/http_client.h>

vgi_rpc::HttpClientConfig config;
config.prefix = "/vgi";
config.max_request_bytes = 8 * 1024 * 1024;
config.max_response_bytes = 8 * 1024 * 1024;
config.compression_level = 3;  // std::nullopt sends identity requests

vgi_rpc::HttpClient client("http://127.0.0.1:8000", config);
auto session = client.open_exchange("transform", init_request,
                                    input_schema, output_schema);
auto output = session.exchange(input);
session.close();
```

## Exchange contract

`open_exchange` sends exactly one caller-provided init batch and requires the
worker to return both the opaque call token and continuation cursor. The
typed conformance contract uses a zero-row, empty-schema init batch. Each
`exchange` sends exactly one batch and returns exactly one batch. A zero-row
batch is data, not end-of-stream. Input and output schemas are compared with
field and schema metadata enabled, so dictionary index widths, timestamp
timezones, decimal precision and scale, nested fields, and nullability are
preserved exactly.

An exchange session is poisoned as soon as an exchange request starts. If
transport or response parsing fails, the old cursor is never retried because
the worker may already have advanced it. Start a new session after such a
failure.

`close()` and the destructor only release local state and perform no network
I/O. `cancel()` performs one best-effort cancellation request and then closes
the local session. All three operations are idempotent.

## Limits and transport scope

Request and response caps are mandatory positive byte counts. Requests are
measured with an Arrow IPC counting pass before the output buffer is allocated.
`max_response_bytes` remains the compatibility default for two independent
limits: `max_encoded_response_bytes` bounds bytes accumulated from the network,
and `max_decoded_response_bytes` bounds the body after HTTP content decoding.
Leave either specific limit at zero to inherit `max_response_bytes`. Both limits
apply to fixed-length and chunked responses.

Request bodies use zstd level 3 by default and carry both the standard
`Accept-Encoding` header and VGI's zstd-first preference header. Set
`compression_level = std::nullopt` to send identity request bodies. The client
streams compression into a capped buffer, so an incompressible request cannot
expand past `max_request_bytes` or reserve `compressBound` bytes speculatively. The client
decodes zstd responses with both an output ceiling and a decoder-window ceiling,
so a small compressed response cannot trigger unbounded output or history-window
allocation. An unsupported request codec response (`415`) disables request
compression and is retried once with the same logical request ID. A
present-but-empty `VGI-Supported-Encodings` advertisement likewise disables
compression for later requests; an absent header retains the legacy assumption
that the peer supports zstd.

`capabilities()` performs and caches `OPTIONS {prefix}/health`. It exposes sticky,
externalization, byte-limit, upload-URL, and supported-encoding advertisements.
Capability headers on ordinary responses refine the same internal model and the
server request limit is enforced on later calls.

This API supports plain `http://` origins only. It does not provide TLS,
external-location fetching, shared-memory pointer resolution, producer
iteration, code generation, or per-call deadlines. Authentication headers
are rejected by default because they would cross a cleartext connection;
`allow_insecure_credentials` is an explicit opt-in for an already protected
local or private channel.

Header names and values containing CR/LF are rejected, and transport-reserved
headers cannot be overridden. Application metadata using reserved method,
version, request-ID, state, call-state, or cancellation keys is stripped before
the client installs its own protocol metadata. A single generated request ID is
written into both Arrow `vgi_rpc.request_id` metadata and `X-Request-ID`; the same
ID is preserved across the safe 415 identity retry.

## Errors and ownership

`HttpClientError` reports transport, HTTP, content-type, framing, limit, and
IPC parse failures. Its `http_status()` is zero when no response status was
available. Arrow exception envelopes are surfaced as `RpcRemoteError`, which
also exposes the remote exception type, error kind, server ID, and request ID.

Returned batches own their Arrow IPC backing buffer and remain valid after a
later call and after the client or parser that produced them is destroyed.
Log and exception envelopes are protocol control only when their batch has zero
rows. Log-looking metadata on a non-empty application batch is preserved as
application data. External-location and shared-memory pointer metadata remains
fail-closed over this client.
