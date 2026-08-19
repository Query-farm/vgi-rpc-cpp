# HTTP client

`HttpClient` is the native C++ client for unary calls and typed bidirectional
exchanges over VGI-RPC's HTTP transport.

```cpp
#include <vgi_rpc/http_client.h>

vgi_rpc::HttpClientConfig config;
config.prefix = "/vgi";
config.max_request_bytes = 8 * 1024 * 1024;
config.max_response_bytes = 8 * 1024 * 1024;

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
measured with an Arrow IPC counting pass before the output buffer is
allocated. Responses are accumulated through a bounded receiver. The client
requests `identity` encoding and rejects compressed responses; it does not
currently offer a separate decoded-response cap.

This API supports plain `http://` origins only. It does not provide TLS,
external-location fetching, shared-memory pointer resolution, producer
iteration, code generation, or per-call deadlines. Authentication headers
are rejected by default because they would cross a cleartext connection;
`allow_insecure_credentials` is an explicit opt-in for an already protected
local or private channel.

Header names and values containing CR/LF are rejected, and transport-reserved
headers cannot be overridden. Application metadata using reserved method,
version, state, call-state, or cancellation keys is stripped before the
client installs its own protocol metadata.

## Errors and ownership

`HttpClientError` reports transport, HTTP, content-type, framing, limit, and
IPC parse failures. Its `http_status()` is zero when no response status was
available. Arrow exception envelopes are surfaced as `RpcRemoteError`, which
also exposes the remote exception type, error kind, server ID, and request ID.

Returned batches own their Arrow IPC backing buffer and remain valid after a
later call and after the client or parser that produced them is destroyed.
