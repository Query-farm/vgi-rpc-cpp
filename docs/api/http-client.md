# HTTP client

`HttpClient` is the native C++ client for unary calls and typed bidirectional
exchanges over VGI-RPC's HTTP transport.

```cpp
#include <vgi_rpc/http_client.h>

auto client = vgi_rpc::HttpClient::builder("https://rpc.example.com")
                  .prefix("/vgi")
                  .custom_ca_file("/etc/my-company/ca.pem")
                  .auth_callback([](const vgi_rpc::HttpAuthRequest&) {
                      return std::map<std::string, std::string>{
                          {"Authorization", "Bearer " + current_access_token()}};
                  })
                  .build();
auto session = client.open_exchange("transform", init_request,
                                    input_schema, output_schema);
auto output = session.exchange(input);
session.close();
```

The builder is the primary API. It is copyable and owns its implementation,
so a configured builder can be copied and specialized without sharing mutable
configuration. The former `HttpClient(base_url, config)` constructor remains
as a deprecated source-compatible adapter.

`describe()` invokes `__describe__` and returns a validated
`ServiceDescription`. Malformed schemas, duplicate method names, and
unsupported describe/request versions fail closed rather than producing a
partial model.

## HTTPS and authentication

`https://` uses OpenSSL with certificate and hostname verification enabled.
With no TLS options, the platform/system trust store is used. `custom_ca_file`
adds an explicit trust anchor, and `client_certificate(cert, key)` configures
mutual TLS. A client certificate without its private key, or any TLS option on
an `http://` origin, is rejected while building the client.

`dangerous_disable_tls_verification_for_testing()` disables both chain and
hostname verification. It is deliberately test-named and must not be used in
production. RPC redirects are never followed, including HTTPS-to-HTTPS
redirects; a redirect is surfaced as a structured HTTP status error.

Static headers can be set with `header()`. An `auth_callback` is invoked once
per logical operation before the client's serialized transport lock is taken,
so refreshing credentials may block or re-enter the client without deadlock.
Per-call headers override callback headers, which override static headers.
CR/LF is rejected everywhere, and transport-owned headers such as
`Content-Type`, `Content-Encoding`, `Host`, and `X-Request-ID` cannot be
overridden. Credential-bearing headers over cleartext HTTP still require the
explicit `allow_insecure_credentials` compatibility opt-in.

## Per-call controls and retry

Every operation accepts `CallOptions`. `deadline` is an absolute
`steady_clock` time; `CallOptions::with_timeout()` creates one relative to now.
A `std::stop_token` interrupts lock acquisition, retry backoff, and in-flight
HTTP I/O. Deadline and cancellation failures are distinguished. The underlying
connection is retired when an in-flight operation is stopped, and the same
client can establish a fresh connection afterward.

`CallOptions::request_id` optionally supplies the logical request ID. The value
must be 1–256 characters without CR/LF. Otherwise the client generates one.
The client overwrites caller Arrow metadata and writes the same value into both
Arrow `vgi_rpc.request_id` and HTTP `X-Request-ID`; it remains unchanged across
connection retries and the safe 415 codec fallback.

`RetryPolicy` defaults match Rust: three attempts, 100 ms initial backoff,
10 second maximum backoff, multiplier 2, and 20% jitter. Automatic retries are
limited to operations whose protocol phase is safe to repeat. Exchange turns
are never retried after an ambiguous transport failure and their session is
poisoned. HTTP status retry is off by default; populate
`retryable_status_codes` only when the application has made the corresponding
idempotency decision. `RetryPolicy::disabled()` makes exactly one attempt.

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

The client does not yet provide external-location fetching, shared-memory
pointer resolution, producer iteration, sticky-session management, or code
generation. Pointer metadata remains fail-closed rather than returning an
unresolved pointer as application data.

Header names and values containing CR/LF are rejected, and transport-reserved
headers cannot be overridden. Application metadata using reserved method,
version, request-ID, state, call-state, or cancellation keys is stripped before
the client installs its own protocol metadata. A single generated request ID is
written into both Arrow `vgi_rpc.request_id` metadata and `X-Request-ID`; the same
ID is preserved across the safe 415 identity retry.

## Errors and ownership

`HttpClientError::kind()` distinguishes transport, TLS, timeout, cancellation,
HTTP status, authentication, protocol, limit, and remote failures. Structured
fields include status, method, request ID, a response-body excerpt,
`Retry-After`, and `VGI-Auth-Reason`. The body excerpt is capped at 4096 bytes
and diagnostic headers at 1024 bytes; credentials from requests are never
included. `HttpAuthenticationError` additionally exposes the bounded
`WWW-Authenticate` challenge. `http_status()` is zero when no response status
was available. Arrow exception envelopes are surfaced as `RpcRemoteError`,
which also exposes the remote exception type, error kind, server ID, and
request ID.

Returned batches own their Arrow IPC backing buffer and remain valid after a
later call and after the client or parser that produced them is destroyed.
Log and exception envelopes are protocol control only when their batch has zero
rows. Log-looking metadata on a non-empty application batch is preserved as
application data. External-location and shared-memory pointer metadata remains
fail-closed over this client.
