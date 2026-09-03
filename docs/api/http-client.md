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

The same typed client accepts `httpi://<EndpointId>[/base/path]` when the
library is built with `VGI_RPC_WITH_IROH_CABI=ON`. The URI path becomes the
RPC prefix, and `iroh_transport_options()` supplies relay, direct-address,
identity-key, and timeout settings. See [Native Iroh client](../native-iroh-client.md).
HTTP-over-Iroh changes only the carrier; all behavior documented below remains
the same.

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

`RetryPolicy` carries a three-attempt schedule with 100 ms initial backoff, a
10 second maximum backoff, multiplier 2, and 20% jitter. RPC POSTs—including
producer continuations—still make one attempt by default because a transport
failure after dispatch is ambiguous. Set `CallOptions::idempotent = true` only
after the application has made that logical operation safe to replay; its
transport and configured status retries then use the policy. Exchange turns
are never retried and their session is poisoned on ambiguous failure. HTTP
status retry is off until `retryable_status_codes` is populated.
`RetryPolicy::disabled()` makes exactly one attempt in every case.

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

## Producers and resumable streams

`open_producer()` returns a move-only `HttpStreamSession`. Each `tick()` consumes
at most one application batch from the current lock-step turn before it sends a
continuation request; a metadata-free zero-row batch is application data, not
end-of-stream.
`open_stream_exchange()` provides the same generalized lifecycle for an
exchange that may terminate without a final batch, while `open_exchange()`
remains the strict one-input/one-output compatibility API.

`next_with_token()` pairs a producer batch with the opaque cursor/call-state
blob that resumes *after* that batch. `resume_stream()` starts directly from a
persisted token without replaying `/init`, and `seek_to_token()` repositions a
fresh producer session. A response containing more than one data batch violates
lock-step and is rejected by both `tick()` and `next_with_token()`. Producer
continuations use the `CallOptions` passed to those methods and are retried only
when the caller marks the turn idempotent. Exchange turns are never retried
after dispatch ambiguity.

Optional stream headers are parsed as their own IPC substream and returned by
`header()`. Externalized headers and data are resolved before cursor metadata is
interpreted. Local `close()` is non-networking; explicit `cancel()` sends one
best-effort cancellation request.

## External locations

Secure external-location resolution is enabled by default. The production
policy accepts HTTPS only, rejects URL credentials/fragments and non-public DNS
answers, validates every address, pins a validated address while retaining the
original TLS hostname, and manually revalidates every redirect. Encoded,
decoded, upload, and upload-response bytes have independent hard caps; zstd
decoding also has a bounded history window. SHA-256 covers the decoded Arrow IPC
payload, nested pointers are rejected, and signed URL query strings are removed
from diagnostics.

Use `external_http_options()` to lower limits. The
`LOOPBACK_HTTP_TEST` policy is an explicit local-test escape hatch and accepts
only loopback HTTP targets. `disable_external_locations()` restores fail-closed
pointer rejection without fetching.

Oversized requests use the server's `VGI-Max-Request-Bytes` and upload-URL
capabilities. A known-oversized request is externalized proactively; an initial
pre-dispatch 413 warms capabilities and is retried once as a pointer. The client
requests a method-bound PUT/GET pair, validates both URLs, uploads the complete
original IPC body without RPC credentials or redirects, and sends a pointer
whose SHA-256 covers that body. The external upload cap, rather than the smaller
inline cap, bounds the one required serialization allocation.

## Sticky sessions

`with_session_token()` creates an independent, move-only `HttpSessionView`
over the client's shared transport. Calls through the view send
`VGI-Session-Accept: true`, capture `VGI-Session` plus every `VGI-Echo-*`
response header, and replay the stripped routing headers on subsequent unary,
producer, exchange, capability, and upload-URL control requests. Session
routing headers override per-call values so a caller cannot accidentally route
a live token to a different worker. Separate views do not share token state.

`current_session_token()` and `current_echo_headers()` return snapshots for
persistence. Pass both back to `with_session_token(token, echo_headers)` when
resuming because some deployments require the echoed routing header to reach
the worker that owns the token. `detach()` transfers the token without deleting
it. `close()` and destruction are idempotent and attempt a bounded,
best-effort `DELETE {prefix}/__session__` for a live token. Teardown never calls
the potentially blocking authentication callback and gives up promptly if the
session or shared transport lock is busy; static authentication headers still
apply. Server TTL eviction is the fallback. A response carrying
`VGI-Session-Close: true` clears
the token and routing echoes without another teardown request.

Captured tokens and echo headers are validated and bounded before being
committed. Credential-bearing echo headers are always forbidden, including on
HTTPS. Invalid, transport-reserved, or oversized echo metadata fails as a
protocol error without partially replacing
the last usable routing state. A malformed response body likewise cannot
commit staged session headers. A valid Arrow exception envelope whose remote
type is `SessionLostError` throws the dedicated C++ `HttpSessionLostError` class.

## Limits and transport scope

Request and response caps are mandatory positive byte counts. Requests are
measured with an Arrow IPC counting pass before the output buffer is allocated.
`max_response_bytes` remains the compatibility default for the encoded network
limit. `max_encoded_response_bytes` overrides that bound, while
`max_decoded_response_bytes` optionally narrows the body after HTTP content
decoding. Leave the encoded limit at zero to use the larger of
`max_response_bytes` and `accepted_max_response_bytes`; leave the decoded limit
at zero to inherit `accepted_max_response_bytes`. Both limits apply to
fixed-length and chunked responses.

`accepted_max_response_bytes` is the protocol-level decoded Arrow IPC budget
and defaults to 256 MiB. Configure it directly or through the builder's
`accepted_max_response_bytes` method. The client advertises the effective local
identity-response limit (the minimum of accepted, encoded, and decoded limits) as
`VGI-Accept-Max-Response-Bytes` on OPTIONS and every RPC, and refuses to
dispatch unless capability discovery reports
`VGI-Accept-Max-Response-Bytes-Support: true`. The header is
transport-reserved; configure the typed option instead of adding it to a
custom-header map.

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

`capabilities()` performs and caches `OPTIONS {prefix}/health`. It exposes
response-budget support, sticky, externalization, byte-limit, upload-URL, and
supported-encoding advertisements.
Capability headers on ordinary responses refine the same internal model and the
server request limit is enforced on later calls.

`request_upload_urls(count)` exposes the `__upload_url__` control method for
counts from 1 through 100 and returns `HttpUploadUrl` values containing upload,
download, and optional microsecond expiry fields. The response schema, row
count, non-null URLs, and configured external URL policy are validated before
anything is returned. The operation fails closed when external resolution and
its URL policy are disabled. Automatic request externalization uses this same
parser.

Shared-memory pointer resolution and code generation are not provided.
Disabled or policy-rejected external locations remain fail-closed rather than
returning an unresolved pointer as application data.

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
