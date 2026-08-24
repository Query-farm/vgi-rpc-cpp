---
description: "vgi-rpc C++: a C++20 RPC framework built on Apache Arrow IPC — unary, producer, and exchange method patterns over pipe, Unix socket, TCP, and HTTP transports, with a shared-memory side channel."
hide:
  - navigation
  - toc
---

<div class="hero" markdown>

<div class="hero-logo" markdown>
![vgi-rpc C++ logo](assets/logo-hero.png){ .hero-logo-img }
</div>

# vgi-rpc C++

C++ implementation of the [vgi_rpc](https://vgi-rpc.query.farm/) framework — Apache Arrow IPC-based RPC for high-performance data services.

<p class="built-by">Built by <a href="https://query.farm">Query.Farm</a></p>

</div>

Define RPC methods with typed C++20 handlers using Arrow schemas. The framework provides server dispatch with automatic parameter extraction and result serialization over four transports: stdin/stdout pipes, Unix domain sockets, TCP, and HTTP.

## Key Features

- **Unary RPCs** with typed parameter extraction via `get<T>(name)`
- **Producer streams** for server-initiated batch data flows
- **Exchange streams** for bidirectional batch processing
- **Client-directed logging** at configurable levels
- **Introspection** via optional `__describe__` method
- **Error handling** — exceptions automatically converted to protocol error responses
- **Builder pattern** — fluent `ServerBuilder` API for registering methods
- **Access log** — JSONL records per call, with the spec's field-shedding size cap

## Transports

| Transport | Entry point | Discovery line |
|---|---|---|
| Pipe (stdin/stdout) | `Server::run()` | — |
| Unix domain socket | `Server::serve_unix(path)` | `UNIX:<path>` |
| TCP (trusted networks; no auth or TLS) | `Server::serve_tcp(host, port)` | `TCP:<host>:<port>` |
| HTTP | `Server::serve_http(HttpConfig)` | `PORT:<port>` |

Pipe, Unix, and TCP share the same raw Arrow IPC framing and differ only in
the socket they read and write. HTTP maps the same protocol onto stateless
request/response pairs and carries the optional features below.

### Shared memory

A **side channel**, not a transport of its own: it rides alongside a pipe or
socket, which still carries control messages and small batches while a large
batch is written into a POSIX segment and replaced on the wire by a zero-row
pointer batch. Enable it with `ServerBuilder::enable_transport_options()`,
which answers the `__transport_options__` handshake — a worker that stays
silent is read as "no shared memory" and peers stay inline.

Batches below `VGI_RPC_SHM_MIN_BATCH_BYTES` (128 KiB by default) stay inline,
because the fixed cost of an allocation, a pointer round trip, and the peer's
resolve and free only pays off once the copy it avoids is large enough.

## HTTP features

All off by default — a server built with `HttpConfig{}` is byte-identical on
the wire to one built before any of them existed.

| Feature | Configuration |
|---|---|
| Capability discovery (`GET`/`OPTIONS {prefix}/health`) | always on |
| Response caps, strict-fail | `max_response_bytes`, `max_externalized_response_bytes` |
| External locations (pointer batches) | `external_storage_url`, `externalize_threshold` |
| Bounded request decoding and response negotiation (zstd + gzip) | `compression` |
| CORS, including `Cross-Origin-Resource-Policy` | `cors_origin` |
| Sticky sessions (AEAD-sealed tokens, TTL, drain) | `sticky`, `sticky_default_ttl`, `sticky_echo_headers` |
| Standardized 401s with `VGI-Auth-Reason` | `reject_all` |
| Proxy proof (HMAC-SHA256 proof-of-hop) | `proof_mode`, `proof_origin_id`, `proof_secrets` |
| Token introspection | `token_introspection` |

Stream state travels as two tokens split by lifetime — a call token minted
once by `/init` and a cursor re-minted every turn — so a continuation does not
re-serialize the fixed half of the call.

### External storage

A batch over `externalize_threshold` is uploaded and replaced on the wire by a
zero-row pointer batch carrying a URL the client re-fetches. `external_storage_url`
picks the backend by scheme:

| Scheme | Backend | Build |
|---|---|---|
| `http(s)://` | A service speaking the four-endpoint alloc/PUT/HEAD/GET contract | always |
| `s3://bucket/prefix` | AWS S3 via `aws-sdk-cpp` | `-DVGI_RPC_WITH_S3=ON` |
| `gs://bucket/prefix` | Google Cloud Storage via `google-cloud-cpp` | `-DVGI_RPC_WITH_GCS=ON` |

The cloud backends are **off by default** — both SDKs are long builds, and a
deployment that externalises through its own HTTPS service needs neither. Turn
them on together with the matching vcpkg manifest features:

```bash
cmake --preset default \
  -DVCPKG_MANIFEST_FEATURES="s3;gcs" \
  -DVGI_RPC_WITH_S3=ON -DVGI_RPC_WITH_GCS=ON
```

A URI naming a backend the binary was not built with is refused **at startup**,
not on the first payload large enough to externalise.

What a pointer batch carries is always a **pre-signed HTTPS URL**, never a
bucket path, so the client fetches it holding no cloud credentials and linking
no SDK. `signed_url_ttl_seconds` bounds how long a leaked pointer stays usable.
Uploaded objects are never deleted — set a lifecycle rule on the bucket.

## Three Method Types

### Unary

A single request produces a single response. The client sends parameters, the server returns a result.

```
Client  ──  add(a=2, b=3)  ──▸  Server
Client  ◂──     5.0         ──  Server
```

### Producer

The server pushes batches to the client until calling `out.finish()`:

```
Client  ──  produce_n(count=3)  ──▸  Server
Client  ◂──  {index: [0]}       ──  Server
Client  ◂──  {index: [1]}       ──  Server
Client  ◂──  {index: [2]}       ──  Server
Client  ◂──    [finish]         ──  Server
```

### Exchange

Lockstep bidirectional streaming — one request, one response, repeat:

```
Client  ──  exchange_scale(factor=2.5)  ──▸  Server
Client  ──    {value: [10.0]}           ──▸  Server
Client  ◂──   {value: [25.0]}          ──  Server
Client  ──    {value: [4.0]}            ──▸  Server
Client  ◂──   {value: [10.0]}          ──  Server
Client  ──      [close]                 ──▸  Server
```

## Quick Example

```cpp title="examples/quick_example.cpp"
--8<-- "examples/quick_example.cpp"
```

## Next Steps

- Read the [Getting Started](getting-started.md) guide for build instructions and setup
- Browse the [Examples](examples/index.md) for hello world, calculator, and streaming
- Check out the [API Reference](api/index.md) for all classes and functions
- Learn about the [wire protocol](https://vgi-rpc.query.farm/wire-protocol) and [benchmarks](https://vgi-rpc.query.farm/benchmarks) on the main vgi-rpc site
- See all [language implementations](https://vgi-rpc.query.farm/#languages) — Python, Go, TypeScript, C++

---

<p style="text-align: center; opacity: 0.7;">
  <a href="https://vgi-rpc.query.farm">vgi-rpc</a> &middot; <a href="https://query.farm">Query.Farm</a>
</p>
