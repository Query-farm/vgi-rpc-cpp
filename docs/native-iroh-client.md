# Native Iroh client

Build with `VGI_RPC_WITH_IROH_CABI=ON` and provide the version-matched `vgi_iroh.h` and
`vgi_iroh_cabi` library. Beginning with the first release after v0.23.3,
[vgi-rpc-rust releases](https://github.com/Query-farm/vgi-rpc-rust/releases) provide static-library
archives for supported native targets. Extract one and pass its prefix as
`-DVGI_RPC_IROH_CABI_ROOT=/path/to/vgi-iroh-cabi-vX.Y.Z-target`. CMake also honors explicit
`VGI_IROH_CABI_INCLUDE_DIR` and `VGI_IROH_CABI_LIBRARY` values for unpackaged POSIX source builds.
When `VGI_RPC_IROH_CABI_ROOT` is set, discovery is constrained to that prefix and fails closed if its
header, library, CMake target, or native link dependencies are incomplete.
`RpcClient::connect_iroh` with
`native_iroh_transport_provider()` then speaks raw `vgi-rpc/arrow-mux/1` in-process. The same C ABI
also backs `HttpClient` for `httpi://` URLs using `iroh-http/2`. No executable is downloaded or
spawned.

The native provider holds a process-lifetime endpoint pool. Every implicit endpoint instance is
derived from one private process-generated key, giving it the same local EndpointId even when relay
or timeout settings require another native endpoint instance. Explicit `secret_key` values remain
separate configured identities. `remote_relay_url` and `direct_addresses` are per-remote route hints
and do not change local endpoint configuration.

Use the ordinary typed HTTP client with an Iroh URL:

```cpp
vgi_rpc::IrohTransportOptions transport;
transport.remote_relay_url = "https://relay.example";

auto client = vgi_rpc::HttpClient::builder("httpi://<64-hex-endpoint-id>/vgi")
                  .iroh_transport_options(std::move(transport))
                  .build();
auto description = client.describe();
```

The URI base path is the RPC prefix: `/vgi` above produces `/vgi/health`, `/vgi/__describe__`, and
`/vgi/<method>` requests. A URI without a path uses bare `/<method>` routes. A later builder
`prefix()` or `config()` call is an explicit override.

Only the HTTP carrier changes. Capability discovery, request/response size negotiation, Arrow IPC,
compression, authentication callbacks, sticky sessions, continuations, typed exchange/producer
state, retry gates, and external-location policy all remain in the existing `HttpClient` state
machine. Each request uses a fresh HTTP/1.1 exchange over `iroh-http/2`, verifies the authenticated
remote EndpointId, and bounds response headers and body while reading. `CallOptions` deadlines and
stop tokens are combined with `IrohTransportOptions::cancel_check`.

`httpi://` is already encrypted and mutually authenticates Iroh EndpointIds, so credential headers
are allowed without `allow_insecure_credentials`. TLS settings and TCP SOCKS proxies do not apply and
are rejected. The remote worker still decides authorization; when using `vgi-iroh-bridge`, configure
the worker to trust only that adjacent bridge's forwarded-Iroh identity boundary.

Set `IrohTransportOptions::cancel_check` when a caller needs cooperative cancellation. Native open
and write operations use the C ABI cancellation callbacks, while reads poll a bounded native timeout.
The callback must be non-blocking; exceptions are treated as cancellation. Raw Arrow I/O failures
retain the C ABI stage, category, dispatch certainty, and native message in `IrohStatusDetail`;
HTTP-over-Iroh failures map those dimensions onto the existing `HttpClientErrorKind` contract.

The installed CMake target links the stable `vgi_iroh::cabi` target name. The package configuration
rediscovers the header and library on the consumer machine, so exported targets contain no build-host
archive path. Distributions can place those two artifacts under their own relocatable prefix.
