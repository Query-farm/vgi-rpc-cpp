# Native Iroh client

Build with `VGI_RPC_WITH_IROH_CABI=ON` and provide the version-matched `vgi_iroh.h` and
`vgi_iroh_cabi` library. `RpcClient::connect_iroh` with
`native_iroh_transport_provider()` then speaks raw `vgi-rpc/arrow-mux/1` in-process. No executable is
downloaded or spawned.

The native provider holds a process-lifetime endpoint pool. Every implicit endpoint instance is
derived from one private process-generated key, giving it the same local EndpointId even when relay
or timeout settings require another native endpoint instance. Explicit `secret_key` values remain
separate configured identities. `remote_relay_url` and `direct_addresses` are per-remote route hints
and do not change local endpoint configuration.

This API is deliberately raw-only. `httpi://` is parsed for cross-SDK conformance but rejected before
provider dispatch; C++ does not yet expose an `iroh-http/2` client.

Set `IrohTransportOptions::cancel_check` when a caller needs cooperative cancellation. Native open
and write operations use the C ABI cancellation callbacks, while reads poll a bounded native timeout.
The callback must be non-blocking; exceptions are treated as cancellation. Arrow I/O failures retain
the C ABI stage, category, dispatch certainty, and native message in `IrohStatusDetail`.

The installed CMake target links the stable `vgi_iroh::cabi` target name. The package configuration
rediscovers the header and library on the consumer machine, so exported targets contain no build-host
archive path. Distributions can place those two artifacts under their own relocatable prefix.
