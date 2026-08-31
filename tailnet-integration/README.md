# C++ live-Tailnet qualification adapter

`vgi-rpc-tailnet-cpp` is a cross-language conformance tool. It is not a
production proxy, ingress, or alternate worker API.

The adapter exposes only transport surfaces implemented by the C++ SDK:

| Mode | Supported | Notes |
| --- | --- | --- |
| `client-tcp` direct | yes | Raw VGI over Tailnet TCP |
| `client-tcp` via SOCKS5h | yes | Proxy-side DNS; no direct fallback |
| `client-http` direct | yes | HTTPS and spoofed Serve-header assertion |
| `client-http` via SOCKS5h | no | `HttpClient` has no per-client proxy dialer |
| `server-tcp` | yes | Direct LocalAPI WhoIs with primary Tailnet auth |
| `server-http` | no | The Serve header provider exists, but C++ HTTP serving does not yet accept peer identity providers/policies |

Raw TCP PROXY v2 is implemented by the C++ library, but this adapter does not
claim a Tailscale Service qualification until the canonical live topology
exercises an adjacent trusted PROXY v2 sender.

Build locally with `-DVGI_RPC_BUILD_TAILNET_ADAPTER=ON`, or build the container:

```console
docker build -f tailnet-integration/Dockerfile -t vgi-rpc-tailnet-cpp:local .
```

Run `vgi-rpc-tailnet-cpp --self-test` for the focused, network-free assertion
tests.
