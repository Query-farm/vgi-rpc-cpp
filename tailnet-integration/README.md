# C++ live-Tailnet qualification adapter

`vgi-rpc-tailnet-cpp` is a cross-language conformance tool. It is not a
production proxy, ingress, or alternate worker API.

The adapter exposes only transport surfaces implemented by the C++ SDK:

| Mode | Supported | Notes |
| --- | --- | --- |
| `client-tcp` direct | yes | Raw VGI over Tailnet TCP |
| `client-tcp` via SOCKS5h | yes | Proxy-side DNS; no direct fallback |
| `client-http` direct | yes | HTTPS and spoofed Serve-header assertion |
| `client-http` via SOCKS5h | yes | Proxy-side DNS; no direct fallback |
| `server-tcp` | yes | LocalAPI WhoIs with primary Tailnet auth; optional required PROXY v2 and Service target |
| `server-http` | yes | Tailscale Serve headers from explicitly trusted proxy IPs; capability-only requests remain anonymous |

For a Tailscale Service qualification, `server-tcp` accepts
`--proxy-protocol-v2 --trusted-proxy-address <exact-ip> --service-name svc:...`.
The trusted address is the immediate PROXY sender, and the listener rejects
connections without a valid PROXY v2 preamble.

Build locally with `-DVGI_RPC_BUILD_TAILNET_ADAPTER=ON`, or build the container:

```console
docker build -f tailnet-integration/Dockerfile -t vgi-rpc-tailnet-cpp:local .
```

Run `vgi-rpc-tailnet-cpp --self-test` for the focused, network-free assertion
tests.
