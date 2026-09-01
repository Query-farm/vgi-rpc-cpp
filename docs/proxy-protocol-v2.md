# Trusted PROXY protocol v2 listeners

`TcpServerOptions` lets a raw VGI worker sit behind a trusted TCP proxy without
losing the original source and destination socket addresses. It does not make
an asserted address an authenticated user. An optional connection identity
resolver can use the trusted assertion as input to Tailscale LocalAPI, SPIFFE,
or another authority and returns the `AuthContext` and `PeerEvidenceSet`
snapshotted for the connection.

```cpp
vgi_rpc::TcpServerOptions options;
options.proxy_protocol_v2_required = true;
options.trusted_proxy_addresses = {"127.0.0.1"};
options.proxy_preamble_timeout = std::chrono::milliseconds(500);
options.connection_setup_timeout = std::chrono::seconds(5);
options.idle_read_timeout = std::chrono::seconds(60);
options.write_timeout = std::chrono::seconds(60);
options.maximum_active_connections = 32;
options.maximum_pending_connections = 128;
options.service_name = "svc:vgi-analytics";
options.resolve_identity = [](const vgi_rpc::PeerResolutionContext& peer) {
    // Resolve peer.asserted_peer through the configured identity authority,
    // apply the worker's authentication policy, and return one immutable
    // connection snapshot. The callback must honor peer.deadline.
    return vgi_rpc::TcpServerOptions::ResolvedIdentity{};
};
server->serve_tcp("127.0.0.1", 9400, options);
```

An Iroh-to-TCP bridge uses the same listener with an explicit local issuer and
post-merge policy:

```cpp
options.iroh_proxy_issuer = "production-mesh";
options.peer_authentication_policy = vgi_rpc::peer_identity_primary("iroh");
```

That opt-in permits only `PROXY` + `UNSPEC` carrying exactly one experimental
TLV type `0xE0`. Its payload is version byte `1` followed by the raw 32-byte
Iroh EndpointId. The listener renders the subject as exactly 64 lowercase hex
characters. The issuer remains worker-local configuration; no preamble field
can choose it. Evidence has `configured_proxy` assurance and the attribute
`original_assurance=cryptographic_peer`, preserving that the bridge verified
the Iroh connection while the worker verified only the adjacent forwarding
hop.

The listener:

- checks the accepted socket's immediate peer against exact configured IP
  addresses before reading any asserted data;
- accepts PROXY protocol version 2 only;
- accepts the `PROXY` command with TCP over IPv4 or IPv6 only by default;
- rejects `LOCAL`, `UNSPEC`, UDP, Unix addresses, malformed lengths,
  truncation, and oversized preambles;
- structurally validates and ignores bounded unknown TLVs;
- applies an independent preamble deadline; and
- applies one absolute setup deadline from `accept()` through the complete
  first VGI request frame, so slow byte-by-byte setup cannot renew the budget;
- applies a bounded idle deadline to every later socket read;
- applies a bounded deadline to each response write so a peer that stops
  reading cannot hold an active worker indefinitely;
- admits at most `maximum_active_connections + maximum_pending_connections`
  sockets and immediately rejects excess connections without stalling the
  accept loop; and
- reads exactly the declared preamble, leaving all following VGI bytes for the
  Arrow IPC reader.

The ordinary `parse_proxy_protocol_v2` overload remains strict. Only
`ProxyProtocolV2ParseOptions::allow_iroh_identity` (used automatically by a
non-empty `iroh_proxy_issuer`) enables the dedicated UNSPEC form. Missing,
duplicate, wrong-version, wrong-sized, or IP-family Iroh TLVs fail closed. The
immediate peer is checked against the exact trusted IP allowlist before any
preamble byte is consumed, and Iroh forwarding requires
`proxy_protocol_v2_required=true`.

The production defaults are 32 active connections, 128 additional admitted
connections, a five-second complete setup budget, and 60-second read/write
budgets. Tune active capacity to the number and cost of calls the worker can
actually execute; increasing the listen backlog does not increase safe worker
capacity. A persistent connection releases its permit on EOF, timeout,
framing failure, provider rejection, or listener drain. Long-running handler
execution is application work and is not interrupted by the socket idle-read
timer.

Identity provider availability remains an authentication-policy input. A
resolver should put provider timeout or capacity exhaustion in the
`PeerEvidenceSet` as a result with `PeerIdentityStatus::UNAVAILABLE`, then run
`observe_peer_identity`, `any_of_peer_identities`, or its configured policy
against the existing application `AuthContext`. That permits observation and
explicit `any_of` fallback without turning invalid credentials into fallback.
If `resolve_identity` throws instead, raw TCP deliberately fails closed and
rejects that connection.

Normal transport logs record only generic rejection/closure events. Provider,
callback, and framing exception messages are not emitted because they can
contain credentials or peer-controlled values. Put detailed diagnostics in a
separately access-controlled callback/metrics path with explicit redaction.

The backend must not be reachable except through the configured proxy. Trusting
loopback means trusting other processes on the same host, so loopback is not
enabled implicitly.

Without `proxy_protocol_v2_required`, the same overload can run a direct-TCP
identity resolver from the accepted source/destination socket snapshot. The
two-argument `serve_tcp` overload remains unchanged and performs no identity
resolution, while retaining the bounded admission and timeout defaults.
