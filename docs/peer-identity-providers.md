# Tailscale, Iroh, and trusted HTTP proxy identity

The C++ adapters in `tailscale_identity.h`, `iroh_identity.h`, and
`spiffe_proxy_identity.h` turn
transport evidence into `PeerIdentity`; they do not make authorization
decisions. Enable them explicitly and apply an authentication policy such as
`peer_identity_primary` or `require_peer_identity` separately.

Every header adapter requires exact immediate-proxy IP addresses. Hostnames,
CIDRs, and source ranges are rejected. The backend must not be reachable around
that proxy, and the proxy must replace or remove client-supplied identity
headers. Raw duplicate headers and case-varied duplicates fail closed. Header
evidence has `configured_proxy` assurance.

## Iroh behind an HTTP bridge

`iroh_forwarded_header_provider` consumes exactly one
`VGI-Forwarded-Iroh-Endpoint` value after the exact immediate-proxy IP check.
The value must be the canonical 64-character lowercase hexadecimal EndpointId;
uppercase, short, duplicate, case-varied duplicate, control-bearing, and
untrusted requests fail closed. The operator supplies the issuer locally.

The resulting stable endpoint identity has `configured_proxy` assurance and
`original_assurance=cryptographic_peer`: the adjacent bridge authenticated the
Iroh peer cryptographically, while the worker authenticates only its trusted
hop to that bridge. The backend must be unreachable except through the bridge,
which must strip any client-provided forwarding header before setting its own.

```cpp
vgi_rpc::HttpConfig config;
config.peer_identity_providers.push_back(vgi_rpc::iroh_forwarded_header_provider(
    {.issuer = "production-mesh", .trusted_proxy_addresses = {"127.0.0.1"}}));
config.peer_authentication_policy = vgi_rpc::peer_identity_primary("iroh");
```

## Tailscale

`tailscale_serve_identity_provider` consumes Tailscale Serve user and
application-capability headers only after the exact proxy check. Funnel is not
identity evidence. Serve logins are verified but login-stable, so they are not
eligible for the built-in stable-subject primary policy. Capability-only
evidence remains subjectless. Values accept plain ASCII or strict RFC 2047
UTF-8 Q encoding; JSON capabilities are size/depth/count bounded, reject
duplicate keys, and require application-capability arrays of objects.

`tailscale_localapi_identity_provider` issues one fresh LocalAPI WhoIs request
per resolution. It never starts the CLI, caches WhoIs, follows redirects, or
uses proxy environment variables. On Unix, configure `unix_socket`, or omit it
to use `/var/run/tailscale/tailscaled.sock`. `endpoint` accepts an explicit
plain-HTTP origin such as a macOS same-user-proof endpoint; `password` becomes
Basic auth with an empty username. Requests use `Host: local-tailscaled.sock`,
scope capabilities with `svc_name` or `dst_ip`, and are bounded by both the
provider timeout and resolution deadline. Responses have bounded headers,
body, JSON depth, and JSON values.

WhoIs assigns stable `user:<numeric UserProfile.ID>` subjects to untagged nodes.
Tagged nodes ignore `UserProfile` as caller identity and require
`node:<StableID>`. Their names and tags remain attributes. Evidence has
`local_daemon` assurance.

Native platform gaps are explicit: this release does not discover either
macOS GUI same-user-proof variant and does not connect to the Windows Tailscale
named pipe. On macOS, pass the discovered loopback `endpoint` and `password`,
or use a Unix-socket tailscaled installation. On Windows, pass an explicit
loopback HTTP endpoint. No subprocess-based discovery fallback is attempted.

## SPIFFE behind L7 proxies

All profiles produce stable workload principals only for exactly one canonical
SPIFFE URI in an allowed trust domain:

- `nginx_spiffe_provider` requires URL-escaped `X-SSL-Client-Cert` and exact
  `X-SSL-Client-Verify: SUCCESS` after nginx verifies the client chain.
- `aws_alb_spiffe_provider` accepts `X-Amzn-Mtls-Clientcert-Leaf` only under the
  operator guarantee that the adjacent ALB listener is in mTLS **verify** mode.
  Passthrough mode is outside this trust contract.
- `gcp_load_balancer_spiffe_provider` requires the custom variables for
  certificate-present, chain-verified, SPIFFE ID, and an empty error value.
- `azure_application_gateway_spiffe_provider` requires strict-mode rewrite
  headers for the client certificate and exact verification `SUCCESS`.
- `envoy_xfcc_spiffe_provider` requires adjacent Envoy mTLS with
  `forward_client_cert_details: SANITIZE_SET`. It accepts exactly one text XFCC
  element, one URI, and one SHA-256 Hash. Forwarded chains, duplicate singleton
  fields, unknown fields, and malformed percent encoding or quoting fail
  closed.

Certificate profiles parse exactly one PEM leaf and validate the X.509-SVID
profile: current validity, one allowed URI SAN, non-CA basic constraints,
critical digital-signature key usage without CA signing bits, and both client
and server auth when extended key usage is present. Certificate validation by
the adapter does not replace the proxy's chain verification.
