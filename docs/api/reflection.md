# Discovering protocols

Every server co-hosts the framework protocol `vgi_rpc.Reflection.v1` beside its
application protocols. A client asks it what is hosted, and for any one
protocol's methods and schemas, through the connection it already holds — on
every transport:

```cpp
#include <vgi_rpc/client.h>

vgi_rpc::RpcClientOptions options;
options.protocol = "Calculator";
auto client = vgi_rpc::RpcClient::spawn({"python", "worker.py"}, options);

for (const vgi_rpc::HostedProtocol& p : client.list_protocols()) {
    std::cout << p.name << ' ' << p.version << ' ' << p.hash << '\n';
}
vgi_rpc::ServiceDescription desc = client.describe_protocol("Calculator");
for (const auto& [name, method] : desc.methods) {
    std::cout << name << ": " << method.method_type << '\n';
}
client.call_unary("add", params);  // the connection is still yours
```

The same two members exist on `RpcClient` (spawned, caller-owned streams,
Unix, named pipe, TCP and raw Iroh), `HttpClient` (HTTP, HTTPS and
`httpi://`) and `HttpSessionView`:

| Member | Returns | Round trips |
|---|---|---|
| `list_protocols()` | `std::vector<HostedProtocol>`, in the server's order | 1 |
| `describe_protocol(name)` | `ServiceDescription` | 2: `list_protocols`, then `describe(name)` |
| `describe()` | the application protocol's `ServiceDescription` | 2 |

## The held connection

Both reuse the client's own connection and never close it. An `RpcClient`
sends the reflection call over its own byte stream — the server routes each
request by its `vgi_rpc.protocol` key, so the client's own
`RpcClientOptions::protocol` does not matter — and, being
single-call-at-a-time, cannot ask while a stream is open. An `HttpClient` or
`HttpSessionView` posts to `{prefix}/vgi_rpc.Reflection.v1/{method}` with its
own connection pool, prefix, credentials, retry policy, sticky session and
response budget. Nothing new is opened, and there is no public way to address
`vgi_rpc.Reflection.v1` directly: the protocol's service definition stays
internal.

## `HostedProtocol`

| Field | Meaning |
|---|---|
| `name` | The wire name, which is the routing key and carries the major version. |
| `version` | The declared semver, or `""` when the protocol declares none. |
| `hash` | SHA-256 of the canonical description, as 64 lowercase hex characters. |
| `deprecated` | Whether callers should migrate off it (default `false`). |
| `deprecation_message` | What to migrate to (default `""`). |
| `features` | Capability tokens the protocol announces (default empty). |

The order is the server's: application protocols in registration order, the
primary first, then the framework's own (`vgi_rpc.Reflection.v1`, then
`vgi_rpc.Identity.v1` where it is hosted). Equal hashes mean an identical wire
surface in any port, so a client that cached a description under a hash can
skip `describe_protocol()`. Use the listing to discover an optional protocol
before calling it, rather than calling it and reading an error.

## Errors

A name the server does not host is the server's own answer: an
`RpcException` (`RpcRemoteError` over HTTP) whose `error_kind()` is
`"protocol_not_supported"`. `describe_protocol()` lists first precisely so that
this stays distinct from the next case.

**A server without reflection** — a Python reference server built without
`enable_describe=True` (its default), or any server older than reflection —
answers "not hosted": `protocol_not_supported`, an unknown method
(`method_not_implemented`), code `UNIMPLEMENTED`, or over HTTP a bare 404 from
a server older than protocol-scoped routes. All three members throw
`ReflectionNotSupportedError` for that answer and for nothing else. It is an
`RpcException` on every transport, HTTP included, carrying the server's
fields (`exception_type()`, `what()`, `error_code()`, `error_kind()`,
`error_details()`, `server_id()`, `request_id()`), plus `http_status()` (0 on a
byte stream). No listing is ever inferred, since only the caller knows which
protocol it expected the server to speak, and the connection remains usable.

This port's server hosts reflection unconditionally; there is no option to
turn it off.
