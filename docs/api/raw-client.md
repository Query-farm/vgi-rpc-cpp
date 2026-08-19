# Raw client

`RpcClient` is the blocking native client for VGI-RPC's ordered Arrow IPC
transports. It supports a directly spawned worker, caller-owned Arrow streams,
Unix sockets, and trusted-network TCP.

```cpp
#include <vgi_rpc/client.h>

auto client = vgi_rpc::RpcClient::spawn({"python", "worker.py"});
auto service = client.describe();
auto result = client.call_unary("add", params);

auto producer = client.open_producer("scan", scan_params);
while (auto batch = producer.tick()) {
    consume(batch->batch);
}
```

The client is dynamic and schema-first: callers build exact Arrow record
batches and receive owned `AnnotatedBatch` values. `describe()` validates the
version-4 service description rather than returning a partial model when the
peer supplies malformed schemas or duplicate methods.

## Transports

`ClientTransport::spawn()` executes the argument vector directly, never via a
shell. On POSIX it uses `posix_spawnp`; on Windows it uses `CreateProcessW`
with Windows-compatible argument quoting and binary inherited pipes. Closing a
subprocess first permits cooperative EOF shutdown, then escalates through the
configured bounded termination periods.

`connect_unix()` and `connect_tcp()` are currently POSIX-only, matching the raw
server transports. Socket connect, read, and write bounds are configured with
`SocketTransportOptions`. The connect deadline applies independently to each
resolved address; platform DNS resolution itself is outside that timer.

Raw TCP has no authentication or TLS and must be confined to a trusted network.
Use [`HttpClient`](http-client.md) when either security property is required.

## Streams and ownership

Only one operation may use a raw connection at a time. A live `ClientStream`
reserves it until the stream finishes, is explicitly closed/cancelled, or is
destroyed. Producer `tick()` and bidirectional `exchange()` follow the raw
lockstep protocol; raw streams are their own continuation handle and do not
have stateless HTTP resume tokens.

`close()` and `cancel()` perform the protocol drain and can block up to the
transport's configured I/O deadline. Destruction never attempts an unbounded
drain: abandoning a live stream aborts the underlying connection. Returned
Arrow batches own their backing buffers and remain valid after later reads.

## Shared memory

Set `RpcClientOptions::shared_memory_bytes` or call `enable_shared_memory()` to
negotiate POSIX shared-memory transport through `__transport_options__`.
Negotiation is capability-gated. Large exchange inputs and returned pointer
batches are resolved transparently, and `shared_memory_live_allocations()` is
available for lifecycle diagnostics. Shared memory is disabled by default.

## Deliberate scope

The raw client rejects external-location and stateless-resume envelopes; those
belong to the HTTP client. Raw streams currently have no response-byte cap,
and subprocess pipes do not provide a per-read deadline, although subprocess
abandonment uses bounded shutdown. Windows subprocess support is native;
Windows Unix/TCP raw sockets remain unavailable with the current server.
