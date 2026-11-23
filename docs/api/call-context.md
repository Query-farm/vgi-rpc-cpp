# CallContext

`#include <vgi_rpc/call_context.h>`

Per-request context passed to method handlers and stream processors. Provides the server/request IDs and a log sink for emitting client-visible log messages.

## Methods

### `client_log`

```cpp
void client_log(LogLevel level, std::string_view message,
                const nlohmann::json& extra = {});
void client_log(const Message& msg);
```

Emit a log message to the client. The message is sent as an in-band LOG batch in the response stream. The `extra` parameter allows attaching structured JSON data to the log entry.

### `server_id`

```cpp
const std::string& server_id() const noexcept;
```

### `request_id`

```cpp
const std::string& request_id() const noexcept;
```

### `log_sink`

```cpp
std::shared_ptr<LogSink> log_sink() const noexcept;
```

Access the underlying log sink directly.

## StateRef and argument references (protocol 3.4)

Large values, per vgi-rpc's `STATE_REF_V1.md`. This port is **single-replica**
in 3.4: HTTP streams live in server memory, so a `StateRef` is always held in
memory and nothing is ever stored or fetched for one. The API is the same as in
the SDKs that do serialize stream state, so worker code stays portable.

```cpp
// In a stream method (the factory), and only there:
vgi_rpc::StateRef ref = ctx.state_ref(std::move(bytes));   // or a RecordBatch
// On any later turn, from the state the factory built:
const std::string& value = ctx.resolve(ref);
auto record = ctx.resolve_record(ref);                      // typed variant
```

- `state_ref()` anywhere else — a unary call, `process`/`produce`/`exchange`
  (the first producer turn folded into an HTTP init included), `on_cancel` —
  throws `StateRefMisuseError` (`state_ref_misuse`, `FAILED_PRECONDITION`).
  Copy the ref into the next state; never mint a second one.
- Within one init, byte-equal values (or the same record instance) return the
  same ref.
- `ref.encode()` renders the inline form (`0x01 || codec || body`, zstd when
  smaller) for state persisted for the caller's own reasons;
  `StateRef::decode()` reads it back. A stored form is refused
  (`invalid_state_ref`).

`ctx.resolve_argument_ref(ref, size, sha256)` resolves a reference minted by
this server's `__upload_url__/init` grant (its `object_id` and `ref` columns)
for the calling principal: it opens the ref, checks the sealed size and SHA-256
against the ones it was sent with, fetches the object by id through the
process-wide object cache (`VGI_RPC_OBJECT_CACHE_BYTES`, default
`min(1 GiB, 40%` of memory`)`, single-flight, entries stale after one day) and
verifies it. Errors: `invalid_arguments_ref`, `arguments_ref_integrity`,
`arguments_ref_missing`, `storage_unavailable`. Only the HTTP transport vends
grants. Grants are authenticated exactly like protocol methods, so the same
client presenting the ref opens it; on an unauthenticated server a ref is a
bearer capability for as long as the object lives.

Objects carry no TTL: set a bucket lifecycle rule of at least one day on the
storage prefix. A multi-worker deployment must share `HttpConfig::token_key`
for references to open on every worker.

## Log Levels

```cpp
enum class LogLevel {
    TRACE,
    DEBUG,
    INFO,
    WARN,
    ERROR,
    EXCEPTION
};
```

## Example

```cpp
auto handler = [](const vgi_rpc::Request& req, vgi_rpc::CallContext& ctx) {
    ctx.client_log(vgi_rpc::LogLevel::INFO, "Processing request");

    // With structured extra data
    ctx.client_log(vgi_rpc::LogLevel::DEBUG, "Input received",
                   {{"param_count", 3}, {"method", "add"}});

    // ...
};
```
