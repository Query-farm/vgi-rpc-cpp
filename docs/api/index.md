# API Reference

All public types live in the `vgi_rpc` namespace. Wire protocol metadata keys are in `vgi_rpc::keys`.

## Headers

| Header | Contents |
|---|---|
| [`server.h`](server.md) | `ServerBuilder`, `Server`, `MethodInfo`, `MethodType` |
| [`request.h`](request.md) | `Request` — typed parameter extraction |
| [`result.h`](result.md) | `Result` — unary response construction |
| [`stream.h`](stream.md) | `StreamState`, `ProducerState`, `ExchangeState`, `Stream` |
| [`call_context.h`](call-context.md) | `CallContext` — per-request context and logging |
| [`client.h`](raw-client.md) | `RpcClient`, raw transports, producer/exchange sessions, shared memory |
| [`http_client.h`](http-client.md) | `HttpClient`, secure HTTP calls, streams, and transport controls |
| [`wire.h`](wire.md) | IPC stream I/O, `BatchType`, `IpcStreamContents` |
| [`arrow_utils.h`, `metadata.h`, `log.h`](utilities.md) | Error handling, protocol constants, log types |
