# Native-client benchmark

Measures the C++ worker's RPC rate with the **Rust** `vgi-rpc-client`, so the
number reflects the server rather than the client driving it.

`scripts/benchmark_transports.py` measures the same thing through the Python
reference client, which is the right tool for "what does an application see"
but the wrong one for "how fast is this server": at ~22 us for a call that
does nothing, most of the sample is the client. A server-side change smaller
than that is invisible to it.

Requires a sibling checkout of
[vgi-rpc-rust](https://github.com/Query-farm/vgi-rpc-rust) at `../vgi-rpc-rust`.

```bash
cargo run --release --manifest-path bench/rust-client/Cargo.toml -- \
    --worker build-release/conformance/conformance_worker
```

Benchmark a **release** build of the worker. A debug build is roughly 2x
slower and will quietly understate everything.
