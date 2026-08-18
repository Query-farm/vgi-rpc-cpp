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

## Platform coverage

On Windows the `unix` and `tcp` columns are absent — the transports are not
implemented there — and `pipe+shm` is **the pipe wearing a different label**:
`shm_available()` returns false on Windows, so the shared-memory handshake
never engages and the numbers are indistinguishable from `pipe`. Read that
column as a duplicate rather than a measurement until Windows shm exists.

Measured on Windows 11 / Core Ultra 7 265 at 3103a5f, for reference: pipe
`void_noop()` 49,751 calls/sec and 2,535 MB/s at 1 MiB; http 13,774 calls/sec
and 467 MB/s. The macOS figures in `docs/benchmarks.md` are a different
machine and a different OS, so the two bound nothing about each other.
