---
description: "vgi-rpc C++ transport benchmarks — unary round-trip latency, call rate, streaming, and payload throughput across pipe, Unix socket, TCP, HTTP, and shared memory."
---

# Benchmarks

Two harnesses, measuring two different things:

```bash
# What an application sees, through the Python reference client.
python scripts/benchmark_transports.py --rounds 5 --iterations 300

# What the server can do, through a native Rust client.
cargo run --release --manifest-path bench/rust-client/Cargo.toml -- \
    --worker build-release/conformance/conformance_worker --rounds 5 --iterations 300
```

**Benchmark a release build.** The `default` CMake preset is `Debug`, which is
about half the speed and understates everything on this page — plausibly
enough that it took the C++ server losing to the Python reference server to
notice. `benchmark_transports.py` now prefers `build-release/` over `build/`
for exactly that reason.

## Method

Rounds are **interleaved** across transports rather than run back to back. A
benchmark that measures each transport once, in sequence, compares whatever
the machine happened to be doing at the time as much as it compares
transports.

Headline figures are the **best** sample, not the median. Interference on a
shared machine is one-sided — it can only make a sample slower — so the
minimum is the closest available estimate of the real cost. The median table
is kept alongside it to show how much the machine added.

Measured on macOS 15.6 / Apple Silicon, 8 CPUs, load average near 4 — a
working laptop, not a quiet bench. The two harness runs below are back to back
against the same `build-release` worker, so they can be compared with each
other; absolute numbers will move with the machine, and the **ratios between
columns** are the point.

## Through the Python reference client

Encode, transport, C++ dispatch, decode. That is the number an application
sees, and it is what the Python reference's own `docs/benchmarks.md` reports,
so the two can be read side by side. It is **not** the C++ server's ceiling —
a large share of each figure is the client, as the next section shows.

### Unary round trip

_best, ms — lower is better_

|                  | pipe | unix | tcp | http | pipe+shm |
|------------------|-----:|-----:|----:|-----:|---------:|
| `void_noop()`      | 0.023 | 0.026 | 0.053 | 0.270 | 0.022 |
| `add_floats(a,b)`  | 0.035 | 0.040 | 0.063 | 0.291 | 0.036 |
| `echo_string(11B)` | 0.032 | 0.038 | 0.061 | 0.281 | 0.033 |
| `echo_all_types()` | 0.251 | 0.256 | 0.282 | 0.546 | 0.253 |

Median, to show the spread the machine added:

|                  | pipe | unix | tcp | http | pipe+shm |
|------------------|-----:|-----:|----:|-----:|---------:|
| `void_noop()`      | 0.027 | 0.030 | 0.075 | 0.308 | 0.026 |
| `add_floats(a,b)`  | 0.040 | 0.043 | 0.072 | 0.336 | 0.039 |
| `echo_string(11B)` | 0.038 | 0.041 | 0.072 | 0.333 | 0.037 |
| `echo_all_types()` | 0.266 | 0.269 | 0.307 | 0.610 | 0.266 |

### Unary rate

_calls/sec — higher is better_

|                  | pipe | unix | tcp | http | pipe+shm |
|------------------|-----:|-----:|----:|-----:|---------:|
| `void_noop()`      | 44,037 | 38,835 | 18,750 | 3,698 | 45,114 |
| `add_floats(a,b)`  | 28,951 | 25,211 | 15,769 | 3,432 | 27,972 |
| `echo_string(11B)` | 31,168 | 26,638 | 16,461 | 3,553 | 30,691 |
| `echo_all_types()` |  3,986 |  3,906 |  3,545 | 1,830 |  3,958 |

`void_noop()` is the per-call framing floor — no parameters, no result.
`echo_all_types()` carries eighteen fields covering every type mapping, so the
gap between the two is encode/decode cost rather than transport cost. That the
transports converge as the payload grows (44k → 4.0k on pipe, 3.7k → 1.8k on
HTTP) says most of the per-call spread is fixed framing overhead.

### Streaming

_best, ms — lower is better_

|                     | pipe | unix | tcp | http | pipe+shm |
|---------------------|-----:|-----:|----:|-----:|---------:|
| `produce_n(50)`       | 1.03 | 1.08 | 1.75 | 16.07 | 1.06 |
| `exchange_scale(20)`  | 0.37 | 0.53 | 0.82 |  6.71 | 0.41 |

HTTP pays worst here, and for a structural reason: the raw-framing transports
hold one connection open for the whole stream, while HTTP maps every producer
turn and every exchange onto a separate request/response pair.

### Payload throughput

_MB/s, both directions, at best time — higher is better_

|         | pipe | unix | tcp | http | pipe+shm |
|---------|-----:|-----:|----:|-----:|---------:|
| 1 KiB   |    57 |    50 |    28 |     6 |    56 |
| 64 KiB  | 2,429 | 2,488 | 1,514 |   329 | 2,320 |
| 1 MiB   | 7,152 | **12,149** | 7,519 | 1,561 | 8,979 |
| 16 MiB  | 3,580 | **5,374** | 5,210 | 1,661 | 5,373 |

The 1 KiB row is latency-bound, not bandwidth-bound — it is really just
`1 / round-trip` wearing different units. Shared memory does not engage below
`VGI_RPC_SHM_MIN_BATCH_BYTES` (128 KiB), which is why its 1 KiB and 64 KiB
figures track the plain pipe.

Unix leading at the top two sizes is new, and is the socket-buffer fix below.

## Through a native client

A Python client can only measure a server down to its own cost per call.
Driving the same worker from Rust — reusing `vgi-rpc-client` from the sibling
[vgi-rpc-rust](https://github.com/Query-farm/vgi-rpc-rust) checkout, see
`bench/rust-client/` — moves the floor by 2x to 5x:

| `void_noop()`, calls/sec | Python client | Rust client | |
|---|---:|---:|---|
| pipe | 44,037 | **102,135** | 2.3x |
| unix | 38,835 |  **87,268** | 2.2x |
| tcp  | 18,750 |  **36,474** | 1.9x |
| http |  3,698 |  **17,978** | **4.9x** |

HTTP is where the difference is starkest: the Python table reads as though
this server costs 0.27 ms per HTTP call, and most of that is the client's HTTP
stack. The server's own figure is nearer 56 us.

Throughput separates the same way — and the shared-memory column most of all,
because the channel exists to avoid copies, which is exactly what a client
spending tens of microseconds per call in Python hides:

| MB/s round trip | pipe | unix | tcp | http | pipe+shm |
|---|---:|---:|---:|---:|---:|
| 1 KiB   |   155 |   139 |    54 |    28 |   100 |
| 64 KiB  | 4,386 | 2,613 | 2,529 |   918 | 3,759 |
| 1 MiB   | 10,327 | 4,994 | 11,928 | 1,910 | **12,223** |
| 16 MiB  |  3,475 | 2,373 |  4,992 | 1,608 |  **5,693** |

Read the `unix` column here as a floor, not a measurement: the Rust client does
not yet widen its socket buffers, so it is the only client in this document
still paying macOS's 8 KiB default. The Python column is the one to trust for
Unix at ≥ 1 MiB.

Everything else peaks near 1 MiB and falls away at 16 MiB, on both harnesses.
That is the cache boundary rather than anything in the protocol: a megabyte of
Arrow still fits, sixteen does not, so the larger payload is bounded by main
memory no matter how it is carried.

## Two socket settings, and how the second one hid

**Unix sockets get 1 MiB buffers — on both ends.** macOS defaults a Unix
domain socket to 8,192 bytes of send and receive buffer
(`net.local.stream.sendspace`), against ~64 KiB for a pipe and 128 KiB for
TCP, so a megabyte of Arrow crossed the kernel in 128 trips instead of a
handful.

Setting it on the server's accepted socket alone bought +46% at 64 KiB, +79%
at 1 MiB and +41% at 16 MiB — a good result that made it easy to stop looking.
But an AF_UNIX write is bounded by space in the *receiver's* buffer, so a
tuned server still had to hand every response to an 8 KiB client. Widening the
client's socket too, in the Python reference's `UnixTransport`, moved the
unix/pipe throughput ratio again, and much further:

| unix ÷ pipe | server only | both ends |
|---|---:|---:|
| 64 KiB |  0.71 | 1.03 |
| 1 MiB  |  0.54 | **1.71** |
| 16 MiB |  0.72 | **1.53** |

That is the difference between a Unix socket costing 46% of the pipe's
throughput and beating it outright. Both figures are ratios against the pipe
column rather than absolute numbers, because the machine drifts further
between runs than the effect is worth: an early absolute A/B of the
server-side change moved the pipe control column — which was not supposed to
move at all — by 26%, and was measuring the laptop.

**TCP deliberately does not get the same treatment.** It already starts at
128 KiB and grows, an explicit `SO_RCVBUF` *disables* Linux's receive-window
auto-tuning and pins the window at whatever constant we guessed, and measuring
it on loopback showed no gain either way. A setting that helps one family is
not a setting that helps sockets.

**TCP does get `TCP_NODELAY`, and it costs about 14% here.** Nagle coalesces
small writes, which is precisely wrong for request/response: it holds a reply
waiting for more to send, and against a peer's delayed ACK that becomes a
tens-of-milliseconds stall. On loopback that stall never arrives — ACKs are
instant — while the coalescing still saves per-segment work, so the
measurement says the setting is a loss. It is kept because the stall it
prevents needs a real network, and a real network is what the transport is
for. A deployment using TCP only between co-located processes would be faster
without it.

## Where shared memory earns its keep

Against the plain pipe, above the 128 KiB threshold and not before: **+26% at
1 MiB and +50% at 16 MiB** through the Python client, +18% and +64% through
the Rust one, while adding a little overhead below the threshold.

Against a properly buffered Unix socket the case is now much weaker — in the
Python harness a 1 MiB Unix socket is *faster* than pipe+shm at 1 MiB (12,149
vs 8,979 MB/s) and ties it at 16 MiB. The Rust harness still shows shared
memory well ahead, but its Unix client is the untuned one, so that comparison
cannot settle the question yet. The shared-memory channel rides any of the
raw-framing transports, so the interesting configuration to measure next is
unix+shm rather than either alone.

It did not start out even this good. The first measurements had shared memory
roughly 40% *behind* the pipe at 1 MiB, from two independent causes:

- **The client never freed a resolved region.** Forty calls left forty live
  allocations — against the Python reference worker exactly as against this
  one, so it was the reference client's unary path, not this port. A stream
  hands its batch to the caller, who owns the release; a unary batch never
  escapes the client, so nobody else could free it. Two consequences: a long
  session walked toward the segment's 4094-allocation ceiling, and because no
  region was ever reused, every call faulted in cold pages. Fixed upstream in
  `_read_unary_response`.

- **This port copied the batch twice.** `allocate_and_write` serialized into a
  heap buffer and then copied that into the segment, because a region has to
  be reserved before the exact byte count is known. It now sizes the
  reservation from `arrow::ipc::GetRecordBatchSize` — which reads the buffer
  layout rather than serializing — plus a measured schema message, and lets
  Arrow write straight into the mapping. The sink is hard-bounded: an estimate
  that came up short would otherwise write past the reservation and corrupt
  whatever was allocated next, so it fails the write and the batch goes inline
  instead.
