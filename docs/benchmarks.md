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
| `void_noop()`      | 0.022 | 0.027 | 0.052 | 0.271 | 0.023 |
| `add_floats(a,b)`  | 0.034 | 0.040 | 0.062 | 0.298 | 0.035 |
| `echo_string(11B)` | 0.032 | 0.038 | 0.061 | 0.289 | 0.032 |
| `echo_all_types()` | 0.250 | 0.254 | 0.280 | 0.543 | 0.250 |

Median, to show the spread the machine added:

|                  | pipe | unix | tcp | http | pipe+shm |
|------------------|-----:|-----:|----:|-----:|---------:|
| `void_noop()`      | 0.026 | 0.031 | 0.074 | 0.300 | 0.025 |
| `add_floats(a,b)`  | 0.037 | 0.043 | 0.071 | 0.323 | 0.039 |
| `echo_string(11B)` | 0.035 | 0.041 | 0.070 | 0.322 | 0.036 |
| `echo_all_types()` | 0.260 | 0.262 | 0.300 | 0.583 | 0.259 |

### Unary rate

_calls/sec — higher is better_

|                  | pipe | unix | tcp | http | pipe+shm |
|------------------|-----:|-----:|----:|-----:|---------:|
| `void_noop()`      | 45,368 | 37,616 | 19,355 | 3,686 | 44,199 |
| `add_floats(a,b)`  | 29,412 | 25,026 | 16,075 | 3,361 | 28,202 |
| `echo_string(11B)` | 31,746 | 26,549 | 16,438 | 3,461 | 31,169 |
| `echo_all_types()` |  4,003 |  3,932 |  3,571 | 1,842 |  4,002 |

`void_noop()` is the per-call framing floor — no parameters, no result.
`echo_all_types()` carries eighteen fields covering every type mapping, so the
gap between the two is encode/decode cost rather than transport cost. That the
transports converge as the payload grows (45k → 4.0k on pipe, 3.7k → 1.8k on
HTTP) says most of the per-call spread is fixed framing overhead.

### Streaming

_best, ms — lower is better_

|                     | pipe | unix | tcp | http | pipe+shm |
|---------------------|-----:|-----:|----:|-----:|---------:|
| `produce_n(50)`       | 0.70 | 1.12 | 1.72 | 15.90 | 1.04 |
| `exchange_scale(20)`  | 0.35 | 0.57 | 0.82 |  6.58 | 0.57 |

HTTP pays worst here, and for a structural reason: the raw-framing transports
hold one connection open for the whole stream, while HTTP maps every producer
turn and every exchange onto a separate request/response pair.

### Payload throughput

_MB/s, both directions, at best time — higher is better_

|         | pipe | unix | tcp | http | pipe+shm |
|---------|-----:|-----:|----:|-----:|---------:|
| 1 KiB   |    60 |    50 |    30 |     6 |    56 |
| 64 KiB  | 2,351 | 1,780 | 1,539 |   336 | 2,247 |
| 1 MiB   | 6,856 | 3,758 | 7,783 | 1,559 | **8,830** |
| 16 MiB  | 3,625 | 2,486 | 5,394 | 1,797 | **5,216** |

The 1 KiB row is latency-bound, not bandwidth-bound — it is really just
`1 / round-trip` wearing different units. Shared memory does not engage below
`VGI_RPC_SHM_MIN_BATCH_BYTES` (128 KiB), which is why its 1 KiB and 64 KiB
figures track the plain pipe.

## Through a native client

A Python client can only measure a server down to its own cost per call.
Driving the same worker from Rust — reusing `vgi-rpc-client` from the sibling
[vgi-rpc-rust](https://github.com/Query-farm/vgi-rpc-rust) checkout, see
`bench/rust-client/` — moves the floor by 2x to 5x:

| `void_noop()`, calls/sec | Python client | Rust client | |
|---|---:|---:|---|
| pipe | 45,368 | **89,888** | 2.0x |
| unix | 37,616 | **92,661** | 2.5x |
| tcp  | 19,355 | **36,922** | 1.9x |
| http |  3,686 | **18,663** | **5.1x** |

HTTP is where the difference is starkest: the Python table reads as though
this server costs 0.27 ms per HTTP call, and most of that is the client's HTTP
stack. The server's own figure is nearer 54 us.

Throughput separates the same way — and the shared-memory column most of all,
because the channel exists to avoid copies, which is exactly what a client
spending tens of microseconds per call in Python hides:

| MB/s round trip | pipe | unix | tcp | http | pipe+shm |
|---|---:|---:|---:|---:|---:|
| 1 KiB   |   159 |   141 |    53 |    32 |   131 |
| 64 KiB  | 4,329 | 2,515 | 2,521 |   875 | 3,911 |
| 1 MiB   | 9,585 | 4,431 | 12,211 | 1,920 | **12,257** |
| 16 MiB  | 3,613 | 2,393 |  5,228 | 1,690 | **5,756** |

Everything peaks near 1 MiB and falls away at 16 MiB, on both harnesses and
every transport. That is the cache boundary rather than anything in the
protocol: a megabyte of Arrow still fits, sixteen does not, so the larger
payload is bounded by main memory no matter how it is carried.

## Two socket settings the sharper instrument found

**Unix sockets got 1 MiB buffers.** macOS gives a Unix domain socket 8,192
bytes of send and receive buffer by default — against ~64 KiB for a pipe and
128 KiB for TCP — so a megabyte of Arrow crossed the kernel in 128 trips
instead of a handful. Raising it moved the unix/pipe throughput ratio by +46%
at 64 KiB, +79% at 1 MiB and +41% at 16 MiB. Measured as a ratio against the
pipe column, because the machine drifts further between runs than the change
is worth: an early A/B of this looked convincing until the pipe column, which
was not supposed to move at all, moved 26%.

**TCP got `TCP_NODELAY`, and it costs about 14% here.** Nagle coalesces small
writes, which is precisely wrong for request/response: it holds a reply
waiting for more to send, and against a peer's delayed ACK that becomes a
tens-of-milliseconds stall. On loopback that stall never arrives — ACKs are
instant — while the coalescing still saves per-segment work, so the
measurement says the setting is a loss. It is kept because the stall it
prevents needs a real network, and a real network is what the transport is
for. A deployment using TCP only between co-located processes would be faster
without it.

## Where shared memory earns its keep

Above the 128 KiB threshold, and not before: **+29% at 1 MiB and +44% at
16 MiB** over the plain pipe through the Python client, +28% and +59% through
the Rust one, while adding a little overhead below the threshold. On a single
machine with a fast pipe that is the whole of the win — the channel is for
large batches between co-located processes, not for making small calls faster.

It did not start out that way. The first measurements had shared memory
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
