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
| `void_noop()`      | 0.022 | 0.025 | 0.051 | 0.272 | 0.023 |
| `add_floats(a,b)`  | 0.034 | 0.040 | 0.064 | 0.298 | 0.036 |
| `echo_string(11B)` | 0.032 | 0.037 | 0.060 | 0.279 | 0.033 |
| `echo_all_types()` | 0.251 | 0.257 | 0.285 | 0.549 | 0.252 |

Median, to show the spread the machine added:

|                  | pipe | unix | tcp | http | pipe+shm |
|------------------|-----:|-----:|----:|-----:|---------:|
| `void_noop()`      | 0.025 | 0.030 | 0.076 | 0.304 | 0.025 |
| `add_floats(a,b)`  | 0.037 | 0.043 | 0.072 | 0.325 | 0.040 |
| `echo_string(11B)` | 0.035 | 0.040 | 0.070 | 0.321 | 0.036 |
| `echo_all_types()` | 0.260 | 0.264 | 0.304 | 0.587 | 0.261 |

### Unary rate

_calls/sec — higher is better_

|                  | pipe | unix | tcp | http | pipe+shm |
|------------------|-----:|-----:|----:|-----:|---------:|
| `void_noop()`      | 45,627 | 39,409 | 19,624 | 3,672 | 44,199 |
| `add_floats(a,b)`  | 29,484 | 24,845 | 15,717 | 3,351 | 27,875 |
| `echo_string(11B)` | 31,704 | 27,335 | 16,552 | 3,589 | 30,456 |
| `echo_all_types()` |  3,991 |  3,893 |  3,506 | 1,821 |  3,964 |

`void_noop()` is the per-call framing floor — no parameters, no result.
`echo_all_types()` carries eighteen fields covering every type mapping, so the
gap between the two is encode/decode cost rather than transport cost. That the
transports converge as the payload grows (46k → 4.0k on pipe, 3.7k → 1.8k on
HTTP) says most of the per-call spread is fixed framing overhead.

### Streaming

_best, ms — lower is better_

|                     | pipe | unix | tcp | http | pipe+shm |
|---------------------|-----:|-----:|----:|-----:|---------:|
| `produce_n(50)`       | 0.70 | 1.12 | 1.47 | 15.83 | 1.04 |
| `exchange_scale(20)`  | 0.35 | 0.57 | 0.82 |  6.67 | 0.39 |

HTTP pays worst here, and for a structural reason: the raw-framing transports
hold one connection open for the whole stream, while HTTP maps every producer
turn and every exchange onto a separate request/response pair.

### Payload throughput

_MB/s, both directions, at best time — higher is better_

|         | pipe | unix | tcp | http | pipe+shm |
|---------|-----:|-----:|----:|-----:|---------:|
| 1 KiB   |    58 |    50 |    28 |     6 |    56 |
| 64 KiB  | 2,373 | 2,506 | 1,508 |   344 | 2,222 |
| 1 MiB   | 6,773 | **12,069** | 7,423 | 1,522 | 9,086 |
| 16 MiB  | 3,573 | **5,467** | 5,160 | 1,727 | 5,243 |

The 1 KiB row is latency-bound, not bandwidth-bound — it is really just
`1 / round-trip` wearing different units. Shared memory does not engage below
`VGI_RPC_SHM_MIN_BATCH_BYTES` (128 KiB), which is why its 1 KiB and 64 KiB
figures track the plain pipe.

## Through a native client

A Python client can only measure a server down to its own cost per call.
Driving the same worker from Rust — reusing `vgi-rpc-client` from the sibling
[vgi-rpc-rust](https://github.com/Query-farm/vgi-rpc-rust) checkout, see
`bench/rust-client/` — moves the floor by 2x to 6x:

| `void_noop()`, calls/sec | Python client | Rust client | |
|---|---:|---:|---|
| pipe | 45,627 |  **91,954** | 2.0x |
| unix | 39,409 | **100,412** | 2.5x |
| tcp  | 19,624 |  **37,441** | 1.9x |
| http |  3,672 |  **21,409** | **5.8x** |

HTTP is where the difference is starkest: the Python table reads as though
this server costs 0.27 ms per HTTP call, and most of that is the client's HTTP
stack. The server's own figure is nearer 47 us.

| MB/s round trip | pipe | unix | tcp | http | pipe+shm |
|---|---:|---:|---:|---:|---:|
| 1 KiB   |   104 |    93 |    57 |    28 |   119 |
| 64 KiB  | 3,783 | 4,438 | 2,648 |   905 | 4,038 |
| 1 MiB   | 10,025 | **18,597** | 12,807 | 1,854 | 12,324 |
| 16 MiB  |  3,543 |  5,369 |  5,164 | 1,589 | **5,569** |

Both harnesses now agree on the ordering: at 1 MiB and 16 MiB a Unix socket is
the fastest transport here, ahead of the pipe and — at 1 MiB, by half again —
ahead of the shared-memory channel. That is a recent change, and the next
section is how it happened.

Everything still peaks near 1 MiB and falls away at 16 MiB, on both harnesses
and every transport. That is the cache boundary rather than anything in the
protocol: a megabyte of Arrow still fits, sixteen does not, so the larger
payload is bounded by main memory no matter how it is carried.

## The socket buffer, and how it hid twice

macOS defaults a Unix domain socket to 8,192 bytes of send and receive buffer
(`net.local.stream.sendspace`), against ~64 KiB for a pipe and 128 KiB for
TCP. A megabyte of Arrow therefore crossed the kernel in 128 trips instead of
a handful, and the Unix transport looked like the slowest raw-framing option
on the page.

The fix is one `setsockopt` pair. It had to be applied three times before it
was actually applied, because an `AF_UNIX` write is bounded by space in the
**receiver's** buffer — so any untuned end caps the whole connection:

1. **The C++ server's accepted socket.** Bought +46% at 64 KiB, +79% at 1 MiB,
   +41% at 16 MiB. A convincing result, and the reason to stop looking.
2. **The Python reference client.** With the server already tuned, this moved
   the unix/pipe throughput ratio from 0.54 to **1.71** at 1 MiB — a Unix
   socket went from costing half the pipe's throughput to beating it outright.
3. **The Rust client** (`vgi-rpc-client`'s `UnixTransport`), which was then the
   only untuned end left, and whose `unix` column had to be labelled a floor
   rather than a measurement until it was fixed:

   | Rust client, MB/s | before | after | |
   |---|---:|---:|---|
   | 64 KiB | 2,613 | 4,438 | 1.7x |
   | 1 MiB  | 4,994 | **18,597** | **3.7x** |
   | 16 MiB | 2,373 | 5,369 | 2.3x |

   The pipe control column moved 3% between those two runs, so that is the
   change and not the laptop.

Ratios against a control column rather than absolute numbers throughout,
because the machine drifts further between runs than most of these effects are
worth: an early absolute A/B of step 1 moved the pipe column — which the change
could not touch — by 26%, and was measuring the laptop.

**TCP deliberately does not get the same treatment.** It already starts at
128 KiB and grows, an explicit `SO_RCVBUF` *disables* Linux's receive-window
auto-tuning and pins the window at whatever constant we guessed, and measuring
it on loopback showed no gain either way. A setting that helps one socket
family is not a setting that helps sockets.

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

Less than it looked like, now that it is being compared against something
tuned.

Against the plain pipe the channel is a clear win above its 128 KiB threshold:
**+34% at 1 MiB and +47% at 16 MiB** through the Python client, +23% and +57%
through the Rust one.

Against a properly buffered Unix socket it is not. Both harnesses now put a
1 MiB Unix socket ahead of pipe+shm at 1 MiB — by 33% in Python, by 51% in
Rust — and score them within about 4% of each other at 16 MiB. Much of what looked
like a shared-memory win was the pipe's 64 KiB buffer and the socket's 8 KiB
one; a copy the kernel makes in one trip is cheaper than the machinery for
avoiding it.

That is not the end of the question, because the comparison is still
unbalanced: shared memory is a side channel that rides any of the raw-framing
transports, and every shm figure here rides the *pipe*. **unix+shm is the
configuration that would settle it, and nothing has measured it** — the Rust
client's `shm_connect` spawns a subprocess and has no way to attach a segment
to an existing Unix connection.

Its history is worth keeping, though, because two real bugs came out of it.
The first measurements had shared memory roughly 40% *behind* the pipe at
1 MiB, from two independent causes:

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
