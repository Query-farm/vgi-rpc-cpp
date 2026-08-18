---
description: "vgi-rpc C++ transport benchmarks — unary round-trip latency, call rate, streaming, and payload throughput across pipe, Unix socket, TCP, HTTP, and shared memory."
---

# Benchmarks

Run them yourself:

```bash
python scripts/benchmark_transports.py --rounds 5 --iterations 300
```

## What is being measured

A full round trip through the **Python reference client** — encode, transport,
C++ dispatch, decode. That is the number an application sees, and it is what
the Python reference's own `docs/benchmarks.md` reports, so the two tables can
be read side by side. It is **not** the C++ server's ceiling: a large share of
each figure is the Python client.

Rounds are **interleaved** across transports rather than run back to back. A
benchmark that measures each transport once, in sequence, compares whatever
the machine happened to be doing at the time as much as it compares
transports.

Headline figures are the **best** sample, not the median. Interference on a
shared machine is one-sided — it can only make a sample slower — so the
minimum is the closest available estimate of the real cost. The median table
is kept alongside it to show how much the machine added.

Measured on macOS 15.6 / Apple Silicon, 8 CPUs, under a load average near 10 —
a working laptop, not a quiet bench. Absolute numbers will move with the
machine; the **ratios between columns** are the point, and those held across
every run.

## Unary round trip

_best, ms — lower is better_

|                  | pipe | unix | tcp | http | pipe+shm |
|------------------|-----:|-----:|----:|-----:|---------:|
| `void_noop()`      | 0.045 | 0.051 | 0.090 | 0.446 | 0.057 |
| `add_floats(a,b)`  | 0.077 | 0.088 | 0.135 | 0.518 | 0.089 |
| `echo_string(11B)` | 0.071 | 0.084 | 0.111 | 0.512 | 0.080 |
| `echo_all_types()` | 0.314 | 0.345 | 0.376 | 0.900 | 0.332 |

## Unary rate

_calls/sec — higher is better_

|                  | pipe | unix | tcp | http | pipe+shm |
|------------------|-----:|-----:|----:|-----:|---------:|
| `void_noop()`      | 22,284 | 19,592 | 11,147 | 2,243 | 17,442 |
| `add_floats(a,b)`  | 13,058 | 11,326 |  7,405 | 1,931 | 11,236 |
| `echo_string(11B)` | 14,085 | 11,928 |  9,019 | 1,954 | 12,572 |
| `echo_all_types()` |  3,186 |  2,902 |  2,659 | 1,111 |  3,016 |

`void_noop()` is the per-call framing floor — no parameters, no result.
`echo_all_types()` carries eighteen fields covering every type mapping, so the
gap between the two is encode/decode cost rather than transport cost. That the
transports converge as the payload grows (22k → 3.2k on pipe, 2.2k → 1.1k on
HTTP) says most of the per-call spread is fixed framing overhead.

## Streaming

_best, ms — lower is better_

|                     | pipe | unix | tcp | http | pipe+shm |
|---------------------|-----:|-----:|----:|-----:|---------:|
| `produce_n(50)`       | 2.46 | 2.66 | 3.62 | 29.29 | 2.36 |
| `exchange_scale(20)`  | 0.88 | 1.13 | 1.56 | 11.90 | 1.10 |

HTTP pays worst here, and for a structural reason: the raw-framing transports
hold one connection open for the whole stream, while HTTP maps every producer
turn and every exchange onto a separate request/response pair.

## Payload throughput

_MB/s, both directions, at best time — higher is better_

|         | pipe | unix | tcp | http | pipe+shm |
|---------|-----:|-----:|----:|-----:|---------:|
| 1 KiB   |    24 |    23 |    16 |   3 |    22 |
| 64 KiB  | 1,367 |   782 |   882 | 158 | 1,126 |
| 1 MiB   | 4,658 | 1,978 | 3,463 | 752 | **5,718** |
| 16 MiB  | 2,817 | 1,419 | 3,477 | 855 | **3,509** |

The 1 KiB row is latency-bound, not bandwidth-bound — it is really just
`1 / round-trip` wearing different units. Shared memory does not engage below
`VGI_RPC_SHM_MIN_BATCH_BYTES` (128 KiB), which is why its 1 KiB and 64 KiB
figures track the plain pipe.

## Where shared memory earns its keep

Above the 128 KiB threshold, and not before: **+23% at 1 MiB and +25% at
16 MiB** over the plain pipe, while adding a little overhead below it. On a
single machine with a fast pipe that is the whole of the win — the channel is
for large batches between co-located processes, not for making small calls
faster.

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
