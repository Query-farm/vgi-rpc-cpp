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

Measured on macOS 15.6 / Apple Silicon, 8 CPUs, load ~3.5. Absolute numbers
will move with the machine; the **ratios between columns** are the point.

## Unary round trip

_best, ms — lower is better_

|                  | pipe | unix | tcp | http | pipe+shm |
|------------------|-----:|-----:|----:|-----:|---------:|
| `void_noop()`      | 0.039 | 0.050 | 0.067 | 0.385 | 0.051 |
| `add_floats(a,b)`  | 0.066 | 0.079 | 0.098 | 0.435 | 0.080 |
| `echo_string(11B)` | 0.063 | 0.072 | 0.093 | 0.429 | 0.073 |
| `echo_all_types()` | 0.277 | 0.293 | 0.319 | 0.769 | 0.293 |

## Unary rate

_calls/sec — higher is better_

|                  | pipe | unix | tcp | http | pipe+shm |
|------------------|-----:|-----:|----:|-----:|---------:|
| `void_noop()`      | 25,370 | 19,967 | 14,944 | 2,596 | 19,449 |
| `add_floats(a,b)`  | 15,190 | 12,712 | 10,243 | 2,298 | 12,579 |
| `echo_string(11B)` | 15,957 | 13,817 | 10,758 | 2,331 | 13,675 |
| `echo_all_types()` |  3,608 |  3,408 |  3,131 | 1,300 |  3,411 |

`void_noop()` is the per-call framing floor — no parameters, no result.
`echo_all_types()` carries eighteen fields covering every type mapping, so the
gap between the two is encode/decode cost rather than transport cost. That the
four transports converge as the payload grows (25k → 3.6k on pipe, 2.6k → 1.3k
on HTTP) says most of the per-call difference is fixed framing overhead.

## Streaming

_best, ms — lower is better_

|                     | pipe | unix | tcp | http | pipe+shm |
|---------------------|-----:|-----:|----:|-----:|---------:|
| `produce_n(50)`       | 2.32 | 2.04 | 3.43 | 24.08 | 2.34 |
| `exchange_scale(20)`  | 0.77 | 1.03 | 1.45 |  9.83 | 0.82 |

HTTP pays worst here, and for a structural reason: the raw-framing transports
hold one connection open for the whole stream, while HTTP maps every producer
turn and every exchange onto a separate request/response pair.

## Payload throughput

_MB/s, both directions, at best time — higher is better_

|         | pipe | unix | tcp | http | pipe+shm |
|---------|-----:|-----:|----:|-----:|---------:|
| 1 KiB   |    28 |    25 |    19 |     4 |    27 |
| 64 KiB  | 1,525 |   873 | 1,082 |   218 | 1,284 |
| 1 MiB   | 7,004 | 2,356 | 6,494 |   961 | 4,077 |
| 16 MiB  | 3,604 | 1,558 | 5,399 | 1,132 | 3,243 |

The 1 KiB row is latency-bound, not bandwidth-bound — it is really just
`1 / round-trip` wearing different units.

## Shared memory is not currently a win

The `pipe+shm` column is at best level with the plain pipe and is roughly
**40% slower at 1 MiB**. Two causes, and they are worth separating.

**The reference client never frees a resolved region.** Confirmed by counting
live allocations: 40 calls leave 40 allocations, against the Python reference
worker exactly as against this one, so it is the client's unary path and not
this port. Two consequences: a long-running session walks toward the 4094-slot
header limit, and because no region is ever reused, every call faults in cold
pages. Forcing reuse recovers a good part of the gap — at 16 MiB, 3,018 →
4,024 MB/s, which turns a loss against the pipe into a win.

**This port copies once more than it needs to.** `allocate_and_write`
serializes the batch into an Arrow buffer and then copies that into the
segment, because the allocation has to be sized before the bytes exist. The
reference instead writes the IPC stream *directly* into the mapped region
through a custom sink, pre-allocating from an estimate. Removing that copy is
the obvious next step, and until it lands the shared-memory numbers above
should be read as a floor rather than as what the channel can do.

The honest summary today: on a single machine with a fast pipe, the
shared-memory side channel earns its keep only for large batches, and only
once the region is reused.
