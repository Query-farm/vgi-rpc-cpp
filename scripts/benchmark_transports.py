# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Measure end-to-end RPC rate against the C++ worker, per transport.

Methodology mirrors the Python reference's ``docs/benchmarks.md`` so the two
tables can be read side by side: a warmup, then N timed iterations of one
call, reported as the **median** round trip. Median rather than mean because a
single scheduler hiccup should not move the number the reader compares.

What this measures is a full round trip through the Python client — encode,
transport, C++ dispatch, decode. That is the number an application actually
sees, and it is what the reference measures too, but it is *not* the C++
server's ceiling: a meaningful share of each figure is the Python client.
Treat the columns as comparable to each other, not as a server benchmark.

Usage:
    python scripts/benchmark_transports.py [--iterations N] [--json out.json]
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

#: Segment for the currently open shared-memory connection, so a workload can
#: clear it between measurements.  The reference client never frees the region
#: a resolved response came from — verified against the Python worker too — so
#: without a reset the segment fills, allocation starts failing, and the
#: transport silently falls back to the pipe while still being labelled "shm".
_ACTIVE_SEGMENT: Any = None

_REPO_ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_WORKER = _REPO_ROOT / "build" / "conformance" / "conformance_worker"


def worker_path() -> str:
    override = os.environ.get("CONFORMANCE_WORKER")
    path = Path(override) if override else _DEFAULT_WORKER
    if not path.is_file():
        sys.exit(f"worker not found: {path} (build it, or set CONFORMANCE_WORKER)")
    return str(path)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


# ---------------------------------------------------------------------------
# Transports
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def _listener(flag: str, value: str, prefix: str) -> Iterator[str]:
    """Spawn the worker on a socket transport, yielding its discovery payload."""
    proc = subprocess.Popen(
        [worker_path(), flag, value], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        if not line.startswith(prefix):
            raise RuntimeError(f"expected {prefix}<...>, got {line!r}")
        yield line[len(prefix):]
    finally:
        proc.terminate()
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=5)
        if proc.poll() is None:
            proc.kill()


@contextlib.contextmanager
def connect_subprocess(shm_bytes: int | None = None) -> Iterator[Any]:
    """Pipe transport, optionally with a shared-memory side channel attached."""
    from vgi_rpc.conformance import ConformanceService
    from vgi_rpc.rpc import RpcConnection, StderrMode, SubprocessTransport

    global _ACTIVE_SEGMENT
    transport = SubprocessTransport([worker_path()], stderr=StderrMode.DEVNULL)
    segment = None
    try:
        if shm_bytes is not None:
            from vgi_rpc.shm import ShmSegment

            segment = ShmSegment.create(shm_bytes)
            _ACTIVE_SEGMENT = segment

            class _WithShm:
                """Expose ``.shm`` so the client routes large batches through it."""

                def __init__(self, inner: Any, shm: Any) -> None:
                    self._inner, self.shm = inner, shm

                def __getattr__(self, name: str) -> Any:
                    return getattr(self._inner, name)

            effective: Any = _WithShm(transport, segment)
        else:
            effective = transport
        with RpcConnection(ConformanceService, effective) as proxy:
            yield proxy
    finally:
        _ACTIVE_SEGMENT = None
        transport.close()
        if segment is not None:
            with contextlib.suppress(Exception):
                segment.unlink()
                segment.close()


@contextlib.contextmanager
def connect_unix() -> Iterator[Any]:
    from vgi_rpc.conformance import ConformanceService
    from vgi_rpc.rpc import unix_connect

    # Short path: sockaddr_un.sun_path is 104 bytes on macOS.
    tmpdir = tempfile.mkdtemp(prefix="vgib", dir=tempfile.gettempdir())
    sock = os.path.join(tmpdir, "b.sock")
    try:
        with _listener("--unix", sock, "UNIX:") as path:
            with unix_connect(ConformanceService, path) as proxy:
                yield proxy
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


@contextlib.contextmanager
def connect_tcp() -> Iterator[Any]:
    from vgi_rpc.conformance import ConformanceService
    from vgi_rpc.rpc import tcp_connect

    with _listener("--tcp", "127.0.0.1:0", "TCP:") as addr:
        host, _, port = addr.rpartition(":")
        with tcp_connect(ConformanceService, host, int(port)) as proxy:
            yield proxy


@contextlib.contextmanager
def connect_http() -> Iterator[Any]:
    from vgi_rpc.conformance import ConformanceService
    from vgi_rpc.http import http_connect

    port = _free_port()
    proc = subprocess.Popen(
        [worker_path(), "--http", "--port", str(port)],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    try:
        assert proc.stdout is not None
        proc.stdout.readline()
        deadline = time.monotonic() + 30
        import httpx2

        while time.monotonic() < deadline:
            try:
                httpx2.get(f"http://127.0.0.1:{port}/health", timeout=1.0)
                break
            except httpx2.TransportError:
                time.sleep(0.05)
        with http_connect(ConformanceService, f"http://127.0.0.1:{port}") as proxy:
            yield proxy
    finally:
        proc.terminate()
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=5)
        if proc.poll() is None:
            proc.kill()


TRANSPORTS: dict[str, Callable[[], Any]] = {
    "pipe": connect_subprocess,
    "unix": connect_unix,
    "tcp": connect_tcp,
    "http": connect_http,
    # Large enough that a whole payload sweep fits without the allocator
    # filling — see _ACTIVE_SEGMENT on why nothing frees along the way.
    "pipe+shm": lambda: connect_subprocess(shm_bytes=768 * 1024 * 1024),
}


# ---------------------------------------------------------------------------
# Workloads
# ---------------------------------------------------------------------------


def _all_types_value() -> Any:
    from vgi_rpc.conformance import AllTypes, Point, Status

    return AllTypes(
        str_field="benchmark",
        bytes_field=b"benchmark",
        int_field=7,
        float_field=1.5,
        bool_field=True,
        list_of_int=[1, 2, 3],
        list_of_str=["a", "b"],
        dict_field={"k": 1},
        enum_field=Status.ACTIVE,
        nested_point=Point(x=1.0, y=2.0),
        optional_str=None,
        optional_int=5,
        optional_nested=Point(x=3.0, y=4.0),
        list_of_nested=[Point(x=5.0, y=6.0)],
        annotated_int32=11,
        annotated_float32=2.5,
        nested_list=[[1, 2], [3]],
        dict_str_str={"a": "b"},
    )


def unary_workloads() -> dict[str, Callable[[Any], Any]]:
    """One call each, chosen to isolate framing cost from payload cost."""
    payload = _all_types_value()
    return {
        # Nothing in, nothing out: the pure per-call framing floor.
        "void_noop()": lambda p: p.void_noop(),
        "add_floats(a,b)": lambda p: p.add_floats(a=1.0, b=2.0),
        "echo_string(11B)": lambda p: p.echo_string(value="hello world"),
        # Eighteen fields covering every type mapping: the encode/decode cost.
        "echo_all_types()": lambda p: p.echo_all_types(data=payload),
    }


def stream_workloads() -> dict[str, Callable[[Any], Any]]:
    from vgi_rpc.rpc import AnnotatedBatch

    def producer_50(p: Any) -> None:
        for _ in p.produce_n(count=50):
            pass

    def exchange_20(p: Any) -> None:
        with p.exchange_scale(factor=2.0) as session:
            for i in range(20):
                session.exchange(AnnotatedBatch.from_pydict({"value": [float(i)]}))

    return {"produce_n(50)": producer_50, "exchange_scale(20)": exchange_20}


def payload_sizes() -> list[int]:
    """Sizes bracketing the 128 KiB shared-memory routing threshold."""
    return [1024, 64 * 1024, 1024 * 1024, 16 * 1024 * 1024]


# ---------------------------------------------------------------------------
# Timing
# ---------------------------------------------------------------------------


def measure(fn: Callable[[], Any], iterations: int, warmup: int) -> list[float]:
    """Time `fn` and return the raw samples, in seconds."""
    for _ in range(warmup):
        fn()
    samples: list[float] = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        fn()
        samples.append(time.perf_counter() - t0)
    return samples


def _bench_one(proxy: Any, args: argparse.Namespace, skip_payload: bool) -> dict[str, Any]:
    """Run every workload once against an open connection."""
    out: dict[str, Any] = {"unary": {}, "stream": {}, "payload": {}}
    for label, call in unary_workloads().items():
        out["unary"][label] = measure(lambda: call(proxy), args.iterations, args.warmup)
    for label, call in stream_workloads().items():
        out["stream"][label] = measure(lambda: call(proxy), max(20, args.iterations // 10), 5)
    if not skip_payload:
        for size in payload_sizes():
            blob = b"\xa5" * size
            if _ACTIVE_SEGMENT is not None:
                # Start each size from an empty segment, so a size is measured
                # on the shared-memory path rather than on whatever the
                # allocator had room left for.
                _ACTIVE_SEGMENT.reset()
            out["payload"][size] = measure(
                lambda: proxy.echo_large_binary(value=blob), args.payload_iterations, 3
            )
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--iterations", type=int, default=200)
    parser.add_argument("--warmup", type=int, default=25)
    parser.add_argument("--payload-iterations", type=int, default=15)
    # Rounds are interleaved across transports rather than run back to back.
    # A benchmark that measures each transport once, in sequence, compares
    # whatever the machine was doing at the time as much as the transports.
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--transports", default=",".join(TRANSPORTS))
    parser.add_argument("--json", type=Path, default=None)
    parser.add_argument("--skip-payload", action="store_true")
    args = parser.parse_args()

    # Record the machine, because a latency table without it invites the
    # reader to compare against numbers taken somewhere else entirely.
    import platform
    load = ", ".join(f"{x:.2f}" for x in os.getloadavg()) if hasattr(os, "getloadavg") else "n/a"
    print(f"host: {platform.platform()}  cpus={os.cpu_count()}  load={load}", file=sys.stderr)
    print(f"worker: {worker_path()}", file=sys.stderr)

    selected = [t.strip() for t in args.transports.split(",") if t.strip()]
    unknown = [t for t in selected if t not in TRANSPORTS]
    if unknown:
        sys.exit(f"unknown transport(s): {', '.join(unknown)}")

    # section -> label -> transport -> pooled samples
    pooled: dict[str, dict[Any, dict[str, list[float]]]] = {
        "unary": {}, "stream": {}, "payload": {}
    }
    failed: dict[str, str] = {}

    for rnd in range(args.rounds):
        for name in selected:
            if name in failed:
                continue
            print(f"round {rnd + 1}/{args.rounds}  [{name}]", file=sys.stderr, flush=True)
            try:
                with TRANSPORTS[name]() as proxy:
                    got = _bench_one(proxy, args, args.skip_payload)
            except Exception as exc:  # noqa: BLE001
                failed[name] = f"{type(exc).__name__}: {exc}"
                print(f"  [{name}] FAILED: {failed[name]}", file=sys.stderr)
                continue
            for section, rows in got.items():
                for label, samples in rows.items():
                    pooled[section].setdefault(label, {}).setdefault(name, []).extend(samples)

    live = [t for t in selected if t not in failed]
    _render(pooled, live)
    for name, why in failed.items():
        print(f"\n{name}: FAILED — {why}", file=sys.stderr)
    if args.json:
        summary = {
            section: {
                str(label): {
                    t: {"best": min(v), "median": statistics.median(v), "n": len(v)}
                    for t, v in per.items()
                }
                for label, per in rows.items()
            }
            for section, rows in pooled.items()
        }
        args.json.write_text(json.dumps(summary, indent=2))
        print(f"\nwrote {args.json}", file=sys.stderr)
    return 0


def _render(pooled: dict[str, Any], live: list[str]) -> None:
    if not live:
        print("no transport produced results")
        return

    def table(title: str, unit: str, rows: dict[Any, Any],
              cell: Callable[[list[float]], str], label: Callable[[Any], str]) -> None:
        if not rows:
            return
        print(f"\n## {title}\n_{unit}_\n")
        width = max(len(label(k)) for k in rows)
        print(f"| {'':<{width}} | " + " | ".join(f"{t:>10}" for t in live) + " |")
        print(f"| {'-' * width} | " + " | ".join("-" * 10 for _ in live) + " |")
        for key, per in rows.items():
            cells = [cell(per[t]) if t in per else "—".rjust(10) for t in live]
            print(f"| {label(key):<{width}} | " + " | ".join(cells) + " |")

    # Best, not median: interference on a shared machine only ever makes a
    # sample slower, so the minimum is the closest thing to the real cost.
    table("Unary round trip", "best of all rounds, ms — lower is better",
          pooled["unary"], lambda v: f"{min(v) * 1000:>10.3f}", str)
    table("Unary rate", "calls/sec at best round trip — higher is better",
          pooled["unary"], lambda v: f"{1 / min(v):>10,.0f}", str)
    table("Unary round trip (median)", "ms — shows the spread the machine added",
          pooled["unary"], lambda v: f"{statistics.median(v) * 1000:>10.3f}", str)
    table("Streaming", "best, ms — lower is better",
          pooled["stream"], lambda v: f"{min(v) * 1000:>10.2f}", str)
    if pooled["payload"]:
        rows = pooled["payload"]
        print("\n## Echo throughput\n_MB/s round trip at best time — higher is better_\n")
        def size_label(n: int) -> str:
            return f"{n // 1024} KiB" if n < 1024 * 1024 else f"{n // (1024 * 1024)} MiB"
        width = max(len(size_label(k)) for k in rows)
        print(f"| {'':<{width}} | " + " | ".join(f"{t:>10}" for t in live) + " |")
        print(f"| {'-' * width} | " + " | ".join("-" * 10 for _ in live) + " |")
        for size, per in rows.items():
            cells = []
            for t in live:
                if t in per:
                    # Both directions cross the wire, hence 2x.
                    cells.append(f"{(2 * size / (1024 * 1024)) / min(per[t]):>10,.0f}")
                else:
                    cells.append("—".rjust(10))
            print(f"| {size_label(size):<{width}} | " + " | ".join(cells) + " |")


if __name__ == "__main__":
    raise SystemExit(main())
