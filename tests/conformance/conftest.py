# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Fixtures that drive the shared vgi-rpc pytest conformance suite.

The suite in ``vgi_rpc.conformance._pytest_suite`` is language-agnostic: it
asks a runner for named fixtures and drives whatever worker they hand back.
This module is the C++ port's side of that contract — every fixture spawns
``conformance_worker`` with the flags that put it into the configuration the
matching test group needs.

Point ``VGI_RPC_CPP_WORKER`` at the built binary, or let it default to
``build/conformance/conformance_worker`` relative to the repo root.
"""

from __future__ import annotations

import contextlib
import os
import socket
import subprocess
import sys
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

#: Unix sockets and the raw TCP transport are POSIX-only in this port — the
#: worker refuses them on Windows rather than pretending — so the matrix drops
#: those legs there instead of failing every test in them.
_SKIP_POSIX_ONLY = pytest.mark.skipif(
    sys.platform == "win32", reason="unix and tcp transports are POSIX-only"
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_WORKER = _REPO_ROOT / "build" / "conformance" / "conformance_worker"

#: Origin the CORS worker is configured to allow.  Must match the constant the
#: shared suite sends as its ``Origin`` header.
CONFORMANCE_CORS_ORIGIN = "https://conformance.example"


def worker_path() -> str:
    """Absolute path to the C++ conformance worker under test."""
    override = os.environ.get("VGI_RPC_CPP_WORKER")
    path = Path(override) if override else _DEFAULT_WORKER
    if not path.is_file() or not os.access(path, os.X_OK):
        pytest.skip(f"conformance worker not built: {path}")
    return str(path)


def _free_port() -> int:
    """Return a free TCP port on loopback."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _wait_for_http(port: int, timeout: float = 30.0) -> None:
    """Poll until the HTTP server answers.

    Retries on any transport error, not just connect refusal: a server that
    has bound but not yet begun serving accepts the connection and drops it,
    which surfaces as a protocol error rather than a connect error.
    """
    import httpx2

    deadline = time.monotonic() + timeout
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            httpx2.get(f"http://127.0.0.1:{port}/health", timeout=1.0)
            return
        except httpx2.TransportError as exc:  # noqa: PERF203
            last = exc
            time.sleep(0.05)
    raise RuntimeError(f"HTTP server on port {port} never became ready: {last}")


@contextlib.contextmanager
def spawn_http(*extra_args: str) -> Iterator[int]:
    """Spawn the C++ worker in HTTP mode with *extra_args*, yielding its port."""
    proc = subprocess.Popen(
        [worker_path(), "--http", *extra_args],
        stdout=subprocess.PIPE,
        stderr=sys.stderr,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        if not line.startswith("PORT:"):
            raise RuntimeError(f"expected PORT:<n> on stdout, got {line!r}")
        port = int(line.split(":", 1)[1])
        _wait_for_http(port)
        yield port
    finally:
        proc.terminate()
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=5)
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)


# ---------------------------------------------------------------------------
# Core fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def conformance_http_port() -> Iterator[int]:
    """A plain HTTP worker: no auth, no CORS, no caps, no storage."""
    with spawn_http() as port:
        yield port


@contextlib.contextmanager
def _spawn_listener(flag: str, value: str, prefix: str) -> Iterator[str]:
    """Spawn the worker on a socket transport, yielding its discovery line's payload."""
    proc = subprocess.Popen(
        [worker_path(), flag, value], stdout=subprocess.PIPE, stderr=sys.stderr
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        if not line.startswith(prefix):
            raise RuntimeError(f"expected {prefix}<...> on stdout, got {line!r}")
        yield line[len(prefix):]
    finally:
        proc.terminate()
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=5)
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)


@pytest.fixture(scope="session")
def conformance_unix_path() -> Iterator[str]:
    """A worker listening on a Unix domain socket, yielding the socket path.

    Deliberately not pytest's ``tmp_path``: ``sockaddr_un.sun_path`` is 104
    bytes on macOS and pytest's per-run temp directories overrun it.
    """
    import shutil
    import tempfile

    tmpdir = tempfile.mkdtemp(prefix="vgi", dir=tempfile.gettempdir())
    sock = os.path.join(tmpdir, "c.sock")
    try:
        with _spawn_listener("--unix", sock, "UNIX:") as path:
            yield path
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture(scope="session")
def conformance_tcp_addr() -> Iterator[tuple[str, int]]:
    """A worker listening on loopback TCP, yielding ``(host, port)``."""
    with _spawn_listener("--tcp", "127.0.0.1:0", "TCP:") as addr:
        host, _, port = addr.rpartition(":")
        yield host, int(port)


@pytest.fixture(
    params=[
        "subprocess",
        "http",
        pytest.param("unix", marks=_SKIP_POSIX_ONLY),
        pytest.param("tcp", marks=_SKIP_POSIX_ONLY),
    ]
)
def conformance_conn(request: pytest.FixtureRequest, conformance_http_port: int) -> Any:
    """Connection factory over each transport the C++ worker implements.

    Overrides the reference runner's fixture of the same name, which
    parametrises over in-process Python servers that say nothing about this
    port.
    """
    from vgi_rpc.conformance import ConformanceService
    from vgi_rpc.http import http_connect
    from vgi_rpc.log import Message
    from vgi_rpc.rpc import connect, tcp_connect, unix_connect

    def factory(on_log: Callable[[Message], None] | None = None) -> Any:
        if request.param == "subprocess":
            return connect(ConformanceService, [worker_path()], on_log=on_log)
        if request.param == "unix":
            path: str = request.getfixturevalue("conformance_unix_path")
            return unix_connect(ConformanceService, path, on_log=on_log)
        if request.param == "tcp":
            host, port = request.getfixturevalue("conformance_tcp_addr")
            return tcp_connect(ConformanceService, host, port, on_log=on_log)
        return http_connect(
            ConformanceService, f"http://127.0.0.1:{conformance_http_port}", on_log=on_log
        )

    return factory


@pytest.fixture(params=["subprocess", "http"])
def conformance_describe(request: pytest.FixtureRequest, conformance_http_port: int) -> Any:
    """``ServiceDescription`` obtained by calling ``__describe__`` over the wire.

    Against the real worker rather than an in-process stand-in, which is the
    point: introspection has to be right on the transport it ships on.
    """
    from vgi_rpc.http import http_introspect
    from vgi_rpc.introspect import introspect
    from vgi_rpc.rpc import SubprocessTransport

    if request.param == "http":
        return http_introspect(f"http://127.0.0.1:{conformance_http_port}")
    transport = SubprocessTransport([worker_path()])
    try:
        return introspect(transport)
    finally:
        transport.close()


# ---------------------------------------------------------------------------
# Capability-gated workers
#
# Each of these is a *server configuration* no request can induce, so the state
# under test needs its own process.  The flags mirror the reference worker's.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def conformance_http_auth_port() -> Iterator[int]:
    """A worker whose RPC endpoints all 401, while ``GET /health`` still answers.

    Also honours ``X-Conformance-Auth-Reason``, which is what makes
    ``TestUnauthorized``'s discrimination tests meaningful rather than
    vacuously green against a server that stamps one constant reason.
    """
    with spawn_http("--auth-reject-all") as port:
        yield port


@pytest.fixture(scope="session")
def conformance_http_auth_reason_port(conformance_http_auth_port: int) -> int:
    """The reject-all worker already reads the header, so this is an alias."""
    return conformance_http_auth_port


@pytest.fixture(scope="session")
def conformance_http_introspect_port() -> Iterator[int]:
    """A worker with the token-introspection route enabled.

    Needs its own process because the route is absent unless explicitly
    enabled — which ``TestTokenIntrospectionOffMode`` asserts against the
    default worker.
    """
    with spawn_http("--introspect") as port:
        yield port


@pytest.fixture(scope="session")
def conformance_http_no_compression_port() -> Iterator[int]:
    """A worker that positively states it produces no codecs.

    Only a server booted this way emits the present-but-empty
    ``VGI-Supported-Encodings`` that distinguishes "speaks no compression"
    from the absent header of a server predating the advertisement.
    """
    with spawn_http("--no-compression") as port:
        yield port


@pytest.fixture(scope="session")
def conformance_http_access_log(
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[tuple[int, Path]]:
    """A worker writing an access log, for the ``X-Request-ID`` correlation case.

    Asserting the header matches the record's ``request_id`` means reading
    back what the server logged, which no amount of poking at the wire can
    substitute for.
    """
    log_path = tmp_path_factory.mktemp("accesslog") / "conformance.log"
    with spawn_http("--access-log", str(log_path), "--access-log-debug") as port:
        yield port, log_path


# ---------------------------------------------------------------------------
# Sticky failure-path workers
#
# A port advertising VGI-Sticky-Enabled must supply all three: withholding
# them drops exactly the session-loss coverage the group exists to provide.
# ---------------------------------------------------------------------------

#: One AEAD key shared by the peer pair.  Sharing it is the point — with
#: per-process keys the peer rejects the token at decryption and never reaches
#: the server_id comparison the test is about.
_STICKY_PEER_TOKEN_KEY = "5f" * 32


@pytest.fixture(scope="session")
def conformance_http_sticky_short_ttl_port() -> Iterator[int]:
    """A sticky worker with a ~1s TTL, short enough for a test to outwait."""
    with spawn_http("--sticky-ttl", "1") as port:
        yield port


@pytest.fixture(scope="session")
def conformance_http_sticky_peer_ports() -> Iterator[tuple[int, int]]:
    """Two sticky workers sharing one token key but reporting distinct server ids."""
    with (
        spawn_http("--token-key", _STICKY_PEER_TOKEN_KEY) as port_a,
        spawn_http("--token-key", _STICKY_PEER_TOKEN_KEY) as port_b,
    ):
        yield port_a, port_b


# ---------------------------------------------------------------------------
# External-location workers
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def conformance_fake_storage() -> Iterator[str]:
    """The in-process fake object store, yielding its base URL.

    Real deployments externalize to S3 or GCS; this speaks the same four-endpoint
    contract without making conformance depend on a cloud account.
    """
    from vgi_rpc.conformance.fake_storage import serve_in_thread

    base_url, shutdown = serve_in_thread()
    try:
        yield base_url
    finally:
        shutdown()


@pytest.fixture(scope="session")
def conformance_http_with_storage_port(conformance_fake_storage: str) -> Iterator[int]:
    """A worker wired against the fake storage, with a small threshold.

    4 KiB so a test can trip externalization deliberately without producing
    megabytes of payload.
    """
    with spawn_http(
        "--fake-storage", conformance_fake_storage, "--externalize-threshold", "4096"
    ) as port:
        yield port


@pytest.fixture(scope="session")
def conformance_http_with_zstd_storage_port(conformance_fake_storage: str) -> Iterator[int]:
    """Same, with zstd applied to externalized payloads."""
    with spawn_http(
        "--fake-storage",
        conformance_fake_storage,
        "--externalize-threshold",
        "4096",
        "--externalize-compression",
        "zstd",
    ) as port:
        yield port


@pytest.fixture(scope="session")
def conformance_http_strict_cap_port() -> Iterator[int]:
    """A worker with caps tight enough for the strict-fail tests to overshoot."""
    with spawn_http(
        "--max-response-bytes", str(64 * 1024),
        "--max-externalized-response-bytes", str(64 * 1024),
    ) as port:
        yield port


@pytest.fixture(scope="session")
def conformance_http_externalized_cap_port(conformance_fake_storage: str) -> Iterator[int]:
    """A worker whose *external-channel* cap is the one that bites.

    The wire cap is deliberately generous: an externalized payload leaves only
    a pointer batch on the wire, so if the body cap were tight too the test
    would pass while proving nothing about the external cap.
    """
    with spawn_http(
        "--fake-storage", conformance_fake_storage,
        "--max-externalized-response-bytes", str(64 * 1024),
        "--max-response-bytes", str(8 * 1024 * 1024),
        "--externalize-threshold", "4096",
    ) as port:
        yield port


@pytest.fixture(scope="session")
def conformance_http_cors_port(conformance_fake_storage: str) -> Iterator[int]:
    """A CORS-enabled worker that *also* has storage on.

    Storage is deliberate: the exposure check derives what it expects from
    what the worker advertises, so a plain worker never advertises the
    conditional capability headers and a server that forgets to expose them
    still passes.  Those are the likeliest to be missed.
    """
    with spawn_http(
        "--cors-origin", CONFORMANCE_CORS_ORIGIN,
        "--fake-storage", conformance_fake_storage,
        "--externalize-threshold", "4096",
    ) as port:
        yield port


@pytest.fixture(scope="session")
def conformance_http_cold_call_cache_port() -> Iterator[int]:
    """A worker with the call-state cache disabled.

    The cache is a pure accelerator, so with it warm a client that never
    echoes the call token still works — the obligation only surfaces once a
    continuation lands somewhere with no cached entry.
    """
    with spawn_http("--no-call-state-cache") as port:
        yield port


@pytest.fixture(scope="session")
def proof_worker_factory() -> Iterator[Callable[..., Any]]:
    """Spawn workers gated on proxy proof, one per configuration the suite asks for.

    A factory rather than a fixture per configuration: that shape is what lets
    the shared suite add a case without touching every port's repository.
    """
    from vgi_rpc.conformance.proof_harness import ProofWorker, ProofWorkerConfig

    @contextlib.contextmanager
    def spawn(config: ProofWorkerConfig) -> Iterator[ProofWorker]:
        args = [
            "--prefix", "/vgi",
            "--proof-mode", config.mode,
            "--proof-origin-id", config.origin_id,
            "--proof-secrets", config.secrets,
            "--proof-skew", str(config.skew_seconds),
        ]
        if not config.replay_cache:
            args.append("--proof-no-replay-cache")
        with spawn_http(*args) as port:
            yield ProofWorker(port=port, prefix="/vgi", config=config)

    yield spawn


@pytest.fixture(scope="session")
def conformance_http_sticky_auth_port() -> Iterator[int]:
    """A sticky worker that authenticates the ``X-Conformance-Principal`` header.

    Mounted at the same prefix as the plain worker, and anonymous when the
    header is absent: the suite probes capabilities before it authenticates
    anything, so a hook that refused unauthenticated requests would fail the
    group at the gate.
    """
    with spawn_http("--sticky-auth") as port:
        yield port
