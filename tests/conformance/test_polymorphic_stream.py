# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Dynamic producer/exchange dispatch parity across every C++ transport."""

from __future__ import annotations

import contextlib
import os
import shutil
import sys
import tempfile
from collections.abc import Iterator
from typing import Protocol

import pyarrow as pa
import pytest
from conftest import _spawn_listener, spawn_http, worker_path
from vgi_rpc.http import http_connect
from vgi_rpc.rpc import (
    AnnotatedBatch,
    Stream,
    StreamState,
    SubprocessTransport,
    _RpcProxy,
    tcp_connect,
    unix_connect,
)


class _PolymorphicService(Protocol):
    def polymorphic_stream(self, as_producer: bool) -> Stream[StreamState]: ...


@contextlib.contextmanager
def _connection(kind: str) -> Iterator[object]:
    flag = "--polymorphic-stream-probe"
    if kind == "pipe":
        transport = SubprocessTransport([worker_path(), flag])
        try:
            yield _RpcProxy(_PolymorphicService, transport, None)
        finally:
            transport.close()
        return
    if kind == "http":
        with (
            spawn_http(flag) as port,
            http_connect(_PolymorphicService, f"http://127.0.0.1:{port}") as proxy,
        ):
            yield proxy
        return
    if kind == "tcp":
        with _spawn_listener("--tcp", "127.0.0.1:0", "TCP:", flag) as address:
            host, _, port = address.rpartition(":")
            with tcp_connect(_PolymorphicService, host, int(port)) as proxy:
                yield proxy
        return

    tmpdir = tempfile.mkdtemp(prefix="vgi-cpp-poly-", dir=tempfile.gettempdir())
    path = os.path.join(tmpdir, "p.sock")
    try:
        with (
            _spawn_listener("--unix", path, "UNIX:", flag) as socket_path,
            unix_connect(_PolymorphicService, socket_path) as proxy,
        ):
            yield proxy
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.mark.parametrize(
    "kind",
    [
        "pipe",
        "http",
        pytest.param(
            "unix",
            marks=pytest.mark.skipif(sys.platform == "win32", reason="POSIX only"),
        ),
        pytest.param(
            "tcp",
            marks=pytest.mark.skipif(sys.platform == "win32", reason="POSIX only"),
        ),
    ],
)
def test_factory_state_selects_producer_or_exchange_dispatch(kind: str) -> None:
    with _connection(kind) as proxy:
        produced = list(proxy.polymorphic_stream(as_producer=True))
        assert len(produced) == 1
        assert produced[0].batch.column("value").to_pylist() == [42.0]

        with proxy.polymorphic_stream(as_producer=False) as session:
            result = session.exchange(AnnotatedBatch(pa.record_batch({"value": [3.0]})))
        assert result.batch.column("value").to_pylist() == [6.0]
