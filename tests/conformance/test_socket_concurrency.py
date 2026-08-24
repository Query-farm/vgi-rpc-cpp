# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Raw socket listeners dispatch independent connections concurrently."""

from __future__ import annotations

import os
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, wait
from contextlib import ExitStack
from typing import Protocol

import pytest
from conftest import _spawn_listener
from vgi_rpc.rpc import tcp_connect, unix_connect


class _ConcurrencyService(Protocol):
    protocol_version = "2.0.0"

    def rendezvous(self, tag: int) -> int: ...


def _assert_concurrent(first: _ConcurrencyService, second: _ConcurrencyService) -> None:
    pool = ThreadPoolExecutor(max_workers=2)
    pending = set()
    try:
        first_result = pool.submit(first.rendezvous, tag=1)
        second_result = pool.submit(second.rendezvous, tag=2)
        done, pending = wait((first_result, second_result), timeout=10)
        if pending:
            pytest.fail("independent socket connections were serialized")
        assert first_result.result() == 1
        assert second_result.result() == 2
    finally:
        # Do not turn a useful timeout failure into an indefinitely blocked
        # executor shutdown. Closing the surrounding clients releases any
        # calls still waiting in a regressed worker.
        pool.shutdown(wait=not pending, cancel_futures=True)


@pytest.mark.skipif(sys.platform == "win32", reason="raw sockets are POSIX-only")
def test_unix_connections_execute_concurrently() -> None:
    with tempfile.TemporaryDirectory(prefix="vgi-cpp-") as tmpdir:
        socket_path = os.path.join(tmpdir, "concurrency.sock")
        with (
            _spawn_listener(
                "--unix", socket_path, "UNIX:", "--http-concurrency-probe"
            ) as bound_path,
            unix_connect(_ConcurrencyService, bound_path) as first,
            unix_connect(_ConcurrencyService, bound_path) as second,
        ):
            _assert_concurrent(first, second)


@pytest.mark.skipif(sys.platform == "win32", reason="raw sockets are POSIX-only")
def test_tcp_connections_execute_concurrently() -> None:
    with ExitStack() as stack:
        address = stack.enter_context(
            _spawn_listener(
                "--tcp", "127.0.0.1:0", "TCP:", "--http-concurrency-probe"
            )
        )
        host, _, port = address.rpartition(":")
        first = stack.enter_context(tcp_connect(_ConcurrencyService, host, int(port)))
        second = stack.enter_context(tcp_connect(_ConcurrencyService, host, int(port)))
        _assert_concurrent(first, second)
