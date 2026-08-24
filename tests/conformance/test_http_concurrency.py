# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""HTTP dispatch concurrency without weakening per-object lock-step."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Protocol

import httpx2
from conftest import spawn_http
from vgi_rpc.http import http_connect


class _ConcurrencyService(Protocol):
    def rendezvous(self, tag: int) -> int: ...


def test_unrelated_http_handlers_execute_concurrently() -> None:
    with (
        spawn_http("--http-concurrency-probe") as port,
        httpx2.Client(
            base_url=f"http://127.0.0.1:{port}", headers={"Accept-Encoding": "identity"}
        ) as first_client,
        httpx2.Client(
            base_url=f"http://127.0.0.1:{port}", headers={"Accept-Encoding": "identity"}
        ) as second_client,
        http_connect(
            _ConcurrencyService, client=first_client, compression_level=None
        ) as first,
        http_connect(
            _ConcurrencyService, client=second_client, compression_level=None
        ) as second,
        ThreadPoolExecutor(max_workers=2) as pool,
    ):
        first_result = pool.submit(first.rendezvous, tag=1)
        second_result = pool.submit(second.rendezvous, tag=2)
        assert first_result.result(timeout=10) == 1
        assert second_result.result(timeout=10) == 2
