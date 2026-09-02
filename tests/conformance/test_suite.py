# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Run the shared vgi-rpc pytest conformance suite against the C++ worker.

The suite is the Python reference's own; importing it here is what keeps this
port honest as the protocol moves.  All fixtures come from ``conftest.py``.
"""

import httpx2
import pytest

from vgi_rpc.conformance._pytest_suite import *  # noqa: F403
from vgi_rpc.conformance._pytest_suite import (  # noqa: E402
    _advertised_encodings,
    _response_codec,
)


class TestHttpCompressionNegotiationConformance(  # type: ignore[no-redef]  # noqa: F811
    TestHttpCompressionNegotiationConformance  # noqa: F405
):
    """Require both standard HTTP wire codecs from the C++ server."""

    def test_zstd_and_gzip_are_mandatory(self, conformance_http_port: int) -> None:
        advertised = _advertised_encodings(conformance_http_port)
        assert advertised == ["zstd", "gzip"]

        for codec in advertised:
            response = self._echo(
                conformance_http_port,
                {"X-VGI-Accept-Encoding": codec, "Accept-Encoding": ""},
            )
            assert _response_codec(response) == codec
            assert response.headers.get("X-VGI-Content-Encoding", "").strip().lower() == codec


def test_response_budget_below_floor_is_arrow_value_error(
    conformance_http_port: int,
) -> None:
    response = httpx2.post(
        f"http://127.0.0.1:{conformance_http_port}/no_such_method",
        headers={
            "Content-Type": "application/vnd.apache.arrow.stream",
            "Accept-Encoding": "identity",
            "VGI-Accept-Max-Response-Bytes": "65535",
        },
        content=b"",
        timeout=10.0,
    )
    assert response.status_code == 400
    assert response.headers["X-VGI-RPC-Error"] == "true"
    assert response.headers["VGI-Accept-Max-Response-Bytes-Support"] == "true"
    assert response.headers["Content-Type"].startswith("application/vnd.apache.arrow.stream")
    assert b"ValueError" in response.content
    assert b"65536" in response.content


@pytest.mark.parametrize(
    "headers",
    [
        [("VGI-Accept-Max-Response-Bytes", "invalid")],
        [("VGI-Accept-Max-Response-Bytes", "65535")],
        [
            ("VGI-Accept-Max-Response-Bytes", "65536"),
            ("VGI-Accept-Max-Response-Bytes", "65537"),
        ],
    ],
)
def test_options_validates_present_response_budget(
    conformance_http_port: int, headers: list[tuple[str, str]]
) -> None:
    response = httpx2.options(
        f"http://127.0.0.1:{conformance_http_port}/health",
        headers=headers,
        timeout=10.0,
    )
    assert response.status_code == 400
    assert response.headers["X-VGI-RPC-Error"] == "true"
    assert response.headers["VGI-Accept-Max-Response-Bytes-Support"] == "true"
    assert response.headers["Content-Type"].startswith("application/vnd.apache.arrow.stream")
    assert b"ValueError" in response.content


def test_options_allows_missing_response_budget(conformance_http_port: int) -> None:
    response = httpx2.options(
        f"http://127.0.0.1:{conformance_http_port}/health", timeout=10.0
    )
    assert response.status_code == 200
    assert response.headers["VGI-Accept-Max-Response-Bytes-Support"] == "true"
