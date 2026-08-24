# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Run the shared vgi-rpc pytest conformance suite against the C++ worker.

The suite is the Python reference's own; importing it here is what keeps this
port honest as the protocol moves.  All fixtures come from ``conftest.py``.
"""

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
