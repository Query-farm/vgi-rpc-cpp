# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Run the shared vgi-rpc pytest conformance suite against the C++ worker.

The suite is the Python reference's own; importing it here is what keeps this
port honest as the protocol moves.  All fixtures come from ``conftest.py``.
"""

from vgi_rpc.conformance._pytest_suite import *  # noqa: F403
