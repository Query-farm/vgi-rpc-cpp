#!/usr/bin/env bash
# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0
#
# Run the shared conformance suite in *client* role: every connection is
# opened by this port's client, reached through the JSONL driver in
# conformance/conformance_client_driver.cpp.
#
# `scripts/run_conformance.sh` is the mirror image of this — it drives this
# port's *server* with the reference Python client. Neither covers the other
# half, and only this one can:
#
#   > A conforming client passes the conformance suite against the reference
#   > server.
#
# A green run against our own server proves the two halves of this port agree
# with each other; it does not prove either agrees with the protocol, because
# every accommodation a server makes for the client it ships with is invisible
# to that pair and only that pair. So the leg that counts is `python`.
#
# Usage:
#   scripts/run_client_conformance.sh            # both legs (python is the gate)
#   scripts/run_client_conformance.sh python     # the gate alone
#   scripts/run_client_conformance.sh cpp        # our own server, for triage
#
# Environment:
#   VGI_CLIENT_DRIVER     driver argv (default: build/conformance/conformance_client_driver)
#   VGI_RPC_CPP_WORKER    C++ conformance worker (default: build/conformance/conformance_worker)
#   VGI_RPC_PYTHON        interpreter with the Python reference importable
#   VGI_RPC_PYTHON_REPO   vgi-rpc-python checkout holding tests/serve_conformance_*.py
#   PYTEST                pytest command (default: python3 -m pytest)

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DRIVER="${VGI_CLIENT_DRIVER:-$ROOT/build/conformance/conformance_client_driver}"
WORKER="${VGI_RPC_CPP_WORKER:-$ROOT/build/conformance/conformance_worker}"
PYTEST_CMD="${PYTEST:-python3 -m pytest}"

if [[ ! -x "${DRIVER%% *}" ]]; then
  echo "ERROR: client driver not built: ${DRIVER%% *}" >&2
  echo "  cmake --build build --target conformance_client_driver" >&2
  exit 2
fi

legs=("$@")
[[ ${#legs[@]} -eq 0 ]] && legs=(python cpp)

rc=0
for leg in "${legs[@]}"; do
  case "$leg" in
    python) label="the Python reference server (the gate)" ;;
    cpp)    label="this port's own server (regression check)" ;;
    *) echo "ERROR: unknown leg '$leg' (expected 'python' or 'cpp')" >&2; exit 2 ;;
  esac
  echo "::group::client role vs $label"
  VGI_CONFORMANCE_ROLE=client \
  VGI_CONFORMANCE_SERVER="$leg" \
  VGI_CLIENT_DRIVER="$DRIVER" \
  VGI_RPC_CPP_WORKER="$WORKER" \
    $PYTEST_CMD "$ROOT/tests/conformance/test_suite.py" -q -p no:randomly || rc=1
  echo "::endgroup::"
done

if [[ "$rc" -ne 0 ]]; then
  echo "CLIENT CONFORMANCE FAILED" >&2
else
  echo "CLIENT CONFORMANCE PASSED"
fi
exit "$rc"
