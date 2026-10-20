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
#   CONFORMANCE_WORKERS   pytest-xdist workers per leg (default: auto, the
#                         cores split between the legs, which run
#                         concurrently). 0 = fully serial (legs one
#                         after another, no -n).

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

LOG_DIR="$(mktemp -d "${TMPDIR:-/tmp}/vgiclXXXX")"
trap 'rm -rf "$LOG_DIR"' EXIT

CONFORMANCE_WORKERS="${CONFORMANCE_WORKERS:-auto}"
PYTEST_PAR=()
if [[ "$CONFORMANCE_WORKERS" != "0" ]]; then
  # Asked of pytest itself, since PYTEST may be any launcher. Captured, not
  # piped into `grep -q`: under pipefail its early exit SIGPIPEs pytest.
  pytest_help="$($PYTEST_CMD --help 2>/dev/null || true)"
  if grep -q -- '--numprocesses' <<<"$pytest_help"; then
    workers="$CONFORMANCE_WORKERS"
    # The legs run concurrently; `-n auto` in each would oversubscribe.
    if [[ "$workers" == "auto" ]]; then
      workers=$(( $(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 2) / ${#legs[@]} ))
      (( workers < 1 )) && workers=1
    fi
    PYTEST_PAR=(-n "$workers")
  else
    echo "WARNING: pytest-xdist not importable; legs run serially" >&2
  fi
fi

# One leg: the shared suite in client role against the named server. Its own
# servers/drivers/sockets come from the fixtures (ephemeral ports, private
# temp dirs), so legs are independent of each other.
run_leg() {
  local leg="$1" label rc=0
  case "$leg" in
    python) label="the Python reference server (the gate)" ;;
    cpp)    label="this port's own server (regression check)" ;;
  esac
  echo "::group::client role vs $label"
  VGI_CONFORMANCE_ROLE=client \
  VGI_CONFORMANCE_SERVER="$leg" \
  VGI_CLIENT_DRIVER="$DRIVER" \
  VGI_RPC_CPP_WORKER="$WORKER" \
    $PYTEST_CMD "$ROOT/tests/conformance/test_suite.py" -q -p no:randomly -rs \
      ${PYTEST_PAR[@]+"${PYTEST_PAR[@]}"} 2>&1 \
      | tee "$LOG_DIR/client-$leg.log"
  [[ "${PIPESTATUS[0]}" -eq 0 ]] || rc=1
  # A skip is a client deliverable not exercised (e.g. the error-model groups
  # without a protocol connector), so it fails the leg like a failure would.
  if grep -q '^SKIPPED' "$LOG_DIR/client-$leg.log"; then
    echo "ERROR: client role vs $leg skipped tests (see SKIPPED lines above)" >&2
    rc=1
  fi
  echo "::endgroup::"
  return "$rc"
}

for leg in "${legs[@]}"; do
  case "$leg" in
    python|cpp) ;;
    *) echo "ERROR: unknown leg '$leg' (expected 'python' or 'cpp')" >&2; exit 2 ;;
  esac
done

rc=0
if [[ "$CONFORMANCE_WORKERS" == "0" || ${#legs[@]} -eq 1 ]]; then
  for leg in "${legs[@]}"; do run_leg "$leg" || rc=1; done
else
  # Concurrent legs, output printed whole and in order once each finishes.
  pids=()
  for i in "${!legs[@]}"; do
    run_leg "${legs[$i]}" >"$LOG_DIR/leg-$i.out" 2>&1 &
    pids[$i]=$!
  done
  for i in "${!legs[@]}"; do
    wait "${pids[$i]}" || rc=1
    cat "$LOG_DIR/leg-$i.out"
  done
fi

if [[ "$rc" -ne 0 ]]; then
  echo "CLIENT CONFORMANCE FAILED" >&2
else
  echo "CLIENT CONFORMANCE PASSED"
fi
exit "$rc"
