#!/usr/bin/env bash
# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0
#
# Run the official vgi-rpc cross-language conformance suite (the `vgi-rpc-test`
# CLI from the Python reference package) against the C++ conformance worker,
# over every transport the worker supports:
#
#   1. pipe / subprocess          (vgi-rpc-test --cmd)
#   2. pipe + access-log          (validates the JSONL access log too)
#   3. HTTP, no response cap      (vgi-rpc-test --url)
#   4. HTTP, with response cap    (activates the http_response_cap.* tests)
#
# Prerequisites:
#   - `vgi-rpc-test` on PATH  (pip install 'vgi-rpc[http]')
#   - the built worker binary  (cmake --build)
#
# Usage:
#   scripts/run_conformance.sh [path/to/conformance_worker]
# or set CONFORMANCE_WORKER. Exits non-zero if any suite/transport fails.

set -uo pipefail

WORKER="${1:-${CONFORMANCE_WORKER:-build/conformance/conformance_worker}}"
if [[ ! -x "$WORKER" ]]; then
  echo "ERROR: worker not found or not executable: $WORKER" >&2
  exit 2
fi
if ! command -v vgi-rpc-test >/dev/null 2>&1; then
  echo "ERROR: vgi-rpc-test not on PATH (pip install 'vgi-rpc[http]')" >&2
  exit 2
fi

WORKER="$(cd "$(dirname "$WORKER")" && pwd)/$(basename "$WORKER")"
TMPDIR_RUN="$(mktemp -d)"
trap 'rm -rf "$TMPDIR_RUN"' EXIT

rc=0

run_pipe() {
  echo "::group::conformance: pipe"
  vgi-rpc-test --cmd "$WORKER" --format table || rc=1
  echo "::endgroup::"
}

run_pipe_access_log() {
  echo "::group::conformance: pipe + access-log"
  local log="$TMPDIR_RUN/access.jsonl"
  vgi-rpc-test --cmd "$WORKER --access-log $log" --access-log "$log" --format table || rc=1
  echo "::endgroup::"
}

# Start the worker in --http mode, wait for it to print PORT:<n>, run the suite,
# then tear the server down.  $1 is a label, remaining args go to the worker.
run_http() {
  local label="$1"; shift
  echo "::group::conformance: http ($label)"
  local out="$TMPDIR_RUN/http_$label.out"
  "$WORKER" --http --host 127.0.0.1 --port 0 "$@" >"$out" 2>&1 &
  local srv=$!
  local port=""
  for _ in $(seq 1 50); do
    port="$(grep -m1 '^PORT:' "$out" 2>/dev/null | cut -d: -f2 | tr -d '[:space:]')"
    [[ -n "$port" ]] && break
    kill -0 "$srv" 2>/dev/null || break
    sleep 0.2
  done
  if [[ -z "$port" ]]; then
    echo "ERROR: worker did not report a port" >&2
    cat "$out" >&2
    kill "$srv" 2>/dev/null
    rc=1
    echo "::endgroup::"
    return
  fi
  vgi-rpc-test --url "http://127.0.0.1:$port" --format table || rc=1
  kill "$srv" 2>/dev/null
  wait "$srv" 2>/dev/null
  echo "::endgroup::"
}

run_pipe
run_pipe_access_log
run_http "no-cap"
run_http "capped" --max-response-bytes 1048576

if [[ "$rc" -ne 0 ]]; then
  echo "CONFORMANCE FAILED" >&2
else
  echo "CONFORMANCE PASSED (all transports)"
fi
exit "$rc"
