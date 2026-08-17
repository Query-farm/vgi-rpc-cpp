#!/usr/bin/env bash
# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0
#
# Run the official vgi-rpc cross-language conformance suites against the C++
# worker, over every transport it supports.
#
# Two suites, and both are load bearing:
#
#   1. `vgi-rpc-test` — the CLI runner, driven over pipe, Unix socket, TCP and
#      HTTP, plus access-log validation.
#   2. `pytest tests/conformance` — the shared pytest suite from
#      `vgi_rpc.conformance._pytest_suite`, which covers the capability-gated
#      HTTP groups the CLI runner has no way to reach: sticky sessions, proxy
#      proof, CORS, standardized 401s, token introspection, compression
#      negotiation, external locations, and the response caps.
#
# Prerequisites:
#   - `vgi-rpc-test` on PATH  (pip install 'vgi-rpc[http,conformance]')
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
  echo "ERROR: vgi-rpc-test not on PATH (pip install 'vgi-rpc[http,conformance]')" >&2
  exit 2
fi

WORKER="$(cd "$(dirname "$WORKER")" && pwd)/$(basename "$WORKER")"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Short, because sockaddr_un.sun_path is 104 bytes on macOS and the usual
# mktemp -d under TMPDIR overruns it.
TMPDIR_RUN="$(mktemp -d "${TMPDIR:-/tmp}/vgiXXXX")"
trap 'rm -rf "$TMPDIR_RUN"' EXIT

rc=0

# Wait for a worker to print its discovery line, then echo the payload.
# $1 = output file, $2 = line prefix, $3 = pid
await_line() {
  local out="$1" prefix="$2" pid="$3" line=""
  for _ in $(seq 1 100); do
    line="$(grep -m1 "^${prefix}" "$out" 2>/dev/null)"
    [[ -n "$line" ]] && break
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.2
  done
  [[ -z "$line" ]] && return 1
  echo "${line#"$prefix"}"
}

run_pipe() {
  echo "::group::conformance: pipe"
  vgi-rpc-test --cmd "$WORKER" --format table || rc=1
  echo "::endgroup::"
}

run_pipe_access_log() {
  # Two passes.  The first validates every record the full suite produces; the
  # second additionally requires `request_data` to be present, which the
  # large_payload tests legitimately shed under the per-record size cap (the
  # Python reference sheds it there too), so they are excluded from that pass
  # rather than the rule being weakened.
  echo "::group::conformance: pipe + access-log"
  local log="$TMPDIR_RUN/access.jsonl"
  vgi-rpc-test --cmd "$WORKER --access-log $log --access-log-debug" \
               --access-log "$log" --format table || rc=1
  echo "::endgroup::"

  echo "::group::conformance: pipe + access-log (--require-request-data)"
  local log2="$TMPDIR_RUN/access-payload.jsonl"
  vgi-rpc-test --cmd "$WORKER --access-log $log2 --access-log-debug" \
               --access-log "$log2" --require-request-data \
               --filter '!large_payload.echo_binary_over_int32_max,!large_payload.echo_binary_4mib' \
               --format table || rc=1
  echo "::endgroup::"
}

run_unix() {
  echo "::group::conformance: unix socket"
  local out="$TMPDIR_RUN/unix.out" sock="$TMPDIR_RUN/c.sock"
  "$WORKER" --unix "$sock" >"$out" 2>&1 &
  local srv=$!
  if ! await_line "$out" "UNIX:" "$srv" >/dev/null; then
    echo "ERROR: worker did not report a unix socket" >&2
    cat "$out" >&2
    rc=1
  else
    vgi-rpc-test --unix "$sock" --format table || rc=1
  fi
  kill "$srv" 2>/dev/null
  wait "$srv" 2>/dev/null
  echo "::endgroup::"
}

run_tcp() {
  echo "::group::conformance: tcp"
  local out="$TMPDIR_RUN/tcp.out"
  "$WORKER" --tcp 127.0.0.1:0 >"$out" 2>&1 &
  local srv=$!
  local addr
  if ! addr="$(await_line "$out" "TCP:" "$srv")"; then
    echo "ERROR: worker did not report a tcp address" >&2
    cat "$out" >&2
    rc=1
  else
    vgi-rpc-test --tcp "$addr" --format table || rc=1
  fi
  kill "$srv" 2>/dev/null
  wait "$srv" 2>/dev/null
  echo "::endgroup::"
}

# Start the worker in --http mode, wait for PORT:<n>, run the suite, tear down.
# $1 is a label, $2 is a --filter expression ("" for none), the rest go to the
# worker.
run_http() {
  local label="$1" filter="$2"; shift 2
  echo "::group::conformance: http ($label)"
  local out="$TMPDIR_RUN/http_$label.out"
  "$WORKER" --http --host 127.0.0.1 --port 0 "$@" >"$out" 2>&1 &
  local srv=$!
  local port
  if ! port="$(await_line "$out" "PORT:" "$srv")"; then
    echo "ERROR: worker did not report a port" >&2
    cat "$out" >&2
    rc=1
  elif [[ -n "$filter" ]]; then
    vgi-rpc-test --url "http://127.0.0.1:$port" --filter "$filter" --format table || rc=1
  else
    vgi-rpc-test --url "http://127.0.0.1:$port" --format table || rc=1
  fi
  kill "$srv" 2>/dev/null
  wait "$srv" 2>/dev/null
  echo "::endgroup::"
}

# The shared pytest suite, which reaches the capability-gated HTTP groups the
# CLI runner cannot. Skipped with a warning rather than failing the run when
# pytest is unavailable, so a bare `pip install vgi-rpc` still gets the CLI
# coverage above.
run_pytest_suite() {
  echo "::group::conformance: pytest suite (capability-gated HTTP groups)"
  if ! python3 -c "import pytest, vgi_rpc.conformance._pytest_suite" 2>/dev/null; then
    echo "WARNING: pytest or the shared suite is unavailable; skipping" >&2
  else
    VGI_RPC_CPP_WORKER="$WORKER" \
      python3 -m pytest "$REPO_ROOT/tests/conformance" -q -p no:randomly || rc=1
  fi
  echo "::endgroup::"
}

run_pipe
run_pipe_access_log
run_unix
run_tcp
run_http "no-cap" ""
# The capped run exists to activate the http_response_cap.* group, which
# scales its payloads off the advertised cap. large_payload.echo_binary_4mib
# sends a fixed 4 MiB and would (correctly) strict-fail against a 1 MiB cap,
# so it is excluded here — the uncapped run above and all three raw-framing
# transports already cover it.
run_http "capped" '!large_payload.echo_binary_4mib' --max-response-bytes 1048576
run_pytest_suite

if [[ "$rc" -ne 0 ]]; then
  echo "CONFORMANCE FAILED" >&2
else
  echo "CONFORMANCE PASSED (all transports)"
fi
exit "$rc"
