#!/usr/bin/env bash
# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0
#
# Run the official vgi-rpc cross-language conformance suites against the C++
# worker, over every transport it supports.
#
# Two suites, and both are load bearing:
#
#   1. `vgi-rpc-test` — the CLI runner, driven over pipe (with and without the
#      shared-memory side channel), Unix socket, TCP and HTTP, plus access-log
#      validation.
#   2. `pytest tests/conformance/test_suite.py` — the shared pytest suite from
#      `vgi_rpc.conformance._pytest_suite`, which covers the capability-gated
#      HTTP groups the CLI runner has no way to reach: sticky sessions, proxy
#      proof, CORS, standardized 401s, vgi_rpc.Identity.v1, compression
#      negotiation, external locations, and the response caps. Optional cloud
#      backend tests run in their own explicitly skippable group.
#
# Prerequisites:
#   - `vgi-rpc-test` on PATH  (pip install 'vgi-rpc[http,conformance]')
#   - the built worker binary  (cmake --build)
#
# Usage:
#   scripts/run_conformance.sh [path/to/conformance_worker]
# or set CONFORMANCE_WORKER. Exits non-zero if any suite/transport fails.
#
# Parallelism. Every leg below (each CLI transport, the pytest suite, the
# optional cloud suite) is independent -- its own worker process, an ephemeral
# port or a socket under its own temp dir -- so they run concurrently, each
# into its own log, printed grouped in the fixed order afterwards. The pytest
# legs additionally fan out across pytest-xdist workers:
#
#   CONFORMANCE_WORKERS=N     pytest -n N per pytest leg (default: auto, the
#                             cores split between the two pytest legs, which
#                             run at the same time)
#   CONFORMANCE_LEG_JOBS=N    at most N legs at once (default: all of them)
#   CONFORMANCE_WORKERS=0     fully serial: legs one after another, no -n
#
# Without pytest-xdist installed the pytest legs run serially (with a warning)
# rather than failing on an unknown -n flag.

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

CONFORMANCE_WORKERS="${CONFORMANCE_WORKERS:-auto}"
PYTEST_PAR=()
if [[ "$CONFORMANCE_WORKERS" != "0" ]]; then
  if python3 -c "import xdist" 2>/dev/null; then
    workers="$CONFORMANCE_WORKERS"
    # Two pytest legs run concurrently; `-n auto` in each would start two
    # workers per core.
    if [[ "$workers" == "auto" ]]; then
      workers=$(( $(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 2) / 2 ))
      (( workers < 1 )) && workers=1
    fi
    PYTEST_PAR=(-n "$workers")
  else
    echo "WARNING: pytest-xdist not installed; pytest legs run serially" >&2
  fi
fi

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
  # Validated with the access log at DEBUG, its most verbose, which is where a
  # payload leak would show.  No `--require-request-data`: request payloads
  # are never logged at any level (docs/access-log-spec.md §4.3), and the
  # reference now rejects that flag.
  echo "::group::conformance: pipe + access-log"
  local log="$TMPDIR_RUN/access.jsonl"
  vgi-rpc-test --cmd "$WORKER --access-log $log --access-log-debug" \
               --access-log "$log" --format table || rc=1
  echo "::endgroup::"
}

# The SHM side channel rides alongside the pipe, so this is the pipe suite with
# a segment attached. VGI_RPC_SHM_MIN_BATCH_BYTES=1 forces *every* data batch
# through it rather than only the handful of conformance payloads that clear
# the 128 KiB default — at the default gate the dictionary-encoding path went
# unexercised, and it was broken.
run_shm() {
  echo "::group::conformance: pipe + shared memory"
  VGI_RPC_SHM_MIN_BATCH_BYTES=1 vgi-rpc-test --cmd "$WORKER" --shm 67108864 --format table || rc=1
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
    # Zero skips, enforced. Several shared groups skip *loudly* when the
    # runner lacks a fixture (the secondary-protocol and error-model groups
    # without `conformance_protocol_connector`, the identity groups without an
    # identity worker), and a skip there is an unimplemented deliverable that
    # a pass/fail exit code would report as green.
    local log="$TMPDIR_RUN/pytest_suite.log"
    VGI_RPC_CPP_WORKER="$WORKER" \
      python3 -m pytest \
        "$REPO_ROOT/tests/conformance/test_suite.py" \
        "$REPO_ROOT/tests/conformance/test_polymorphic_stream.py" \
        "$REPO_ROOT/tests/conformance/test_http_concurrency.py" \
        "$REPO_ROOT/tests/conformance/test_socket_concurrency.py" \
        -q -p no:randomly -rs ${PYTEST_PAR[@]+"${PYTEST_PAR[@]}"} 2>&1 | tee "$log"
    [[ "${PIPESTATUS[0]}" -eq 0 ]] || rc=1
    if grep -q '^SKIPPED' "$log"; then
      echo "ERROR: the mandatory shared suite skipped tests (see SKIPPED lines above)" >&2
      rc=1
    fi
    # The multi-protocol / error-model groups must have run, not merely been
    # collected out of existence by a narrowed import. One collection covers
    # every group; each must appear in it.
    local groups=(TestSecondaryIsHosted TestErrorModelRoundTrip TestErrorModelOnTheWire
                  TestTracebackPolicy TestUnavailableCarriesARetryHint)
    local expr collected group
    expr="$(printf ' or %s' "${groups[@]}")"; expr="${expr# or }"
    # Captured, not piped into `grep -q`: under pipefail, grep exiting at the
    # first match SIGPIPEs pytest and fails the pipeline.
    collected="$(VGI_RPC_CPP_WORKER="$WORKER" python3 -m pytest \
           "$REPO_ROOT/tests/conformance/test_suite.py" -q -p no:randomly \
           --collect-only -k "$expr" 2>/dev/null || true)"
    for group in "${groups[@]}"; do
      if ! grep -q "$group" <<<"$collected"; then
        echo "ERROR: shared group $group is not collected by tests/conformance/test_suite.py" >&2
        rc=1
      fi
    done
  fi
  echo "::endgroup::"
}

# Cloud backends are opt-in build profiles with intentionally heavyweight
# dependencies. Keep their result visible but separate from the mandatory
# portable suite above, whose expected skip count is zero.
run_optional_cloud_suite() {
  echo "::group::conformance: pytest suite (optional cloud backends)"
  if ! python3 -c "import pytest, vgi_rpc.conformance._pytest_suite" 2>/dev/null; then
    echo "WARNING: pytest or the shared suite is unavailable; skipping" >&2
  else
    VGI_RPC_CPP_WORKER="$WORKER" \
      python3 -m pytest "$REPO_ROOT/tests/conformance/test_cloud_storage.py" \
        -q -p no:randomly -rs ${PYTEST_PAR[@]+"${PYTEST_PAR[@]}"} || rc=1
  fi
  echo "::endgroup::"
}

# The capped run exists to activate the http_response_cap.* group, which
# scales its payloads off the advertised cap. large_payload.echo_binary_4mib
# sends a fixed 4 MiB and would (correctly) strict-fail against a 1 MiB cap,
# so it is excluded there -- the uncapped run and all three raw-framing
# transports already cover it.
LEGS=(
  "run_pipe"
  "run_pipe_access_log"
  "run_shm"
  "run_unix"
  "run_tcp"
  "run_http no-cap ''"
  "run_http capped '!large_payload.echo_binary_4mib' --max-response-bytes 1048576"
  "run_pytest_suite"
  "run_optional_cloud_suite"
)

# One leg in a subshell with its own rc; its status lands in a file so the
# result never depends on which `wait` reaped it.
run_leg() {
  local i="$1" start=$SECONDS
  ( rc=0; eval "${LEGS[$i]}"; echo "($(( SECONDS - start ))s: ${LEGS[$i]%% *})"; exit "$rc" ) \
    >"$TMPDIR_RUN/leg-$i.log" 2>&1
  echo "$?" >"$TMPDIR_RUN/leg-$i.rc"
}

if [[ "$CONFORMANCE_WORKERS" == "0" ]]; then
  LEG_JOBS=1
else
  LEG_JOBS="${CONFORMANCE_LEG_JOBS:-${#LEGS[@]}}"
fi
for i in "${!LEGS[@]}"; do
  if [[ "$LEG_JOBS" -le 1 ]]; then
    run_leg "$i"
  else
    # `wait -n` needs bash >= 4.3; macOS's stock 3.2 falls back to polling.
    while [[ "$(jobs -rp | wc -l)" -ge "$LEG_JOBS" ]]; do wait -n 2>/dev/null || sleep 0.2; done
    run_leg "$i" &
  fi
done
wait
# Logs are printed whole, in LEGS order, once every leg has finished.
for i in "${!LEGS[@]}"; do
  cat "$TMPDIR_RUN/leg-$i.log"
  [[ "$(cat "$TMPDIR_RUN/leg-$i.rc" 2>/dev/null)" == "0" ]] || rc=1
done

if [[ "$rc" -ne 0 ]]; then
  echo "CONFORMANCE FAILED" >&2
else
  echo "CONFORMANCE PASSED (all transports)"
fi
exit "$rc"
