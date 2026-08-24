# CLAUDE.md - Project Instructions for Claude Code

## Project Overview

vgi-rpc-cpp is a C++20 RPC framework built on Apache Arrow IPC. It provides unary and streaming (producer/exchange) method patterns over four transports: pipe (stdin/stdout), Unix domain socket, TCP, and HTTP, plus a shared-memory side channel that rides alongside the raw-framing ones. Dispatch is single-threaded by design — the HTTP transport serializes requests under one mutex to preserve that.

The HTTP transport additionally carries the optional features of the wire spec: capability discovery, response and externalization caps, external-location pointer batches, bounded zstd/gzip request decoding and response negotiation, CORS, sticky sessions, standardized 401s, proxy proof, and token introspection. All are off by default except codec support, which is part of the HTTP wire baseline; `HttpConfig::compression = false` disables response compression without disabling request decoding.

External storage picks its backend by URL scheme: `http(s)://` always works, while `s3://` and `gs://` need the opt-in vcpkg manifest features (`VCPKG_MANIFEST_FEATURES="s3;gcs"` plus `-DVGI_RPC_WITH_S3=ON -DVGI_RPC_WITH_GCS=ON`). Keep them opt-in — aws-sdk-cpp and google-cloud-cpp roughly triple the dependency build time.

## Build

```bash
cmake --preset default    # Debug with tests
cmake --preset release    # Release build
cmake --preset sanitizer  # ASAN + UBSAN
cmake --build build
ctest --test-dir build
```

Dependencies managed via vcpkg (Arrow, nlohmann-json, Catch2).

## Testing Philosophy

**The Python vgi-rpc conformance suite is the definitive test source.** The C++ unit tests exist for fast feedback on core utilities (request parsing, wire protocol, metadata, crypto primitives) but should be kept minimal. Do NOT write extensive standalone C++ tests that duplicate protocol behavior — they will break when the protocol evolves and the Python reference implementation is always authoritative.

Two conformance suites, both driven by `scripts/run_conformance.sh`:

1. `vgi-rpc-test` — the CLI runner, over pipe, Unix socket, TCP, and HTTP, plus access-log validation.
2. `pytest tests/conformance/test_suite.py tests/conformance/test_polymorphic_stream.py` — the mandatory, zero-skip shared suite re-exported from `vgi_rpc.conformance._pytest_suite`, which reaches the capability-gated HTTP groups the CLI runner cannot: sticky sessions, proxy proof, CORS, 401s, token introspection, zstd/gzip compression negotiation, external locations, response caps. `tests/conformance/conftest.py` supplies the fixtures by spawning the worker with the matching flags. `test_cloud_storage.py` runs separately because S3/GCS are opt-in build profiles.

Prefer:
- Conformance tests (Python) for protocol correctness and end-to-end validation
- Minimal C++ unit tests only for internal utilities and type conversion logic — the exception is `tests/crypto_test.cpp`, which pins the hand-written primitives against published vectors (FIPS 180-4, RFC 4231, RFC 4648) and an envelope sealed by an independent implementation. A crypto implementation that only agrees with itself is worth nothing.
- Running sanitizer builds to catch memory/UB issues rather than writing more unit tests

## Code Style

- C++20 standard
- snake_case for functions/variables, PascalCase for classes/structs
- Single `vgi_rpc` namespace, `vgi_rpc::keys` for protocol constants
- `#pragma once` for include guards
- Smart pointers throughout — no raw new/delete
- Exceptions for errors, `std::optional` for nullable access
- Arrow errors converted via `unwrap()` / `VGI_RPC_THROW_NOT_OK()`

Formatting is not a matter of opinion here — run `scripts/format.sh` (or
`--check`, which is what CI runs before it builds anything). clang-format,
Google style with 4-space indent and a 100-column limit. The script pins the
major version, because clang-format's output changes between releases and
contributors on different versions reformat each other's files on every
commit.

`-Wall -Wextra` are on. `-Wreturn-stack-address` is the reason: it caught a
reference bound into the temporary `shared_ptr` that `ArrayBuilder::type()`
returns, which the full conformance suite ran straight past.

Line endings are LF, enforced by `.gitattributes`. The tree previously had
files that were part CRLF and part LF, which turned every small edit into a
whole-file diff.
