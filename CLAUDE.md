# CLAUDE.md - Project Instructions for Claude Code

## Project Overview

vgi-rpc-cpp is a C++20 RPC framework built on Apache Arrow IPC. It provides unary and streaming (producer/exchange) method patterns over pipe-based transport (stdin/stdout). Single-threaded by design.

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

**The Python vgi-rpc conformance suite is the definitive test source.** The C++ unit tests exist for fast feedback on core utilities (request parsing, wire protocol, metadata) but should be kept minimal. Do NOT write extensive standalone C++ tests that duplicate protocol behavior — they will break when the protocol evolves and the Python reference implementation is always authoritative.

Prefer:
- Conformance tests (Python) for protocol correctness and end-to-end validation
- Minimal C++ unit tests only for internal utilities and type conversion logic
- Running sanitizer builds to catch memory/UB issues rather than writing more unit tests

## Code Style

- C++20 standard
- snake_case for functions/variables, PascalCase for classes/structs
- Single `vgi_rpc` namespace, `vgi_rpc::keys` for protocol constants
- `#pragma once` for include guards
- Smart pointers throughout — no raw new/delete
- Exceptions for errors, `std::optional` for nullable access
- Arrow errors converted via `unwrap()` / `VGI_RPC_THROW_NOT_OK()`
