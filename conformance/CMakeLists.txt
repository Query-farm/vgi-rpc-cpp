# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

add_executable(conformance_worker conformance_worker.cpp)
target_link_libraries(conformance_worker PRIVATE vgi_rpc)

# The client-side half of conformance: a JSONL bridge that lets the shared
# Python suite drive *this port's client*, against the Python reference server
# as well as our own.  A server-only suite cannot validate a client — every
# accommodation a server makes for the client it ships with is invisible to
# that pair and only that pair.
add_executable(conformance_client_driver conformance_client_driver.cpp)
target_link_libraries(conformance_client_driver PRIVATE vgi_rpc)

# The `conformance_negotiating_client` runner hook (PROTOCOL_VERSIONS_V1.md §9):
# this port's HTTP client negotiating protocol versions against a scripted
# reference server.  Its own binary because the client-driver harness stamps
# vgi_rpc.protocol_version itself, which is the thing under test.
add_executable(conformance_negotiating_client conformance_negotiating_client.cpp)
target_link_libraries(conformance_negotiating_client PRIVATE vgi_rpc)
