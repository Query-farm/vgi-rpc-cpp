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
