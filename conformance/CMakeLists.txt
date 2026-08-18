# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

add_executable(conformance_worker conformance_worker.cpp)
target_link_libraries(conformance_worker PRIVATE vgi_rpc)
