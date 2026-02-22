# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

add_executable(hello_world hello_world.cpp)
target_link_libraries(hello_world PRIVATE vgi_rpc)

add_executable(calculator calculator.cpp)
target_link_libraries(calculator PRIVATE vgi_rpc)

add_executable(streaming streaming.cpp)
target_link_libraries(streaming PRIVATE vgi_rpc)

add_executable(quick_example quick_example.cpp)
target_link_libraries(quick_example PRIVATE vgi_rpc)

add_executable(getting_started getting_started.cpp)
target_link_libraries(getting_started PRIVATE vgi_rpc)
