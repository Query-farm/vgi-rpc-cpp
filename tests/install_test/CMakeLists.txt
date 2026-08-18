# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

cmake_minimum_required(VERSION 3.20)
project(vgi_rpc_install_test LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(vgi_rpc REQUIRED)
add_executable(install_test main.cpp)
target_link_libraries(install_test vgi_rpc::vgi_rpc)
