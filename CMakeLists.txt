# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

cmake_minimum_required(VERSION 3.20)
project(vgi-rpc-cpp VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Default to hidden visibility so only VGI_RPC_EXPORT symbols are public
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_VISIBILITY_INLINES_HIDDEN ON)

find_package(Arrow CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)

add_library(vgi_rpc
    src/metadata.cpp
    src/wire.cpp
    src/log.cpp
    src/log_sink.cpp
    src/request.cpp
    src/result.cpp
    src/call_context.cpp
    src/server.cpp
    src/pipe_transport.cpp
    src/describe.cpp
    src/output_collector.cpp
)

target_compile_definitions(vgi_rpc PRIVATE VGI_RPC_BUILDING)

target_include_directories(vgi_rpc PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

option(VGI_RPC_ARROW_SHARED "Link against shared Arrow" OFF)
if(VGI_RPC_ARROW_SHARED)
    set(_VGI_RPC_ARROW_TARGET Arrow::arrow_shared)
else()
    set(_VGI_RPC_ARROW_TARGET Arrow::arrow_static)
endif()

target_link_libraries(vgi_rpc PUBLIC
    ${_VGI_RPC_ARROW_TARGET}
    nlohmann_json::nlohmann_json
)

add_subdirectory(examples)

# ── Install & Package Config ───────────────────────────────────────────
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

install(TARGETS vgi_rpc
    EXPORT vgi_rpc-targets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

install(DIRECTORY include/vgi_rpc
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    FILES_MATCHING PATTERN "*.h"
)

install(EXPORT vgi_rpc-targets
    FILE vgi_rpc-targets.cmake
    NAMESPACE vgi_rpc::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/vgi_rpc
)

configure_package_config_file(
    cmake/vgi_rpc-config.cmake.in
    "${CMAKE_CURRENT_BINARY_DIR}/vgi_rpc-config.cmake"
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/vgi_rpc
)

write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/vgi_rpc-config-version.cmake"
    COMPATIBILITY SameMajorVersion
)

install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/vgi_rpc-config.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/vgi_rpc-config-version.cmake"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/vgi_rpc
)

configure_file(cmake/vgi_rpc.pc.in vgi_rpc.pc @ONLY)
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/vgi_rpc.pc"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig
)

# ── Benchmarks (opt-in) ───────────────────────────────────────────────
option(VGI_RPC_BUILD_BENCHMARKS "Build vgi_rpc benchmarks" OFF)

# ── Tests (opt-in) ────────────────────────────────────────────────────
option(VGI_RPC_BUILD_TESTS "Build vgi_rpc unit tests" OFF)
if(VGI_RPC_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
    add_subdirectory(conformance)

    find_package(Python3 COMPONENTS Interpreter QUIET)
    if(Python3_FOUND)
        add_test(
            NAME conformance
            COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/test_conformance.py
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        )
        set_tests_properties(conformance PROPERTIES
            TIMEOUT 60
            ENVIRONMENT "CONFORMANCE_WORKER=$<TARGET_FILE:conformance_worker>"
        )
    endif()
endif()
