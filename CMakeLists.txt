# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

cmake_minimum_required(VERSION 3.20)
project(vgi-rpc-cpp VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# On, because they earn their keep: -Wreturn-stack-address catches a reference
# bound into a temporary `shared_ptr`, which the conformance suite runs past.
# Scoped to our own targets — vcpkg's headers are not ours to clean.
if(NOT MSVC)
  add_compile_options(-Wall -Wextra)
endif()

# Default to hidden visibility so only VGI_RPC_EXPORT symbols are public
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_VISIBILITY_INLINES_HIDDEN ON)

find_package(Arrow CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(httplib CONFIG REQUIRED)
find_package(zstd CONFIG REQUIRED)
# libsodium supplies every primitive the wire needs: XChaCha20-Poly1305 (the
# 24-byte-nonce AEAD the token envelopes use, which OpenSSL does not ship),
# SHA-256, HMAC-SHA256, a CSPRNG and a constant-time compare. Audited code in
# place of ours, and the same library the Python reference reaches through
# PyNaCl — so the two implementations agree by construction rather than by
# both having been written from the same RFC.
find_package(unofficial-sodium CONFIG REQUIRED)

# Cloud backends for external-location payloads.  Off by default: aws-sdk-cpp
# and google-cloud-cpp are long builds, and a deployment that externalises over
# pre-signed HTTPS URLs vended by its own service needs neither.  Enable with
#   cmake --preset default -DVCPKG_MANIFEST_FEATURES="s3;gcs" \
#         -DVGI_RPC_WITH_S3=ON -DVGI_RPC_WITH_GCS=ON
option(VGI_RPC_WITH_S3 "Build the S3 external-storage backend (aws-sdk-cpp)" OFF)
option(VGI_RPC_WITH_GCS "Build the GCS external-storage backend (google-cloud-cpp)" OFF)

if(VGI_RPC_WITH_S3)
    find_package(AWSSDK CONFIG REQUIRED COMPONENTS s3)
endif()
if(VGI_RPC_WITH_GCS)
    find_package(google_cloud_cpp_storage CONFIG REQUIRED)
endif()

add_library(vgi_rpc
    src/crypto.cpp
    src/errors.cpp
    src/external.cpp
    src/proxy_proof.cpp
    src/session.cpp
    src/shm.cpp
    src/metadata.cpp
    src/wire.cpp
    src/log.cpp
    src/log_sink.cpp
    src/request.cpp
    src/result.cpp
    src/call_context.cpp
    src/server.cpp
    src/pipe_transport.cpp
    src/socket_transport.cpp
    src/describe.cpp
    src/access_log.cpp
    src/http_transport.cpp
    src/http_client.cpp
    src/client.cpp
    src/client_description.cpp
    src/client_transport.cpp
    src/output_collector.cpp
)

target_compile_definitions(vgi_rpc PRIVATE VGI_RPC_BUILDING)
# Apple's libc++ ships the standard C++20 stop-token implementation behind
# its experimental-library feature gate.  HttpClient exposes std::stop_token,
# so propagate the gate to consumers of both the build and installed targets.
if(APPLE AND CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
    target_compile_definitions(vgi_rpc PUBLIC _LIBCPP_ENABLE_EXPERIMENTAL)
endif()
# PUBLIC so it reaches consumers, including those of the installed package:
# whether the dllimport attribute is correct is a property of how this library
# was built, and only this build knows it.
if(BUILD_SHARED_LIBS)
    target_compile_definitions(vgi_rpc PUBLIC VGI_RPC_SHARED)
endif()

target_include_directories(vgi_rpc PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

# Which Arrow target exists is decided by how Arrow was built, and vcpkg's
# default triplet differs by platform: static on Linux and macOS, a DLL on
# Windows.  Assuming either one turns a perfectly good Arrow into a "target
# not found" at generate time, so take what the found package actually
# defines unless an explicit choice was made.
option(VGI_RPC_ARROW_SHARED "Link against shared Arrow" OFF)
if(VGI_RPC_ARROW_SHARED)
    if(NOT TARGET Arrow::arrow_shared)
        message(FATAL_ERROR
            "VGI_RPC_ARROW_SHARED is ON but the Arrow package defines no "
            "Arrow::arrow_shared target; this Arrow was built static.")
    endif()
    set(_VGI_RPC_ARROW_TARGET Arrow::arrow_shared)
elseif(TARGET Arrow::arrow_static)
    set(_VGI_RPC_ARROW_TARGET Arrow::arrow_static)
elseif(TARGET Arrow::arrow_shared)
    set(_VGI_RPC_ARROW_TARGET Arrow::arrow_shared)
else()
    message(FATAL_ERROR
        "The Arrow package defines neither Arrow::arrow_static nor "
        "Arrow::arrow_shared.")
endif()
message(STATUS "Linking Arrow via ${_VGI_RPC_ARROW_TARGET}")

target_link_libraries(vgi_rpc PUBLIC
    ${_VGI_RPC_ARROW_TARGET}
    nlohmann_json::nlohmann_json
    httplib::httplib
)

# PRIVATE: libsodium is entirely behind `vgi_rpc::crypto`, so a consumer sees
# no sodium header and links no sodium symbol of its own.
target_link_libraries(vgi_rpc PRIVATE unofficial-sodium::sodium)
target_link_libraries(vgi_rpc PRIVATE zstd::libzstd)

# shm_open/shm_unlink moved from librt into libc in glibc 2.34.  Linking it
# where it exists costs nothing (it is a stub on modern glibc) and is what
# keeps the shared-memory channel building on older distributions.
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    find_library(VGI_RPC_RT_LIBRARY rt)
    if(VGI_RPC_RT_LIBRARY)
        # The bare name, not find_library's absolute path: this target is
        # exported, and an absolute path would pin the installed package to
        # the machine that built it.
        target_link_libraries(vgi_rpc PUBLIC rt)
    endif()
endif()

# PRIVATE: the SDKs are an implementation detail of src/external.cpp, and the
# compile definitions gate only that file's backends.  A consumer of the
# installed library links neither.
if(VGI_RPC_WITH_S3)
    target_compile_definitions(vgi_rpc PRIVATE VGI_RPC_WITH_S3)
    target_link_libraries(vgi_rpc PRIVATE ${AWSSDK_LINK_LIBRARIES})
endif()
if(VGI_RPC_WITH_GCS)
    target_compile_definitions(vgi_rpc PRIVATE VGI_RPC_WITH_GCS)
    target_link_libraries(vgi_rpc PRIVATE google-cloud-cpp::storage)
endif()

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
    find_package(Python3 COMPONENTS Interpreter QUIET)
    add_subdirectory(tests)
    add_subdirectory(conformance)

    if(Python3_FOUND)
        add_test(
            NAME conformance
            COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/test_conformance.py
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        )
        set_tests_properties(conformance PROPERTIES
            TIMEOUT 60
            # 77 means the interpreter CMake found cannot import the
            # Python reference; that is a missing dependency, not a
            # failing worker.  scripts/run_conformance.sh is the
            # authoritative suite either way.
            SKIP_RETURN_CODE 77
            ENVIRONMENT "CONFORMANCE_WORKER=$<TARGET_FILE:conformance_worker>"
        )
    endif()
endif()
