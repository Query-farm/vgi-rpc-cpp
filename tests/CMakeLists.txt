# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

find_package(Catch2 3 CONFIG REQUIRED)

include(CTest)
include(Catch)

function(vgi_rpc_add_test name)
    add_executable(${name} ${name}.cpp)
    target_link_libraries(${name} PRIVATE vgi_rpc Catch2::Catch2WithMain)
    catch_discover_tests(${name} TEST_PREFIX "${name}:" PROPERTIES TIMEOUT 60)
endfunction()

vgi_rpc_add_test(crypto_test)
vgi_rpc_add_test(identity_test)
vgi_rpc_add_test(iroh_identity_test)
vgi_rpc_add_test(identity_providers_test)
target_link_libraries(identity_providers_test PRIVATE OpenSSL::Crypto)
target_compile_definitions(identity_providers_test PRIVATE
    VGI_RPC_IDENTITY_VECTORS="${CMAKE_CURRENT_SOURCE_DIR}/data/transport_identity_vectors.json"
)
vgi_rpc_add_test(proxy_protocol_v2_test)
vgi_rpc_add_test(socket_admission_test)
vgi_rpc_add_test(metadata_test)
vgi_rpc_add_test(wire_test)
vgi_rpc_add_test(request_test)
vgi_rpc_add_test(result_test)
vgi_rpc_add_test(output_collector_test)
vgi_rpc_add_test(server_test)
vgi_rpc_add_test(shm_test)
vgi_rpc_add_test(client_description_test)
vgi_rpc_add_test(client_external_test)
vgi_rpc_add_test(client_test)

add_executable(client_integration client_integration.cpp)
target_link_libraries(client_integration PRIVATE vgi_rpc)
add_test(NAME client_integration COMMAND client_integration)
set_tests_properties(client_integration PROPERTIES
    TIMEOUT 90
    SKIP_RETURN_CODE 77
)
if(Python3_FOUND)
    set_tests_properties(client_integration PROPERTIES
        ENVIRONMENT "VGI_RPC_PYTHON=${Python3_EXECUTABLE}"
    )
endif()

add_executable(http_client_integration http_client_integration.cpp)
target_link_libraries(http_client_integration PRIVATE vgi_rpc)
add_test(NAME http_client_integration COMMAND http_client_integration)
set_tests_properties(http_client_integration PROPERTIES
    TIMEOUT 60
    SKIP_RETURN_CODE 77
)
if(Python3_FOUND)
    set_tests_properties(http_client_integration PROPERTIES
        ENVIRONMENT "VGI_RPC_PYTHON=${Python3_EXECUTABLE}"
    )
endif()

add_executable(http_client_sticky_integration http_client_sticky_integration.cpp)
target_link_libraries(http_client_sticky_integration PRIVATE vgi_rpc)
add_test(NAME http_client_sticky_integration COMMAND http_client_sticky_integration)
set_tests_properties(http_client_sticky_integration PROPERTIES
    TIMEOUT 60
    SKIP_RETURN_CODE 77
)
if(Python3_FOUND)
    set_tests_properties(http_client_sticky_integration PROPERTIES
        ENVIRONMENT "VGI_RPC_PYTHON=${Python3_EXECUTABLE}"
    )
endif()

add_executable(http_client_fault_test http_client_fault_test.cpp)
target_link_libraries(http_client_fault_test PRIVATE vgi_rpc zstd::libzstd)
add_test(NAME http_client_fault_test COMMAND http_client_fault_test)
set_tests_properties(http_client_fault_test PROPERTIES TIMEOUT 60)

add_executable(http_client_resilience_test http_client_resilience_test.cpp)
target_link_libraries(http_client_resilience_test PRIVATE vgi_rpc)
add_test(NAME http_client_resilience_test COMMAND http_client_resilience_test)
set_tests_properties(http_client_resilience_test PROPERTIES TIMEOUT 60)

if(VGI_RPC_BUILD_BENCHMARKS)
    add_executable(vgi_rpc_benchmark benchmark.cpp)
    target_link_libraries(vgi_rpc_benchmark PRIVATE vgi_rpc Catch2::Catch2WithMain)
endif()
