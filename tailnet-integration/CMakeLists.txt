# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

add_executable(vgi-rpc-tailnet-cpp main.cpp)
target_link_libraries(vgi-rpc-tailnet-cpp PRIVATE vgi_rpc)

if(VGI_RPC_BUILD_TESTS)
    add_test(NAME tailnet_adapter_self_test COMMAND vgi-rpc-tailnet-cpp --self-test)
    set_tests_properties(tailnet_adapter_self_test PROPERTIES TIMEOUT 10)
endif()
