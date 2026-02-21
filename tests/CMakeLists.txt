find_package(Catch2 3 CONFIG REQUIRED)

include(CTest)
include(Catch)

function(vgi_rpc_add_test name)
    add_executable(${name} ${name}.cpp)
    target_link_libraries(${name} PRIVATE vgi_rpc Catch2::Catch2WithMain)
    catch_discover_tests(${name} TEST_PREFIX "${name}:" PROPERTIES TIMEOUT 60)
endfunction()

vgi_rpc_add_test(metadata_test)
vgi_rpc_add_test(wire_test)
vgi_rpc_add_test(request_test)
vgi_rpc_add_test(result_test)
vgi_rpc_add_test(output_collector_test)
vgi_rpc_add_test(server_test)
