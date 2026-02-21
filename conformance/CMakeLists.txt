add_executable(conformance_worker conformance_worker.cpp)
target_link_libraries(conformance_worker PRIVATE vgi_rpc)
