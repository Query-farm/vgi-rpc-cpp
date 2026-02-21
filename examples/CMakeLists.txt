add_executable(hello_world hello_world.cpp)
target_link_libraries(hello_world PRIVATE vgi_rpc)

add_executable(calculator calculator.cpp)
target_link_libraries(calculator PRIVATE vgi_rpc)

add_executable(streaming streaming.cpp)
target_link_libraries(streaming PRIVATE vgi_rpc)
