# Getting Started

## Prerequisites

- C++20 compiler (GCC 12+, Clang 15+, Apple Clang 15+)
- CMake 3.24+
- [vcpkg](https://vcpkg.io/) for dependency management

## Building

Clone the repository and build with CMake presets:

```bash
git clone https://github.com/Query-farm/vgi-rpc-cpp.git
cd vgi-rpc-cpp
```

### Debug build with tests

```bash
cmake --preset default
cmake --build build
ctest --test-dir build
```

### Release build

```bash
cmake --preset release
cmake --build build
```

### Sanitizer build (ASAN + UBSAN)

```bash
cmake --preset sanitizer
cmake --build build
```

## Dependencies

Managed automatically via vcpkg:

| Dependency | Purpose |
|---|---|
| Apache Arrow | Columnar IPC format and record batch handling |
| nlohmann-json | JSON parsing for log metadata |
| Catch2 | Unit testing (test builds only) |

## Installing

After building, install the library:

```bash
cmake --install build --prefix /usr/local
```

The install provides:

- Headers in `include/vgi_rpc/`
- Shared library
- CMake package config (`find_package(vgi_rpc)`)
- pkg-config file

## Using in Your Project

### CMake

```cmake
find_package(vgi_rpc REQUIRED)
target_link_libraries(my_server PRIVATE vgi_rpc::vgi_rpc)
```

### pkg-config

```bash
g++ -std=c++20 my_server.cpp $(pkg-config --cflags --libs vgi_rpc)
```

## Your First Server

Create a minimal server with a single unary method:

```cpp title="examples/getting_started.cpp"
--8<-- "examples/getting_started.cpp"
```

The server reads Arrow IPC requests from stdin and writes responses to stdout. Use `enable_describe()` to register the built-in `__describe__` introspection method, which lets clients discover available methods and their schemas.
