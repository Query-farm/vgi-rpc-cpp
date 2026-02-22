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

```cpp
#include <vgi_rpc/server.h>
#include <vgi_rpc/request.h>
#include <vgi_rpc/result.h>
#include <vgi_rpc/arrow_utils.h>

#include <arrow/array/builder_primitive.h>
#include <arrow/type.h>

int main() {
    auto params = arrow::schema({
        arrow::field("a", arrow::float64()),
        arrow::field("b", arrow::float64()),
    });
    auto result = arrow::schema({
        arrow::field("result", arrow::float64()),
    });

    auto server = vgi_rpc::ServerBuilder()
        .add_unary("add", params, result,
            [](const vgi_rpc::Request& req, vgi_rpc::CallContext& ctx) {
                double a = req.get<double>("a");
                double b = req.get<double>("b");

                arrow::DoubleBuilder builder;
                VGI_RPC_THROW_NOT_OK(builder.Append(a + b));
                auto array = vgi_rpc::unwrap(builder.Finish());

                return vgi_rpc::Result::value(
                    arrow::schema({arrow::field("result", arrow::float64())}),
                    {array});
            },
            "Add two numbers.")
        .enable_describe("MyServer")
        .build();

    server->run();
    return 0;
}
```

The server reads Arrow IPC requests from stdin and writes responses to stdout. Use `enable_describe()` to register the built-in `__describe__` introspection method, which lets clients discover available methods and their schemas.
