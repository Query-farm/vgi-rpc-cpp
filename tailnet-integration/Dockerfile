# syntax=docker/dockerfile:1
FROM ubuntu:24.04 AS build

ARG VCPKG_COMMIT=05442024c3fda64320bd25d2251cc9807b84fb6f
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       autoconf automake bison build-essential ca-certificates cmake curl flex git libtool \
       ninja-build pkg-config tar unzip zip \
    && rm -rf /var/lib/apt/lists/*
RUN git clone https://github.com/microsoft/vcpkg.git /opt/vcpkg \
    && git -C /opt/vcpkg checkout "$VCPKG_COMMIT" \
    && /opt/vcpkg/bootstrap-vcpkg.sh -disableMetrics

WORKDIR /src
COPY . .
RUN cmake -S . -B build-tailnet -G Ninja \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_TOOLCHAIN_FILE=/opt/vcpkg/scripts/buildsystems/vcpkg.cmake \
      -DVCPKG_OVERLAY_PORTS=/src/vcpkg-overlays \
      -DVGI_RPC_BUILD_TESTS=OFF \
      -DVGI_RPC_BUILD_TAILNET_ADAPTER=ON \
    && cmake --build build-tailnet --target vgi-rpc-tailnet-cpp --parallel

FROM ubuntu:24.04
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/build-tailnet/tailnet-integration/vgi-rpc-tailnet-cpp \
    /usr/local/bin/vgi-rpc-tailnet-cpp
ENTRYPOINT ["vgi-rpc-tailnet-cpp"]
