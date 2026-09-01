// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include <catch2/catch_test_macros.hpp>

#ifndef _WIN32

#include "../src/socket_transport_internal.h"

#include <array>
#include <atomic>
#include <chrono>
#include <future>
#include <mutex>
#include <optional>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace std::chrono_literals;
using namespace vgi_rpc::socket_detail;

namespace {

std::array<int, 2> socket_pair() {
    std::array<int, 2> pair{-1, -1};
    REQUIRE(::socketpair(AF_UNIX, SOCK_STREAM, 0, pair.data()) == 0);
    return pair;
}

std::vector<uint8_t> iroh_preamble() {
    std::vector<uint8_t> value = {0x0d,
                                  0x0a,
                                  0x0d,
                                  0x0a,
                                  0x00,
                                  0x0d,
                                  0x0a,
                                  0x51,
                                  0x55,
                                  0x49,
                                  0x54,
                                  0x0a,
                                  0x21,
                                  0x00,
                                  0x00,
                                  0x24,
                                  vgi_rpc::VGI_IROH_ENDPOINT_TLV,
                                  0x00,
                                  0x21,
                                  0x01};
    for (uint8_t index = 0; index < 32; ++index) value.push_back(index);
    return value;
}

}  // namespace

TEST_CASE("socket first-frame deadline is absolute across a slow byte stream",
          "[socket][admission]") {
    auto pair = socket_pair();
    DeadlineFdInputStream input(pair[0], std::chrono::steady_clock::now() + 80ms, 1s);
    std::thread writer([fd = pair[1]] {
        for (uint8_t value = 0; value < 4; ++value) {
            std::this_thread::sleep_for(30ms);
            (void)::send(fd, &value, 1, 0);
        }
    });

    std::array<uint8_t, 4> bytes{};
    const auto started = std::chrono::steady_clock::now();
    const auto result = input.Read(bytes.size(), bytes.data());
    const auto elapsed = std::chrono::steady_clock::now() - started;
    REQUIRE_FALSE(result.ok());
    REQUIRE(elapsed >= 50ms);
    REQUIRE(elapsed < 300ms);

    writer.join();
    ::close(pair[0]);
    ::close(pair[1]);
}

TEST_CASE("socket idle read deadline starts after the first frame", "[socket][admission]") {
    auto pair = socket_pair();
    DeadlineFdInputStream input(pair[0], std::chrono::steady_clock::now() + 1s, 50ms);
    input.mark_first_frame_complete();

    uint8_t byte = 0;
    const auto started = std::chrono::steady_clock::now();
    const auto result = input.Read(1, &byte);
    const auto elapsed = std::chrono::steady_clock::now() - started;
    REQUIRE_FALSE(result.ok());
    REQUIRE(elapsed >= 30ms);
    REQUIRE(elapsed < 250ms);

    ::close(pair[0]);
    ::close(pair[1]);
}

TEST_CASE("socket write deadline releases a worker when the peer stops reading",
          "[socket][admission]") {
    auto pair = socket_pair();
    const int small_buffer = 4096;
    REQUIRE(::setsockopt(pair[0], SOL_SOCKET, SO_SNDBUF, &small_buffer, sizeof(small_buffer)) == 0);
    DeadlineFdOutputStream output(pair[0], 60ms);
    const int flags = ::fcntl(pair[0], F_GETFL);
    REQUIRE(flags >= 0);
    REQUIRE((flags & O_NONBLOCK) != 0);
    std::vector<uint8_t> payload(8 * 1024 * 1024, 0x5a);

    const auto started = std::chrono::steady_clock::now();
    const auto result = output.Write(payload.data(), payload.size());
    const auto elapsed = std::chrono::steady_clock::now() - started;
    REQUIRE_FALSE(result.ok());
    REQUIRE(elapsed >= 40ms);
    REQUIRE(elapsed < 300ms);

    ::close(pair[0]);
    ::close(pair[1]);
}

TEST_CASE("socket admission rejects without blocking and releases permits", "[socket][admission]") {
    std::atomic<int> calls{0};
    std::promise<void> first_entered_promise;
    auto first_entered = first_entered_promise.get_future();
    std::promise<void> release_first_promise;
    auto release_first = release_first_promise.get_future().share();
    std::mutex failures_mutex;
    std::vector<ConnectionFailure> failures;

    SocketConnectionPool pool(
        1, 1,
        [&](int, const AcceptedPeer&) {
            const int call = ++calls;
            if (call == 1) {
                first_entered_promise.set_value();
                release_first.wait();
                throw std::runtime_error("credential=must-never-reach-the-log");
            }
        },
        [&](ConnectionFailure failure) {
            std::lock_guard lock(failures_mutex);
            failures.push_back(failure);
        });

    auto first = socket_pair();
    auto second = socket_pair();
    auto saturated = socket_pair();
    REQUIRE(pool.submit(first[0], {}));
    REQUIRE(first_entered.wait_for(1s) == std::future_status::ready);
    REQUIRE(pool.submit(second[0], {}));

    const auto started = std::chrono::steady_clock::now();
    REQUIRE_FALSE(pool.submit(saturated[0], {}));
    REQUIRE(std::chrono::steady_clock::now() - started < 100ms);
    ::close(saturated[0]);
    ::close(saturated[1]);

    release_first_promise.set_value();
    const auto release_deadline = std::chrono::steady_clock::now() + 1s;
    while (calls.load() < 2 && std::chrono::steady_clock::now() < release_deadline)
        std::this_thread::sleep_for(1ms);
    REQUIRE(calls.load() >= 2);

    auto after_release = socket_pair();
    bool admitted = false;
    while (!admitted && std::chrono::steady_clock::now() < release_deadline) {
        admitted = pool.submit(after_release[0], {});
        if (!admitted) std::this_thread::sleep_for(1ms);
    }
    REQUIRE(admitted);
    while (calls.load() < 3 && std::chrono::steady_clock::now() < release_deadline)
        std::this_thread::sleep_for(1ms);
    REQUIRE(calls.load() == 3);

    pool.stop();
    ::close(first[1]);
    ::close(second[1]);
    ::close(after_release[1]);
    std::lock_guard lock(failures_mutex);
    REQUIRE(failures == std::vector{ConnectionFailure::REJECTED});
}

TEST_CASE("trusted PROXY v2 Iroh identity is promoted before connection policy",
          "[socket][admission][iroh]") {
    auto pair = socket_pair();
    auto preamble = iroh_preamble();
    preamble.insert(preamble.end(), {'V', 'G', 'I'});
    REQUIRE(::send(pair[1], preamble.data(), preamble.size(), 0) ==
            static_cast<ssize_t>(preamble.size()));

    vgi_rpc::TcpServerOptions options;
    options.proxy_protocol_v2_required = true;
    options.trusted_proxy_addresses = {"127.0.0.1"};
    options.iroh_proxy_issuer = "production-mesh";
    options.peer_authentication_policy = vgi_rpc::peer_identity_primary("iroh");
    std::optional<vgi_rpc::PeerResolutionContext> observed;
    options.resolve_identity = [&](const vgi_rpc::PeerResolutionContext& context) {
        observed = context;
        return vgi_rpc::TcpServerOptions::ResolvedIdentity{};
    };
    AcceptedPeer peer{"127.0.0.1", "127.0.0.1:43123", std::chrono::steady_clock::now()};
    const auto resolved = resolve_tcp_identity(
        pair[0], peer, options, std::chrono::steady_clock::now() + std::chrono::seconds(1));

    constexpr const char* endpoint =
        "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    REQUIRE(observed);
    REQUIRE_FALSE(observed->asserted_peer);
    REQUIRE(observed->metadata["iroh_endpoint_id"] == endpoint);
    const auto& identity = resolved.evidence.unique_verified_subject("iroh");
    REQUIRE(identity.subject_key() == std::optional<std::string>(endpoint));
    REQUIRE(identity.issuer() == "production-mesh");
    REQUIRE(identity.assurance() == vgi_rpc::IdentityAssurance::CONFIGURED_PROXY);
    REQUIRE(identity.attributes()["original_assurance"] == "cryptographic_peer");
    REQUIRE(identity.proxy_address() == std::optional<std::string>("127.0.0.1:43123"));
    REQUIRE(resolved.auth.authenticated);

    std::array<uint8_t, 3> following{};
    REQUIRE(::recv(pair[0], following.data(), following.size(), 0) == 3);
    REQUIRE(following == std::array<uint8_t, 3>{'V', 'G', 'I'});
    ::close(pair[0]);
    ::close(pair[1]);
}

TEST_CASE("Iroh PROXY v2 rejects an untrusted immediate peer before reading a preamble",
          "[socket][admission][iroh]") {
    auto pair = socket_pair();
    vgi_rpc::TcpServerOptions options;
    options.proxy_protocol_v2_required = true;
    options.trusted_proxy_addresses = {"127.0.0.1"};
    options.iroh_proxy_issuer = "production-mesh";
    AcceptedPeer peer{"192.0.2.1", "192.0.2.1:43123", std::chrono::steady_clock::now()};

    try {
        (void)resolve_tcp_identity(pair[0], peer, options,
                                   std::chrono::steady_clock::now() + std::chrono::seconds(1));
        FAIL("untrusted immediate peer was accepted");
    } catch (const vgi_rpc::ProxyProtocolV2Error& error) {
        REQUIRE(std::string(error.what()) == "immediate peer is not a trusted PROXY v2 sender");
    }
    ::close(pair[0]);
    ::close(pair[1]);
}

#else

TEST_CASE("socket admission is unavailable on Windows", "[socket][admission]") {
    SUCCEED();
}

#endif
