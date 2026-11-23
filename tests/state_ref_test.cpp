// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Protocol 3.4 large values (STATE_REF_V1.md): the language-local half.  The
// shared conformance suite drives argument references and the object cache
// over the wire; these pin what it cannot observe: the codec tags, the
// argument-reference AAD's peer-evidence binding, the cache's single flight,
// byte bound and age rule, and the in-memory StateRef's memo and misuse rule.

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include "vgi_rpc/call_context.h"
#include "vgi_rpc/crypto.h"
#include "vgi_rpc/object_cache.h"
#include "vgi_rpc/state_ref.h"

#include <atomic>
#include <chrono>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace vgi_rpc;
using Catch::Matchers::ContainsSubstring;

namespace {

std::string sha256_hex(const std::string& data) {
    const auto digest = crypto::sha256(reinterpret_cast<const uint8_t*>(data.data()), data.size());
    return crypto::hex_encode(digest.data(), digest.size());
}

CallContext make_ctx() {
    CallContext ctx(std::make_shared<LogSink>("server", "request"), "server", "request");
    ctx.set_call_identity("Test.v1", "stream");
    return ctx;
}

AuthContext principal(const std::string& name) {
    AuthContext auth;
    auth.authenticated = true;
    auth.domain = "conformance";
    auth.principal = name;
    return auth;
}

}  // namespace

TEST_CASE("token codec: zstd when smaller, raw otherwise, unknown tags refused", "[state-ref]") {
    const std::string large(8192, 'a');
    const std::string encoded = token_codec::encode(large);
    CHECK(static_cast<uint8_t>(encoded[0]) == token_codec::ZSTD);
    CHECK(encoded.size() < large.size() / 4);
    CHECK(token_codec::decode(encoded) == large);

    const std::string tiny = "abc";
    const std::string raw = token_codec::encode(tiny);
    CHECK(static_cast<uint8_t>(raw[0]) == token_codec::RAW);
    CHECK(raw.substr(1) == tiny);
    CHECK(token_codec::decode(raw) == tiny);

    CHECK_THROWS_AS(token_codec::decode(std::string("\x7f", 1) + "x"), std::invalid_argument);
    CHECK_THROWS_AS(token_codec::decode(std::string("\x02", 1) + "x"), std::invalid_argument);
    CHECK_THROWS_AS(token_codec::decode(""), std::invalid_argument);
    // The declared frame size is checked against the bound before inflating.
    CHECK_THROWS_AS(token_codec::decode(encoded, 100), std::invalid_argument);
}

TEST_CASE("StateRef: in memory, memoized within the init, refused elsewhere", "[state-ref]") {
    CallContext ctx = make_ctx();
    CHECK_THROWS_AS(ctx.state_ref(std::string("outside an init")), StateRefMisuseError);
    try {
        (void)ctx.state_ref(std::string("x"));
    } catch (const StateRefMisuseError& e) {
        CHECK(e.kind() == "state_ref_misuse");
        CHECK(e.code() == Code::FAILED_PRECONDITION);
        CHECK_THAT(e.what(),
                   ContainsSubstring("state_ref() may only be called while handling a stream's "
                                     "init request (Test.v1.stream)"));
    }

    StateRef first, second, other;
    {
        CallContext::StreamInitScope scope(ctx);
        first = ctx.state_ref(std::string(40000, 'q'));
        second = ctx.state_ref(std::string(40000, 'q'));
        other = ctx.state_ref(std::string("different"));
    }
    CHECK(std::string(first.form()) == "memory");
    // Byte-equal values share one ref (one copy of the value).
    CHECK(&ctx.resolve(first) == &ctx.resolve(second));
    CHECK(&ctx.resolve(first) != &ctx.resolve(other));
    CHECK(ctx.resolve(other) == "different");
    CHECK_THROWS_AS(ctx.state_ref(std::string("after the init")), StateRefMisuseError);

    // The inline encoding round-trips; a stored form is never valid here.
    const std::string encoded = first.encode();
    CHECK(static_cast<uint8_t>(encoded[0]) == 0x01);
    CHECK(static_cast<uint8_t>(encoded[1]) == token_codec::ZSTD);
    const StateRef decoded = StateRef::decode(encoded);
    CHECK(std::string(decoded.form()) == "inline");
    CHECK(ctx.resolve(decoded) == std::string(40000, 'q'));
    CHECK_THROWS_AS(StateRef::decode(std::string("\x02\x01", 2)), InvalidStateRefError);
    CHECK_THROWS_AS(StateRef::decode(std::string("\x07", 1)), InvalidStateRefError);
    CHECK_THROWS_AS(ctx.resolve(StateRef()), InvalidStateRefError);
}

TEST_CASE("argument refs open for the granting principal and binding only", "[state-ref]") {
    const auto key = crypto::random_key();
    const std::string sha = sha256_hex("payload");
    const std::string ref = mint_argument_ref(key, principal("alice"), 7, sha, "obj-1");
    CHECK(static_cast<uint8_t>(ref[0]) == 0x01);

    const auto claims = open_argument_ref(key, principal("alice"), ref);
    CHECK(claims.size == 7);
    CHECK(claims.sha256_hex == sha);
    CHECK(claims.object_id == "obj-1");

    CHECK_THROWS_AS(open_argument_ref(key, principal("bob"), ref), InvalidArgumentsRefError);
    CHECK_THROWS_AS(open_argument_ref(key, AuthContext::anonymous(), ref),
                    InvalidArgumentsRefError);
    CHECK_THROWS_AS(open_argument_ref(crypto::random_key(), principal("alice"), ref),
                    InvalidArgumentsRefError);
    std::string tampered = ref;
    tampered.back() = static_cast<char>(tampered.back() ^ 1);
    CHECK_THROWS_AS(open_argument_ref(key, principal("alice"), tampered), InvalidArgumentsRefError);

    // Peer-evidence binding (decision 23): the binding digest is in the AAD,
    // so a ref does not cross bindings even for the same principal.
    AuthContext bound = principal("alice");
    bound.claims["peer_evidence_binding"] = "digest-a";
    const std::string bound_ref = mint_argument_ref(key, bound, 7, sha, "obj-1");
    CHECK_NOTHROW(open_argument_ref(key, bound, bound_ref));
    AuthContext rebound = principal("alice");
    rebound.claims["peer_evidence_binding"] = "digest-b";
    CHECK_THROWS_AS(open_argument_ref(key, rebound, bound_ref), InvalidArgumentsRefError);
    CHECK_THROWS_AS(open_argument_ref(key, principal("alice"), bound_ref),
                    InvalidArgumentsRefError);
    CHECK(base_aad(bound, "P").rfind(std::string("vgi_rpc.state.v7\0", 17), 0) == 0);
    CHECK(base_aad(principal("alice"), "P").rfind(std::string("vgi_rpc.state.v6\0", 17), 0) == 0);

    CHECK_THROWS_AS(mint_argument_ref(key, principal("a"), 0, sha, "x"), std::invalid_argument);
    CHECK_THROWS_AS(mint_argument_ref(key, principal("a"), 1, "ABC", "x"), std::invalid_argument);
    CHECK_THROWS_AS(mint_argument_ref(key, principal("a"), 1, sha, ""), std::invalid_argument);
}

TEST_CASE("object cache: single flight, byte bound, no retention over the bound, age",
          "[state-ref][object-cache]") {
    double clock = 1000.0;
    ObjectCache cache(100, OBJECT_CACHE_MAX_AGE_SECONDS, [&] { return clock; });

    SECTION("concurrent lookups share one fetch") {
        std::atomic<int> fetches{0};
        std::vector<std::thread> threads;
        for (int i = 0; i < 8; ++i) {
            threads.emplace_back([&] {
                auto [value, hit] = cache.get_or_fetch("id", [&] {
                    ++fetches;
                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
                    return std::string(10, 'v');
                });
                CHECK(*value == std::string(10, 'v'));
            });
        }
        for (auto& thread : threads) thread.join();
        CHECK(fetches == 1);
        CHECK(cache.stats().fetches == 1);
    }

    SECTION("the bound evicts least recently used, and resident bytes stay under it") {
        (void)cache.get_or_fetch("a", [] { return std::string(60, 'a'); });
        (void)cache.get_or_fetch("b", [] { return std::string(60, 'b'); });
        auto stats = cache.stats();
        CHECK(stats.resident_bytes == 60);
        CHECK(stats.entries == 1);
        CHECK(stats.evictions == 1);
        CHECK_FALSE(cache.contains("a"));
        CHECK(cache.contains("b"));
    }

    SECTION("a value over the bound is returned but not retained") {
        auto [value, hit] = cache.get_or_fetch("big", [] { return std::string(500, 'x'); });
        CHECK(value->size() == 500);
        CHECK_FALSE(hit);
        CHECK(cache.stats().resident_bytes == 0);
    }

    SECTION("an entry fetched over a day ago is a miss") {
        int fetches = 0;
        auto fetch = [&] {
            ++fetches;
            return std::string("v");
        };
        (void)cache.get_or_fetch("id", fetch);
        CHECK(cache.get_or_fetch("id", fetch).second);
        clock += OBJECT_CACHE_MAX_AGE_SECONDS + 1;
        CHECK_FALSE(cache.get_or_fetch("id", fetch).second);
        CHECK(fetches == 2);
        CHECK(cache.age_entries(OBJECT_CACHE_MAX_AGE_SECONDS + 1) == 1);
        CHECK_FALSE(cache.get_or_fetch("id", fetch).second);
        CHECK(fetches == 3);
    }

    SECTION("a failed fetch is shared, not cached") {
        CHECK_THROWS_AS(cache.get_or_fetch("bad", []() -> std::string {
            throw std::runtime_error("storage down");
        }),
                        std::runtime_error);
        CHECK(cache.get_or_fetch("bad", [] { return std::string("ok"); }).first->size() == 2);
    }
}
