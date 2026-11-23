// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// The blob-vendor client (vgi_rpc/vendor.h): the host pin, the constructor's
// refusals and the environment wiring run everywhere; the round trips run
// against vgi-rpc-python's fake vendor when VGI_TEST_FAKE_VENDOR_URL names one
// (`uvx --from git+https://github.com/Query-farm/vgi-rpc-python@<ref>
// vgi-rpc-fake-vendor --test-routes --token test-token=tenant-a`; the token is
// VGI_TEST_FAKE_VENDOR_TOKEN, default test-token).

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include "vgi_rpc/crypto.h"
#include "vgi_rpc/external.h"
#include "vgi_rpc/http_config.h"
#include "vgi_rpc/vendor.h"

#include <httplib.h>

#include <cstdlib>
#include <map>
#include <optional>
#include <string>

using namespace vgi_rpc;
using Catch::Matchers::ContainsSubstring;

namespace {

using Env = std::map<std::string, std::optional<std::string>>;

// Sets (or unsets) environment variables for one scope, restoring them after.
class ScopedEnv {
public:
    explicit ScopedEnv(const Env& values) {
        for (const auto& [name, value] : values) {
            const char* old = std::getenv(name.c_str());
            saved_[name] = old ? std::optional<std::string>(old) : std::nullopt;
            set(name, value);
        }
    }
    ~ScopedEnv() {
        for (const auto& [name, value] : saved_) set(name, value);
    }

private:
    static void set(const std::string& name, const std::optional<std::string>& value) {
        if (value) {
            ::setenv(name.c_str(), value->c_str(), 1);
        } else {
            ::unsetenv(name.c_str());
        }
    }
    std::map<std::string, std::optional<std::string>> saved_;
};

std::string sha256_hex(const std::string& data) {
    const auto digest = crypto::sha256(reinterpret_cast<const uint8_t*>(data.data()), data.size());
    return crypto::hex_encode(digest.data(), digest.size());
}

std::optional<std::string> fake_vendor() {
    const char* url = std::getenv("VGI_TEST_FAKE_VENDOR_URL");
    if (!url || !*url) return std::nullopt;
    return std::string(url);
}

std::string fake_token() {
    const char* token = std::getenv("VGI_TEST_FAKE_VENDOR_TOKEN");
    return token && *token ? token : "test-token";
}

const std::map<std::string, std::optional<std::string>> kClearVendorEnv = {
    {"VGI_BLOB_VENDOR_URL", std::nullopt},
    {"VGI_BLOB_VENDOR_TOKEN", std::nullopt},
    {"VGI_CF_DO_TOKEN", std::nullopt},
    {"VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES", std::nullopt},
    {"VGI_RPC_EXTERNAL_COMPRESSION", std::nullopt},
    {"VGI_RPC_MAX_UPLOAD_BYTES", std::nullopt},
    {"VGI_RPC_MAX_FETCH_BYTES", std::nullopt},
    {"VGI_RPC_MAX_REQUEST_BYTES", std::nullopt},
};

}  // namespace

TEST_CASE("the host pin accepts only its hosts, schemes and prefixes", "[vendor]") {
    const auto pin = pinned_hosts_validator({"blobs.example.com", "127.0.0.1:9000"}, false);
    CHECK_NOTHROW(pin("https://blobs.example.com/a/b?X-Amz-Signature=x"));
    CHECK_NOTHROW(pin("https://BLOBS.example.com:443/a"));
    CHECK_THROWS(pin("http://blobs.example.com/a"));        // scheme
    CHECK_THROWS(pin("https://blobs.example.com:8443/a"));  // port
    CHECK_THROWS(pin("https://evil.blobs.example.com/a"));  // subdomain
    CHECK_THROWS(pin("https://user@blobs.example.com/a"));  // userinfo
    CHECK_THROWS(pin("https://127.0.0.1/a"));               // wrong port
    CHECK_NOTHROW(pin("https://127.0.0.1:9000/a"));
    CHECK_THROWS(pin("not a url"));

    const auto loopback = pinned_hosts_validator({"127.0.0.1"}, true, {"http://127.0.0.1/r/"});
    CHECK_NOTHROW(loopback("http://127.0.0.1/r/obj"));
    CHECK_THROWS(loopback("http://127.0.0.1/other/obj"));       // prefix
    CHECK_THROWS(loopback("http://127.0.0.1/r/../other/obj"));  // dot-dot
    CHECK_THROWS(pinned_hosts_validator({}, false));
    CHECK_THROWS(pinned_hosts_validator({"https://host/path"}, false));

    // A refusal never echoes a signed query string.
    try {
        pin("https://other.example.com/a?X-Amz-Signature=secret");
        FAIL("expected a refusal");
    } catch (const std::invalid_argument& error) {
        CHECK_THAT(error.what(), !ContainsSubstring("secret"));
    }
}

TEST_CASE("a vendor needs https (http on loopback only) and a token", "[vendor]") {
    CHECK_THROWS_WITH(VendorStorage("http://blobs.example.com", "t"), ContainsSubstring("https"));
    CHECK_THROWS_WITH(VendorStorage("ftp://127.0.0.1", "t"), ContainsSubstring("https"));
    CHECK_THROWS_WITH(VendorStorage("https://blobs.example.com", " "), ContainsSubstring("token"));
    CHECK_NOTHROW(VendorStorage("http://127.0.0.1:1", "t"));
    CHECK_NOTHROW(VendorStorage("https://blobs.example.com/", "t"));
}

TEST_CASE("configure_http_from_env without a vendor sets only the request cap", "[vendor]") {
    ScopedEnv clear(kClearVendorEnv);
    {
        HttpConfig cfg;
        CHECK(configure_http_from_env(cfg) == nullptr);
        CHECK(cfg.max_request_bytes == -1);
        CHECK(cfg.external_storage == nullptr);
    }
    {
        ScopedEnv cap(Env{{"VGI_RPC_MAX_REQUEST_BYTES", "32768"}});
        HttpConfig cfg;
        CHECK(configure_http_from_env(cfg) == nullptr);
        CHECK(cfg.max_request_bytes == 32768);
    }
    {
        ScopedEnv bad(Env{{"VGI_RPC_MAX_REQUEST_BYTES", "12abc"}});
        HttpConfig cfg;
        CHECK_THROWS_WITH(configure_http_from_env(cfg), ContainsSubstring("integer"));
    }
}

TEST_CASE("configure_http_from_env with a vendor installs it and the caps", "[vendor]") {
    ScopedEnv clear(kClearVendorEnv);
    SECTION("defaults") {
        ScopedEnv env(
            Env{{"VGI_BLOB_VENDOR_URL", "http://127.0.0.1:1"}, {"VGI_BLOB_VENDOR_TOKEN", "t"}});
        HttpConfig cfg;
        auto storage = configure_http_from_env(cfg);
        REQUIRE(storage);
        CHECK(cfg.external_storage == storage);
        CHECK(dynamic_cast<ObjectStore*>(cfg.external_storage.get()) != nullptr);
        CHECK(cfg.externalize_threshold == 1048576);
        CHECK(cfg.max_fetch_bytes == 256LL << 20);
        CHECK(cfg.max_upload_bytes == 256LL << 20);
        CHECK(cfg.external_url_validator);
    }
    SECTION("the lane's thresholds") {
        ScopedEnv env(Env{{"VGI_BLOB_VENDOR_URL", "http://127.0.0.1:1"},
                          {"VGI_CF_DO_TOKEN", "t"},
                          {"VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES", "4096"},
                          {"VGI_RPC_MAX_REQUEST_BYTES", "32768"},
                          {"VGI_RPC_MAX_UPLOAD_BYTES", "268435456"},
                          {"VGI_RPC_EXTERNAL_COMPRESSION", "zstd:3"}});
        HttpConfig cfg;
        REQUIRE(configure_http_from_env(cfg));
        CHECK(cfg.externalize_threshold == 4096);
        CHECK(cfg.max_request_bytes == 32768);
        CHECK(cfg.max_upload_bytes == 268435456);
        CHECK(cfg.externalize_compression == "zstd");
    }
    SECTION("an upload cap above an explicit fetch cap") {
        ScopedEnv env(Env{{"VGI_BLOB_VENDOR_URL", "http://127.0.0.1:1"},
                          {"VGI_BLOB_VENDOR_TOKEN", "t"},
                          {"VGI_RPC_MAX_UPLOAD_BYTES", "2048"},
                          {"VGI_RPC_MAX_FETCH_BYTES", "1024"}});
        HttpConfig cfg;
        CHECK_THROWS_WITH(configure_http_from_env(cfg), ContainsSubstring("refuses to fetch"));
    }
    SECTION("a URL without a token") {
        ScopedEnv env(Env{{"VGI_BLOB_VENDOR_URL", "http://127.0.0.1:1"}});
        HttpConfig cfg;
        CHECK_THROWS_WITH(configure_http_from_env(cfg), ContainsSubstring("VGI_BLOB_VENDOR_TOKEN"));
    }
    SECTION("an unknown compression") {
        ScopedEnv env(Env{{"VGI_BLOB_VENDOR_URL", "http://127.0.0.1:1"},
                          {"VGI_BLOB_VENDOR_TOKEN", "t"},
                          {"VGI_RPC_EXTERNAL_COMPRESSION", "brotli"}});
        HttpConfig cfg;
        CHECK_THROWS_WITH(configure_http_from_env(cfg), ContainsSubstring("zstd"));
    }
}

TEST_CASE("fake vendor: objects round-trip by id and a deleted one is missing",
          "[vendor][fake-vendor]") {
    const auto url = fake_vendor();
    if (!url) SKIP("VGI_TEST_FAKE_VENDOR_URL is not set");
    VendorStorage storage(*url, fake_token());

    const std::string data(10000, 'x');
    const auto id = storage.put_object(data);
    CHECK_FALSE(id.empty());
    CHECK(storage.get(id, 1 << 20) == data);
    CHECK_THROWS(storage.get(id, 100));  // over the cap
    CHECK_THROWS_AS(storage.get("0123456789abcdef0123456789abcdef", 1 << 20), ObjectMissingError);

    // put_object grants carry origin state_ref; delete them like the lifecycle rule.
    httplib::Client admin(*url);
    auto res = admin.Get("/__admin__/delete?origin=state_ref");
    REQUIRE(res);
    CHECK(res->status == 200);
    CHECK_THROWS_AS(storage.get(id, 1 << 20), ObjectMissingError);
}

TEST_CASE("fake vendor: results upload and inbound pointers fetch through the pin",
          "[vendor][fake-vendor]") {
    const auto url = fake_vendor();
    if (!url) SKIP("VGI_TEST_FAKE_VENDOR_URL is not set");
    VendorStorage storage(*url, fake_token());

    const std::string data(5000, 'r');
    const auto download = storage.upload(data, "");
    CHECK_NOTHROW(storage.validate_url(download));
    CHECK(storage.fetch(download) == data);
    CHECK_THROWS(storage.validate_url("http://192.0.2.1/elsewhere"));
    CHECK_THROWS(storage.fetch("http://192.0.2.1/elsewhere"));
}

TEST_CASE("fake vendor: a client upload grant is signed for exactly its bytes",
          "[vendor][fake-vendor]") {
    const auto url = fake_vendor();
    if (!url) SKIP("VGI_TEST_FAKE_VENDOR_URL is not set");
    VendorStorage storage(*url, fake_token());

    const std::string body(3000, 'c');
    const auto grant =
        storage.generate_upload_url(static_cast<int64_t>(body.size()), sha256_hex(body));
    CHECK_FALSE(grant.object_id.empty());
    CHECK(grant.headers.count("content-length") == 0);
    CHECK(grant.headers.count("Content-Length") == 0);

    const auto put = [&](const std::string& payload) {
        const auto scheme_end = grant.upload_url.find("://") + 3;
        const auto path_start = grant.upload_url.find('/', scheme_end);
        httplib::Client client(grant.upload_url.substr(0, path_start));
        httplib::Headers headers;
        for (const auto& [name, value] : grant.headers) headers.emplace(name, value);
        return client.Put(grant.upload_url.substr(path_start), headers, payload,
                          "application/octet-stream");
    };
    auto wrong = put(std::string(3000, 'd'));
    REQUIRE(wrong);
    CHECK(wrong->status >= 400);  // BadDigest
    auto right = put(body);
    REQUIRE(right);
    CHECK(right->status < 300);
    CHECK(storage.fetch(grant.download_url) == body);
    CHECK(storage.get(grant.object_id, 1 << 20) == body);
}
