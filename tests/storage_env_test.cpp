// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Direct-storage contract v1: VGI_STORAGE_* parsing (no bucket is contacted).

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include "vgi_rpc/http_config.h"
#include "vgi_rpc/storage_env.h"
#include "vgi_rpc/vendor.h"

#include <cstdlib>
#include <map>
#include <optional>
#include <string>

using namespace vgi_rpc;
using Catch::Matchers::ContainsSubstring;

namespace {

const char* kNames[] = {
    "VGI_STORAGE_BACKEND",
    "VGI_BLOB_VENDOR_URL",
    "VGI_BLOB_VENDOR_TOKEN",
    "VGI_STORAGE_S3_BUCKET",
    "VGI_STORAGE_S3_PREFIX",
    "VGI_STORAGE_S3_ENDPOINT",
    "VGI_STORAGE_S3_REGION",
    "VGI_STORAGE_S3_FORCE_PATH_STYLE",
    "VGI_STORAGE_S3_PUBLIC_ENDPOINT",
    "VGI_STORAGE_S3_CHECKSUM",
    "VGI_STORAGE_S3_ACCESS_KEY_ID",
    "VGI_STORAGE_S3_SECRET_ACCESS_KEY",
    "VGI_STORAGE_S3_SESSION_TOKEN",
    "VGI_STORAGE_S3_SESSION_EXPIRES",
    "VGI_STORAGE_S3_VERIFY",
    "VGI_STORAGE_GCS_BUCKET",
    "VGI_STORAGE_GCS_PREFIX",
    "VGI_STORAGE_GCS_ENDPOINT",
    "VGI_STORAGE_GCS_VERIFY",
    "VGI_STORAGE_URL_TTL_SECONDS",
    "VGI_STORAGE_ALLOW_PLAIN_HTTP",
    "AWS_REGION",
    "AWS_DEFAULT_REGION",
};

// Clears every storage variable, sets `values`, and restores on exit.
class Env {
public:
    explicit Env(const std::map<std::string, std::string>& values) {
        for (const char* name : kNames) {
            const char* old = std::getenv(name);
            saved_[name] = old ? std::optional<std::string>(old) : std::nullopt;
            ::unsetenv(name);
        }
        for (const auto& [name, value] : values) ::setenv(name.c_str(), value.c_str(), 1);
    }
    ~Env() {
        for (const auto& [name, value] : saved_) {
            if (value) {
                ::setenv(name.c_str(), value->c_str(), 1);
            } else {
                ::unsetenv(name.c_str());
            }
        }
    }

private:
    std::map<std::string, std::optional<std::string>> saved_;
};

}  // namespace

TEST_CASE("storage backend selection (contract section 1)") {
    {
        Env env({});
        CHECK(storage_backend_from_env() == StorageBackend::None);
    }
    {
        Env env({{"VGI_STORAGE_S3_BUCKET", "b"}});
        CHECK(storage_backend_from_env() == StorageBackend::S3);
    }
    {
        Env env({{"VGI_STORAGE_GCS_BUCKET", "b"}});
        CHECK(storage_backend_from_env() == StorageBackend::Gcs);
    }
    {
        Env env({{"VGI_BLOB_VENDOR_URL", "https://v.example"}, {"VGI_STORAGE_S3_BUCKET", "b"}});
        CHECK_THROWS_WITH(storage_backend_from_env(),
                          ContainsSubstring("VGI_BLOB_VENDOR_URL, VGI_STORAGE_S3_BUCKET"));
    }
    {
        Env env({{"VGI_BLOB_VENDOR_URL", "https://v.example"},
                 {"VGI_STORAGE_S3_BUCKET", "b"},
                 {"VGI_STORAGE_BACKEND", "S3"}});
        CHECK(storage_backend_from_env() == StorageBackend::S3);
    }
    {
        Env env({{"VGI_STORAGE_BACKEND", "azure"}});
        CHECK_THROWS_WITH(storage_backend_from_env(), ContainsSubstring("vendor, s3, gcs or none"));
    }
}

TEST_CASE("S3 settings, defaults and keys under <prefix>rpc/") {
    Env env({{"VGI_STORAGE_S3_BUCKET", "bucket"},
             {"VGI_STORAGE_S3_PREFIX", "vgi/"},
             {"AWS_DEFAULT_REGION", "eu-west-1"}});
    auto settings = direct_storage_settings_from_env();
    REQUIRE(settings);
    CHECK(settings->config.uri == "s3://bucket/vgi/rpc");
    CHECK(settings->config.region == "eu-west-1");
    CHECK(settings->config.s3_addressing_style == "virtual");
    CHECK(settings->config.s3_checksum == "sha256");
    CHECK(settings->upload_grants);
    CHECK(settings->config.signed_url_ttl_seconds == 3600);
    CHECK(settings->warnings.empty());
    const auto validate = direct_storage_url_validator(*settings);
    CHECK_NOTHROW(validate("https://bucket.s3.eu-west-1.amazonaws.com/vgi/rpc/x?X-Amz=1"));
    CHECK_THROWS(validate("https://evil.example/vgi/rpc/x"));
    CHECK_THROWS(validate("http://bucket.s3.eu-west-1.amazonaws.com/x"));
}

TEST_CASE("S3 custom endpoint: path style, plain http opt-in, public endpoint") {
    {
        Env env({{"VGI_STORAGE_S3_BUCKET", "b"}, {"VGI_STORAGE_S3_ENDPOINT", "http://minio:9000"}});
        CHECK_THROWS_WITH(direct_storage_settings_from_env(),
                          ContainsSubstring("VGI_STORAGE_ALLOW_PLAIN_HTTP"));
    }
    Env env({{"VGI_STORAGE_S3_BUCKET", "b"},
             {"VGI_STORAGE_S3_ENDPOINT", "http://minio:9000/"},
             {"VGI_STORAGE_S3_PUBLIC_ENDPOINT", "https://s3.example.com"},
             {"VGI_STORAGE_S3_REGION", "auto"}});
    auto settings = direct_storage_settings_from_env();
    REQUIRE(settings);
    CHECK(settings->config.endpoint_url == "http://minio:9000");
    CHECK(settings->config.s3_public_endpoint_url == "https://s3.example.com");
    CHECK(settings->config.s3_addressing_style == "path");
    CHECK(settings->config.region == "auto");
    CHECK(settings->config.uri == "s3://b/rpc");
    const auto validate = direct_storage_url_validator(*settings);
    CHECK_NOTHROW(validate("https://s3.example.com/b/rpc/x"));
    CHECK_NOTHROW(validate("https://s3.example.com:443/b/rpc/x"));
    CHECK_THROWS(validate("http://minio:9000/b/rpc/x"));
}

TEST_CASE("S3 checksum none turns upload grants off") {
    Env env({{"VGI_STORAGE_S3_BUCKET", "b"}, {"VGI_STORAGE_S3_CHECKSUM", "none"}});
    auto settings = direct_storage_settings_from_env();
    REQUIRE(settings);
    CHECK_FALSE(settings->upload_grants);
    CHECK(settings->config.s3_checksum == "none");
    CHECK_THAT(settings->describe(), ContainsSubstring("upload_grants=off"));
}

TEST_CASE("S3 explicit credentials and session expiry (Addendum A.1)") {
    {
        Env env({{"VGI_STORAGE_S3_BUCKET", "b"}, {"VGI_STORAGE_S3_ACCESS_KEY_ID", "AKID"}});
        CHECK_THROWS_WITH(direct_storage_settings_from_env(), ContainsSubstring("set together"));
    }
    {
        Env env({{"VGI_STORAGE_S3_BUCKET", "b"}, {"VGI_STORAGE_S3_SESSION_TOKEN", "t"}});
        CHECK_THROWS_WITH(direct_storage_settings_from_env(), ContainsSubstring("needs"));
    }
    {
        Env env({{"VGI_STORAGE_S3_BUCKET", "b"},
                 {"VGI_STORAGE_S3_ACCESS_KEY_ID", "AKID"},
                 {"VGI_STORAGE_S3_SECRET_ACCESS_KEY", "secret-value"},
                 {"VGI_STORAGE_S3_SESSION_TOKEN", "token-value"}});
        auto settings = direct_storage_settings_from_env();
        REQUIRE(settings);
        REQUIRE(settings->warnings.size() == 1);
        CHECK_THAT(settings->warnings[0], ContainsSubstring("VGI_STORAGE_S3_SESSION_EXPIRES"));
        // Secrets never reach the startup line.
        CHECK(settings->describe().find("secret-value") == std::string::npos);
        CHECK(settings->describe().find("token-value") == std::string::npos);
        CHECK_THAT(settings->describe(), ContainsSubstring("credentials=explicit"));
    }
    {
        Env env({{"VGI_STORAGE_S3_BUCKET", "b"},
                 {"VGI_STORAGE_S3_ACCESS_KEY_ID", "AKID"},
                 {"VGI_STORAGE_S3_SECRET_ACCESS_KEY", "s"},
                 {"VGI_STORAGE_S3_SESSION_TOKEN", "t"},
                 {"VGI_STORAGE_S3_SESSION_EXPIRES", "2026-11-25T18:00:00Z"}});
        auto settings = direct_storage_settings_from_env();
        REQUIRE(settings);
        CHECK(settings->config.s3_session_expires_epoch == 1795629600);
        CHECK(settings->warnings.empty());
    }
    {
        Env env({{"VGI_STORAGE_S3_BUCKET", "b"},
                 {"VGI_STORAGE_S3_SESSION_EXPIRES", "2026-11-25T20:30:00+02:30"}});
        auto settings = direct_storage_settings_from_env();
        REQUIRE(settings);
        CHECK(settings->config.s3_session_expires_epoch == 1795629600);
    }
    {
        Env env({{"VGI_STORAGE_S3_BUCKET", "b"}, {"VGI_STORAGE_S3_SESSION_EXPIRES", "1795629600"}});
        CHECK(direct_storage_settings_from_env()->config.s3_session_expires_epoch == 1795629600);
    }
}

TEST_CASE("URL TTL bounds") {
    {
        Env env({{"VGI_STORAGE_S3_BUCKET", "b"}, {"VGI_STORAGE_URL_TTL_SECONDS", "900"}});
        CHECK(direct_storage_settings_from_env()->config.signed_url_ttl_seconds == 900);
    }
    {
        Env env({{"VGI_STORAGE_S3_BUCKET", "b"}, {"VGI_STORAGE_URL_TTL_SECONDS", "604801"}});
        CHECK_THROWS_WITH(direct_storage_settings_from_env(), ContainsSubstring("7 days"));
    }
}

TEST_CASE("GCS settings") {
    Env env({{"VGI_STORAGE_GCS_BUCKET", "g"},
             {"VGI_STORAGE_GCS_PREFIX", "p/"},
             {"VGI_STORAGE_GCS_VERIFY", "1"}});
    auto settings = direct_storage_settings_from_env();
    REQUIRE(settings);
    CHECK(settings->backend == StorageBackend::Gcs);
    CHECK(settings->config.uri == "gs://g/p/rpc");
    CHECK(settings->config.gcs_verify_at_startup);
    CHECK_NOTHROW(direct_storage_url_validator(*settings)("https://storage.googleapis.com/g/x"));
}

TEST_CASE("configure_http_from_env: an s3 backend this build lacks is refused by name") {
    if (s3_storage_available()) SKIP("this build has the S3 backend");
    Env env({{"VGI_STORAGE_S3_BUCKET", "b"}});
    HttpConfig config;
    CHECK_THROWS_WITH(configure_http_from_env(config), ContainsSubstring("'s3' vcpkg feature"));
}
