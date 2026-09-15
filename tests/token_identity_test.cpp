// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// `vgi_rpc.Identity.v1` -- ported from the Python reference's
// tests/test_token_identity.py, which is the normative test set for this
// protocol.
//
// The two methods are guarded very differently and the difference is the point,
// so most of what is tested here is the *asymmetry*: introspection answers a
// question about somebody else's credential and is therefore an oracle that has
// to be locked down; issuance is always about the caller and therefore is not.

#include "vgi_rpc/token_identity.h"

#include <atomic>
#include <chrono>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include "vgi_rpc/metadata.h"
#include "vgi_rpc/protocol_hash.h"
#include "vgi_rpc/reflection.h"
#include "vgi_rpc/request.h"
#include "vgi_rpc/result.h"
#include "vgi_rpc/server.h"
#include "vgi_rpc/wire.h"

using namespace vgi_rpc;
using Catch::Matchers::ContainsSubstring;

namespace {

double now_seconds() {
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    return std::chrono::duration<double>(now).count();
}

AuthContext make_auth(std::optional<std::string> principal = std::string("alice"),
                      bool authenticated = true, std::optional<double> auth_time = std::nullopt) {
    AuthContext auth;
    auth.domain = "test";
    auth.authenticated = authenticated;
    auth.principal = std::move(principal);
    auth.claims = nlohmann::json::object();
    if (auth_time.has_value()) auth.claims["auth_time"] = *auth_time;
    return auth;
}

std::optional<TokenIdentity> resolver(const std::string& token) {
    if (token == "good") return TokenIdentity{"bob", "ci-key", 300};
    return std::nullopt;
}

IssuedGrant minter(const std::string& principal, const std::string&,
                   const std::vector<std::string>&, int64_t ttl_seconds) {
    return IssuedGrant{"grant-for-" + principal, now_seconds() + static_cast<double>(ttl_seconds),
                       "g1"};
}

IdentityOptions introspect_only() {
    IdentityOptions options;
    options.resolve_token = resolver;
    options.introspect_principals = {"proxy"};
    return options;
}

IdentityOptions mint_only() {
    IdentityOptions options;
    options.mint_grant = minter;
    return options;
}

std::string identity_hash(const std::set<std::string>& offered) {
    auto hash = BindingHash(kIdentityProtocolName, IdentityMethods(offered));
    REQUIRE(hash.ok());
    return *hash;
}

}  // namespace

// ── The cross-port hash vectors ───────────────────────────────────────
//
// Produced by the Python reference; a mismatch means this port and that one
// would disagree about whether they speak the same protocol.  The two
// single-method digests are not decoration: they prove that method-level
// narrowing actually narrows the *hash* rather than hosting a method that
// refuses, which is the difference between a client discovering the shape from
// reflection and discovering it from an error.

TEST_CASE("identity protocol hash matches the reference, both methods") {
    CHECK(identity_hash({"introspect_token", "issue_grant"}) ==
          "8317f2ad8e2476bb99e8b94800ab79b19a8cf0c6bdd6d66c2d82bd62ffbe69d5");
}

TEST_CASE("identity protocol hash matches the reference, introspect_token only") {
    CHECK(identity_hash({"introspect_token"}) ==
          "27b75bef22e4c70baab92a5188a473506b89055d2cb2b58cc187f6fe7a436385");
}

TEST_CASE("identity protocol hash matches the reference, issue_grant only") {
    CHECK(identity_hash({"issue_grant"}) ==
          "c71b12f453310139b6b6a445378064661c52711d03ae1e4fba29b8f7976ef4d8");
}

TEST_CASE("narrowing the method set narrows the protocol hash") {
    const auto both = identity_hash({"introspect_token", "issue_grant"});
    CHECK(both != identity_hash({"introspect_token"}));
    CHECK(both != identity_hash({"issue_grant"}));
    CHECK(identity_hash({"introspect_token"}) != identity_hash({"issue_grant"}));
}

// The canonical preimage, so a digest mismatch is a JSON diff rather than a
// guess.  `scopes`'s list item being nullable is the single most likely thing
// to get wrong, and it is visible right here as `list<item?:utf8>`.
TEST_CASE("identity canonical description spells the nullable list item") {
    auto description = CanonicalDescription(kIdentityProtocolName, [] {
        std::vector<HashMethod> entries;
        for (const auto& [name, info] : IdentityMethods({"issue_grant"})) {
            entries.push_back(HashMethod{name, "unary", true, false, info.params_schema,
                                         info.result_schema, nullptr});
        }
        return entries;
    }());
    REQUIRE(description.ok());
    CHECK_THAT(*description, ContainsSubstring(R"("type":"list<item?:utf8>")"));
}

// ── Registration: absent beats routed-and-refusing ────────────────────

TEST_CASE("identity is absent by default") {
    // A dependency upgrade must not grow a credential-to-identity oracle on
    // every existing worker.
    ServerBuilder builder;
    builder.add_unary("echo", arrow::schema({arrow::field("value", arrow::utf8())}),
                      arrow::schema({arrow::field("result", arrow::utf8())}),
                      [](const Request&, CallContext&) { return Result::void_result(); });
    auto server = builder.build();
    CHECK(server->identity() == nullptr);
}

TEST_CASE("only methods with hooks are hosted") {
    // What the server hosts describes what it actually does.  A worker that
    // resolves credentials but does not mint grants offers one method, and a
    // client learns that from reflection rather than by calling.
    CHECK(IdentityImpl(introspect_only()).offered_methods() ==
          std::set<std::string>{"introspect_token"});
    CHECK(IdentityImpl(mint_only()).offered_methods() == std::set<std::string>{"issue_grant"});

    IdentityOptions both;
    both.resolve_token = resolver;
    both.mint_grant = minter;
    both.introspect_principals = {"proxy"};
    CHECK(IdentityImpl(both).offered_methods() ==
          std::set<std::string>{"introspect_token", "issue_grant"});
}

TEST_CASE("the hosted method table carries only the offered methods") {
    auto impl = std::make_shared<IdentityImpl>(mint_only());
    ServerBuilder builder;
    builder.add_unary("echo", arrow::schema({arrow::field("value", arrow::utf8())}),
                      arrow::schema({arrow::field("result", arrow::utf8())}),
                      [](const Request&, CallContext&) { return Result::void_result(); });
    builder.identity(impl);
    auto server = builder.build();
    REQUIRE(server->identity() != nullptr);

    auto methods = IdentityMethods(server->identity()->offered_methods());
    CHECK(methods.size() == 1);
    CHECK(methods.count("issue_grant") == 1);
    CHECK(identity_hash(server->identity()->offered_methods()) ==
          "c71b12f453310139b6b6a445378064661c52711d03ae1e4fba29b8f7976ef4d8");
}

TEST_CASE("identity claims the reserved prefix") {
    // Framework-owned, so an application cannot register a protocol that
    // impersonates it.
    CHECK(std::string(kIdentityProtocolName).rfind("vgi_rpc.", 0) == 0);
}

// ── Introspection is locked down ──────────────────────────────────────
//
// The answer is an identity assertion the asker acts on with its own
// credentials.  "Trust it as much as you trust the worker" is the wrong frame:
// the asker trusts it *more*, because it authorizes with credentials the worker
// does not hold.

TEST_CASE("introspection resolves for an allowlisted caller") {
    IdentityImpl impl(introspect_only());
    auto got = impl.introspect_token("good", make_auth("proxy"));
    CHECK(got.principal == "bob");
    CHECK(got.token_name == "ci-key");
}

TEST_CASE("a caller off the allowlist is refused") {
    // Authentication is not the same capability as introspection.  A deployment
    // where any valid credential may introspect lets any user test guesses of
    // any other user's credential at unlimited rate, and resolve a stolen one
    // to its owner.
    for (const auto& caller : {std::optional<std::string>("alice"), std::optional<std::string>(""),
                               std::optional<std::string>()}) {
        IdentityImpl impl(introspect_only());
        CHECK_THROWS_AS(impl.introspect_token("good", make_auth(caller)),
                        IntrospectionRefusedError);
    }
}

TEST_CASE("an unauthenticated caller is refused") {
    // The pipe, Unix and TCP transports carry no authenticated principal.
    IdentityImpl impl(introspect_only());
    CHECK_THROWS_AS(impl.introspect_token("good", make_auth("proxy", /*authenticated=*/false)),
                    IntrospectionRefusedError);
}

TEST_CASE("refusal precedes the resolver") {
    // An unauthorized caller learns nothing, including how long it took.
    std::vector<std::string> seen;
    IdentityOptions options;
    options.resolve_token = [&seen](const std::string& token) -> std::optional<TokenIdentity> {
        seen.push_back(token);
        return std::nullopt;
    };
    options.introspect_principals = {"proxy"};
    IdentityImpl impl(std::move(options));

    CHECK_THROWS_AS(impl.introspect_token("secret", make_auth("mallory")),
                    IntrospectionRefusedError);
    CHECK(seen.empty());
}

// The order of the guards is load-bearing, so it gets pinned rather than
// trusted.  An unauthorized caller presenting an over-long or JWS-shaped token
// must still be told `introspection_refused` and never `token_unresolved`:
// answering the cheap syntactic question first would tell an unauthorized
// caller something about the subject credential -- and, by how long the call
// took, tell it even when the message is identical.
TEST_CASE("authorization is checked before the subject credential is touched") {
    IdentityImpl impl(introspect_only());
    const std::string oversize(kMaxTokenChars + 1, 'x');

    for (const std::string& token : {std::string(""), oversize, std::string("aaa.bbb.ccc")}) {
        CHECK_THROWS_AS(impl.introspect_token(token, make_auth("mallory")),
                        IntrospectionRefusedError);
        // And not the other one, which is the actual claim being made.
        CHECK_THROWS_AS(impl.introspect_token(token, make_auth("mallory")), KindedError);
        try {
            impl.introspect_token(token, make_auth("mallory"));
            FAIL("expected a refusal");
        } catch (const KindedError& e) {
            CHECK(e.kind() == "introspection_refused");
        }
    }
}

// The rate limit is also ahead of the credential checks, for the same reason:
// an unauthorized caller must not be able to distinguish "over limit" from
// "malformed token" by which answer comes back.
TEST_CASE("the rate limit is checked before the subject credential is touched") {
    auto options = introspect_only();
    options.introspect_rate_limit = 1;
    IdentityImpl impl(std::move(options));

    CHECK(impl.introspect_token("good", make_auth("proxy")).principal == "bob");
    try {
        // A JWS-shaped token, which would be `token_unresolved` on a fresh
        // budget, is `introspection_refused` once the budget is spent.
        impl.introspect_token("aaa.bbb.ccc", make_auth("proxy"));
        FAIL("expected a refusal");
    } catch (const KindedError& e) {
        CHECK(e.kind() == "introspection_refused");
        CHECK_THAT(std::string(e.what()), ContainsSubstring("rate limit"));
    }
}

TEST_CASE("introspection rejections are uniform") {
    // Unknown, malformed and over-long are one answer.  Distinguishing them
    // would confirm that a guessed credential exists.
    IdentityImpl impl(introspect_only());
    const std::string oversize(kMaxTokenChars + 1, 'x');
    for (const std::string& token : {std::string(""), std::string("unknown"), oversize}) {
        try {
            impl.introspect_token(token, make_auth("proxy"));
            FAIL("expected a rejection");
        } catch (const TokenUnresolvedError& e) {
            CHECK(std::string(e.what()) == "unresolved");
            CHECK(e.kind() == "token_unresolved");
        }
    }
}

TEST_CASE("a JWS never reaches the resolver") {
    // Routing one onward hands a third party a token the asker may have
    // rejected.  A JWS is validated locally against a key set; forwarding one
    // the asker already refused -- expired, wrong audience -- to something that
    // might accept it turns this method into a laundering step.
    std::vector<std::string> seen;
    IdentityOptions options;
    options.resolve_token = [&seen](const std::string& token) -> std::optional<TokenIdentity> {
        seen.push_back(token);
        return TokenIdentity{"bob", "", 300};
    };
    options.introspect_principals = {"proxy"};
    IdentityImpl impl(std::move(options));

    CHECK_THROWS_AS(impl.introspect_token("aaa.bbb.ccc", make_auth("proxy")), TokenUnresolvedError);
    // An unsecured JWT: the third segment may be empty.
    CHECK_THROWS_AS(impl.introspect_token("aaa.bbb.", make_auth("proxy")), TokenUnresolvedError);
    CHECK(seen.empty());

    // An opaque credential that merely contains dots is not a JWS and must
    // still reach the resolver -- refusing everything dotted would be a
    // different bug, and a silent one, since the answer is uniform either way.
    CHECK(impl.introspect_token("a.b", make_auth("proxy")).principal == "bob");
    CHECK(seen == std::vector<std::string>{"a.b"});
}

// ── The JWS shape test survives translation ───────────────────────────
//
// Whitespace must not be a way to walk a JWS past the guard.  The shape test
// runs against the trimmed credential while the resolver still receives what
// the caller sent, so trimming can only add refusals.
//
// This exists because the ports diverged here and the reference was the
// accident: Python's `$` matches before a single trailing newline, so
// "aaa.bbb.ccc\n" was refused there while Go's `\A..\z` and JavaScript's
// unflagged `$` routed it straight to the resolver -- the one outcome the guard
// exists to prevent.  Python was not even self-consistent about it, refusing
// one trailing newline and admitting two.  Trimming first is the rule that
// means the same thing in seven regex dialects, and it is the reason this
// port's hand-rolled matcher needs no anchor special case of its own.

TEST_CASE("padding does not smuggle a JWS past the guard") {
    // No amount of surrounding whitespace makes a JWS resolvable.
    for (const std::string& token :
         {std::string("aaa.bbb.ccc"), std::string("aaa.bbb.ccc\n"), std::string("aaa.bbb.ccc\n\n"),
          std::string("  aaa.bbb.ccc  "), std::string("\taaa.bbb.ccc\r\n")}) {
        CHECK_THROWS_AS(reject_jws_shaped(token), TokenUnresolvedError);
    }
}

TEST_CASE("every codepoint in the spec's trim floor is trimmed") {
    // `IDENTITY_V1_SPEC.md` enumerates the floor every port must trim:
    //
    //     U+0009 U+000A U+000B U+000C U+000D U+0020 U+0085 U+00A0
    //
    // Measured across the ports, this one trimmed only the first six -- which
    // moves the divergence one layer down rather than closing it.  A port
    // trimming a narrower set routes a padded JWS that another port refuses,
    // which is the same hole the guard exists to close.  U+0085 and U+00A0 are
    // two UTF-8 bytes each (`C2 85`, `C2 A0`), so a byte-wise trim has to
    // handle them rather than compare `char`s.
    const std::vector<std::string> floor = {"\x09", "\x0a", "\x0b",     "\x0c",
                                            "\x0d", " ",    "\xc2\x85", "\xc2\xa0"};
    for (const std::string& space : floor) {
        CAPTURE(space);
        // Padding a JWS with it must not smuggle the JWS past the guard,
        // leading, trailing, or both.
        CHECK_THROWS_AS(reject_jws_shaped(space + "aaa.bbb.ccc"), TokenUnresolvedError);
        CHECK_THROWS_AS(reject_jws_shaped("aaa.bbb.ccc" + space), TokenUnresolvedError);
        CHECK_THROWS_AS(reject_jws_shaped(space + "aaa.bbb.ccc" + space), TokenUnresolvedError);
        // And a credential made only of it is not a credential.
        CHECK_THROWS_AS(reject_jws_shaped(space), TokenUnresolvedError);
        CHECK_THROWS_AS(reject_jws_shaped(space + space), TokenUnresolvedError);
    }

    // The whole floor at once, in both orders, is still not a way through.
    std::string all;
    for (const std::string& space : floor) all += space;
    CHECK_THROWS_AS(reject_jws_shaped(all + "aaa.bbb.ccc" + all), TokenUnresolvedError);
    CHECK_THROWS_AS(reject_jws_shaped(all), TokenUnresolvedError);

    // A lone 0x85 or 0xA0 byte is a UTF-8 continuation of some other codepoint,
    // not whitespace: trimming it would corrupt the credential the shape test
    // runs against, so an opaque token carrying one still reaches the resolver.
    CHECK_NOTHROW(reject_jws_shaped(std::string("\xc3\xa0opaque\xc3\xa0")));
}

TEST_CASE("a blank credential is not a credential") {
    // Whitespace-only never reaches a resolver either.
    for (const std::string& token :
         {std::string(""), std::string("   "), std::string("\n"), std::string("\t\r\n")}) {
        CHECK_THROWS_AS(reject_jws_shaped(token), TokenUnresolvedError);
    }
}

TEST_CASE("an opaque credential still reaches the resolver") {
    // Trimming tightens the JWS test; it must not refuse ordinary tokens.
    for (const std::string& token : {std::string("opaque-token"), std::string("a.b.c.d"),
                                     std::string("two.segments"), std::string("sk_live_abc123")}) {
        CHECK_NOTHROW(reject_jws_shaped(token));
    }
}

TEST_CASE("the resolver receives the credential unmodified") {
    // Trimming is for the shape test only -- never for what is resolved.
    // Rewriting a credential before resolving it would make the worker answer
    // about a string the caller never sent.
    std::vector<std::string> seen;
    IdentityOptions options;
    options.resolve_token = [&seen](const std::string& token) -> std::optional<TokenIdentity> {
        seen.push_back(token);
        return TokenIdentity{"p", "", 300};
    };
    options.introspect_principals = {"proxy"};
    IdentityImpl impl(std::move(options));

    impl.introspect_token("  padded-opaque-token  ", make_auth("proxy"));
    CHECK(seen == std::vector<std::string>{"  padded-opaque-token  "});
}

TEST_CASE("identity_unavailable is transient, not definitive") {
    // A caller that negative-caches "unknown" must not cache this.  Cache an
    // outage and a worker restart takes the fleet down for the cache's
    // lifetime; retry a rejection and the worker is hammered.
    IdentityOptions options;
    options.resolve_token = [](const std::string&) -> std::optional<TokenIdentity> {
        throw IdentityUnavailableError("store is down");
    };
    options.introspect_principals = {"proxy"};
    IdentityImpl impl(std::move(options));

    try {
        impl.introspect_token("good", make_auth("proxy"));
        FAIL("expected an outage");
    } catch (const IdentityUnavailableError& e) {
        CHECK(e.retry_after() > 0);
        CHECK(e.kind() == "identity_unavailable");
    }

    // This port's equivalent of the Python hazard: `exception_type_of()` maps
    // `std::invalid_argument` onto the wire class a caller reads as "your input
    // was wrong", and an authenticator chain that advances on that signal would
    // turn a thirty-second sidecar outage into a fleet-wide re-login.  So the
    // transient error must not be catchable as one.
    bool caught_as_invalid_argument = false;
    try {
        impl.introspect_token("good", make_auth("proxy"));
    } catch (const std::invalid_argument&) {
        caught_as_invalid_argument = true;
    } catch (const IdentityUnavailableError&) {
    }
    CHECK_FALSE(caught_as_invalid_argument);

    const IdentityUnavailableError outage("x");
    const std::exception& as_exception = outage;
    CHECK(dynamic_cast<const std::invalid_argument*>(&as_exception) == nullptr);
    // And it names itself on the wire rather than borrowing ValueError.
    CHECK(exception_type_of(outage) == "IdentityUnavailableError");
    CHECK(error_kind_of(outage) == "identity_unavailable");
}

TEST_CASE("introspection is rate limited") {
    // Bounds, rather than closes, the oracle an allowlisted caller still has.
    auto options = introspect_only();
    options.introspect_rate_limit = 2;
    IdentityImpl impl(std::move(options));

    const auto auth = make_auth("proxy");
    CHECK(impl.introspect_token("good", auth).principal == "bob");
    CHECK(impl.introspect_token("good", auth).principal == "bob");
    try {
        impl.introspect_token("good", auth);
        FAIL("expected a refusal");
    } catch (const IntrospectionRefusedError& e) {
        CHECK_THAT(std::string(e.what()), ContainsSubstring("rate limit"));
    }
}

TEST_CASE("an introspector allowlist is mandatory") {
    // There is no permissive default, so it cannot be reached by omission --
    // and it is validated at construction, so a worker that would refuse every
    // introspection fails to start rather than serving traffic until someone
    // tries.
    IdentityOptions no_allowlist;
    no_allowlist.resolve_token = resolver;
    CHECK_THROWS_AS(IdentityImpl(no_allowlist), std::invalid_argument);
    try {
        IdentityImpl impl(no_allowlist);
        FAIL("expected a construction failure");
    } catch (const std::invalid_argument& e) {
        CHECK_THAT(std::string(e.what()), ContainsSubstring("at least one principal"));
    }

    IdentityOptions empty_allowlist;
    empty_allowlist.resolve_token = resolver;
    empty_allowlist.introspect_principals = {};
    CHECK_THROWS_AS(IdentityImpl(empty_allowlist), std::invalid_argument);

    // Blank entries do not count as an allowlist either.
    IdentityOptions blank_allowlist;
    blank_allowlist.resolve_token = resolver;
    blank_allowlist.introspect_principals = {""};
    CHECK_THROWS_AS(IdentityImpl(blank_allowlist), std::invalid_argument);
}

// ── Issuance is not an oracle ─────────────────────────────────────────
//
// Issuance is always about the caller, so it needs neither allowlist nor limit.

TEST_CASE("issuance mints for the caller") {
    // The happy path: a present user minting their own standing grant.
    IdentityImpl impl(mint_only());
    auto grant =
        impl.issue_grant("reports", {"read"}, 3600, make_auth("alice", true, now_seconds()));
    CHECK(grant.token == "grant-for-alice");
    CHECK(grant.expires_at > now_seconds());
}

TEST_CASE("the subject is the caller and is not a parameter") {
    // Cross-subject minting is closed by construction, not by a check.  A check
    // is something one of several ports can forget; a missing parameter is not
    // -- and because the parameter list *is* the wire schema, a port that added
    // one would be caught by the protocol hash too.
    auto methods = IdentityMethods({"issue_grant"});
    const auto& params = methods.at("issue_grant").params_schema;
    CHECK(params->GetFieldIndex("subject") == -1);
    CHECK(params->GetFieldIndex("principal") == -1);
    std::vector<std::string> names;
    for (const auto& field : params->fields()) names.push_back(field->name());
    CHECK(names == std::vector<std::string>{"purpose", "scopes", "ttl_seconds"});
}

TEST_CASE("issuance needs no allowlist") {
    // Unlike introspection -- and the asymmetry is the whole design.
    CHECK(IdentityImpl(mint_only()).offered_methods() == std::set<std::string>{"issue_grant"});
}

// ── Freshness ─────────────────────────────────────────────────────────
//
// A credential with no verifiable auth_time cannot mint.

TEST_CASE("absent auth_time is refused") {
    // A static bearer proves a machine holds a secret, never that a human just
    // logged in.
    IdentityImpl impl(mint_only());
    try {
        impl.issue_grant("p", {}, 60, make_auth("alice"));
        FAIL("expected a refusal");
    } catch (const StaleAuthError& e) {
        CHECK_THAT(std::string(e.what()), ContainsSubstring("no auth_time"));
    }
}

TEST_CASE("stale auth_time is refused actionably") {
    // Naming the reason leaks nothing here: it is always about the caller.  A
    // console that cannot tell "your login is too old" from "no" cannot know to
    // re-prompt.
    IdentityImpl impl(mint_only());
    try {
        impl.issue_grant("p", {}, 60, make_auth("alice", true, now_seconds() - 5000));
        FAIL("expected a refusal");
    } catch (const StaleAuthError& e) {
        CHECK_THAT(std::string(e.what()), ContainsSubstring("re-authenticate"));
    }
}

TEST_CASE("fresh auth_time is accepted") {
    // The ceiling is a ceiling, not an equality.
    IdentityImpl impl(mint_only());
    auto grant = impl.issue_grant("p", {}, 60, make_auth("alice", true, now_seconds() - 10));
    CHECK(grant.token == "grant-for-alice");
}

TEST_CASE("a grant cannot mint another grant") {
    // The lineage cannot escape the identity provider.  A grant is not an
    // IdP-issued token, so it carries no auth_time, so presenting one here
    // fails the freshness check.  That single rule is what stops indefinite
    // self-renewal.
    IdentityImpl impl(mint_only());
    const auto grant_bearer = make_auth("alice");  // no auth_time: this is a grant
    CHECK_THROWS_AS(impl.issue_grant("p", {}, 60, grant_bearer), StaleAuthError);
}

TEST_CASE("an unauthenticated transport fails closed") {
    // Pipe, Unix and TCP have no authenticated principal at all.
    IdentityImpl impl(mint_only());
    try {
        impl.issue_grant("p", {}, 60, AuthContext::anonymous());
        FAIL("expected a refusal");
    } catch (const StaleAuthError& e) {
        CHECK_THAT(std::string(e.what()), ContainsSubstring("not authenticated"));
    }
}

TEST_CASE("an unusable auth_time is refused distinctly from an absent one") {
    // Two different things a console should do about it: re-prompt versus fix
    // the IdP mapping.  A stringified number is still a number, though -- an
    // IdP that quotes its claims must not read as "no auth_time at all".
    IdentityImpl impl(mint_only());
    auto auth = make_auth("alice");
    auth.claims["auth_time"] = "not-a-number";
    try {
        impl.issue_grant("p", {}, 60, auth);
        FAIL("expected a refusal");
    } catch (const StaleAuthError& e) {
        CHECK_THAT(std::string(e.what()), ContainsSubstring("unusable auth_time"));
    }

    auth.claims["auth_time"] = std::to_string(now_seconds());
    CHECK(impl.issue_grant("p", {}, 60, auth).token == "grant-for-alice");
}

// ── Absent hooks ──────────────────────────────────────────────────────

TEST_CASE("introspection without a resolver") {
    // Refused rather than crashing, for a caller that reached it anyway: the
    // per-method guard is the belt to method-level narrowing's braces.
    IdentityImpl impl(mint_only());
    try {
        impl.introspect_token("good", make_auth("proxy"));
        FAIL("expected a refusal");
    } catch (const IntrospectionRefusedError& e) {
        CHECK_THAT(std::string(e.what()), ContainsSubstring("does not resolve"));
    }
}

TEST_CASE("issuance without a minter") {
    IdentityImpl impl(introspect_only());
    try {
        impl.issue_grant("p", {}, 60, make_auth("alice", true, now_seconds()));
        FAIL("expected a refusal");
    } catch (const GrantRefusedError& e) {
        CHECK_THAT(std::string(e.what()), ContainsSubstring("does not mint"));
    }
}

// ── Diagnostics ───────────────────────────────────────────────────────

TEST_CASE("the token digest is not the token") {
    // Stable enough to correlate one credential's failures; not the credential.
    CHECK(token_digest("secret") != "secret");
    CHECK(token_digest("secret") == token_digest("secret"));
    CHECK(token_digest("secret") != token_digest("other"));
    CHECK(token_digest("secret").size() == 64);
}

TEST_CASE("error kinds are stable") {
    // They are the only definitive/transient signal a caller has.  These were
    // an HTTP route whose callers classified on the status code (404 vs 503).
    // As protocol methods every handler exception surfaces the same way, so
    // `error_kind` carries the whole distinction.
    CHECK(IntrospectionRefusedError("x").kind() == "introspection_refused");
    CHECK(TokenUnresolvedError().kind() == "token_unresolved");
    CHECK(StaleAuthError("x").kind() == "stale_auth");
    CHECK(GrantRefusedError("x").kind() == "grant_refused");
    CHECK(IdentityUnavailableError("x").kind() == "identity_unavailable");
    CHECK(IdentityUnavailableError().retry_after() == kDefaultIdentityRetryAfter);
}

// ── The rate limiter ──────────────────────────────────────────────────
//
// Fixed-window, because the state is two integers rather than an aged float.

TEST_CASE("the limiter admits up to the limit within a window") {
    RateLimiter limiter(3);
    std::vector<bool> admitted;
    for (int i = 0; i < 4; ++i) admitted.push_back(limiter.allow("a", 100.0));
    CHECK(admitted == std::vector<bool>{true, true, true, false});
}

TEST_CASE("the limiter's window rolls") {
    RateLimiter limiter(1);
    CHECK(limiter.allow("a", 100.0));
    CHECK_FALSE(limiter.allow("a", 100.5));
    CHECK(limiter.allow("a", 101.5));
}

TEST_CASE("limiter callers are independent") {
    // One caller exhausting its budget must not refuse another.
    RateLimiter limiter(1);
    CHECK(limiter.allow("a", 100.0));
    CHECK(limiter.allow("b", 100.0));
    CHECK_FALSE(limiter.allow("a", 100.0));
}

TEST_CASE("cycling keys cannot grow the limiter's map") {
    // Whole-map reset rather than per-key ageing, so an attacker cannot.
    // Per-key ageing would let a caller cycling keys grow the map without bound
    // between sweeps.
    RateLimiter limiter(1);
    for (int i = 0; i < 1000; ++i) limiter.allow("k" + std::to_string(i), 100.0);
    limiter.allow("fresh", 200.0);
    CHECK(limiter.tracked_keys() == 1);
}

TEST_CASE("the limiter is safe under concurrent callers") {
    // This port dispatches unrelated HTTP and socket calls concurrently, so an
    // unsynchronized counter would not merely race -- it would *undercount*,
    // which is the direction that fails open on a credential oracle.
    RateLimiter limiter(100);
    std::atomic<int> admitted{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < 8; ++t) {
        threads.emplace_back([&limiter, &admitted]() {
            for (int i = 0; i < 200; ++i) {
                if (limiter.allow("shared", 100.0)) admitted.fetch_add(1);
            }
        });
    }
    for (auto& thread : threads) thread.join();
    CHECK(admitted.load() == 100);
}

// ── The wire: routing, narrowing and reflection ───────────────────────

namespace {

std::shared_ptr<arrow::Buffer> identity_request(const std::string& method,
                                                const std::shared_ptr<arrow::Schema>& schema,
                                                const std::shared_ptr<arrow::RecordBatch>& batch) {
    auto md = std::make_shared<arrow::KeyValueMetadata>();
    md->Append(keys::METHOD, method);
    md->Append(keys::REQUEST_VERSION, REQUEST_VERSION_VALUE);
    md->Append(keys::PROTOCOL, kIdentityProtocolName);
    auto sink = arrow::io::BufferOutputStream::Create().ValueUnsafe();
    AnnotatedBatch ab;
    ab.batch = batch;
    ab.custom_metadata = md;
    write_ipc_stream(sink, schema, {ab});
    return sink->Finish().ValueUnsafe();
}

std::shared_ptr<arrow::Buffer> introspect_request(const std::string& token) {
    auto schema = arrow::schema({arrow::field("token", arrow::utf8(), /*nullable=*/false)});
    arrow::StringBuilder builder;
    REQUIRE(builder.Append(token).ok());
    std::shared_ptr<arrow::Array> column;
    REQUIRE(builder.Finish(&column).ok());
    return identity_request("introspect_token", schema,
                            arrow::RecordBatch::Make(schema, 1, {column}));
}

// Run one request through the server and return the decoded response.
IpcStreamContents round_trip(Server& server, const std::shared_ptr<arrow::Buffer>& request,
                             const AuthContext& auth) {
    auto input = std::make_shared<arrow::io::BufferReader>(request);
    auto output = arrow::io::BufferOutputStream::Create().ValueUnsafe();
    REQUIRE(server.serve_one(input, output, TransportKind::TCP, auth, PeerEvidenceSet()));
    auto buffer = output->Finish().ValueUnsafe();
    auto source = std::make_shared<arrow::io::BufferReader>(buffer);
    auto contents = read_ipc_stream(source);
    REQUIRE(contents.has_value());
    return std::move(*contents);
}

std::string wire_error_kind(const IpcStreamContents& contents) {
    for (const auto& ab : contents.batches) {
        if (ab.custom_metadata == nullptr) continue;
        const auto index = ab.custom_metadata->FindKey(keys::ERROR_KIND);
        if (index >= 0) return ab.custom_metadata->value(index);
    }
    return "";
}

std::unique_ptr<Server> server_with(std::shared_ptr<IdentityImpl> impl) {
    ServerBuilder builder;
    builder.add_unary("echo", arrow::schema({arrow::field("value", arrow::utf8())}),
                      arrow::schema({arrow::field("result", arrow::utf8())}),
                      [](const Request&, CallContext&) { return Result::void_result(); });
    if (impl != nullptr) builder.identity(std::move(impl));
    return builder.build();
}

}  // namespace

TEST_CASE("the identity protocol routes by protocol key and returns its payload") {
    auto server = server_with(std::make_shared<IdentityImpl>(introspect_only()));
    auto contents = round_trip(*server, introspect_request("good"), make_auth("proxy"));

    REQUIRE(contents.batches.size() == 1);
    const auto& batch = contents.batches[0].batch;
    REQUIRE(batch->num_columns() == 1);
    REQUIRE(batch->schema()->field(0)->name() == "result");

    // The payload is the TokenIdentity, as an IPC stream in the single `result`
    // binary column -- the framework's ordinary convention for a structured
    // return, which reflection also uses.
    auto column = std::static_pointer_cast<arrow::BinaryArray>(batch->column(0));
    auto payload = column->GetString(0);
    auto payload_source =
        std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(std::move(payload)));
    auto decoded = read_ipc_stream(payload_source);
    REQUIRE(decoded.has_value());
    REQUIRE(decoded->batches.size() == 1);
    auto identity = decoded->batches[0].batch;
    CHECK(identity->schema()->Equals(*TokenIdentity::arrow_schema()));
    CHECK(std::static_pointer_cast<arrow::StringArray>(identity->column(0))->GetString(0) == "bob");
}

TEST_CASE("an identity refusal carries its error_kind onto the wire") {
    auto server = server_with(std::make_shared<IdentityImpl>(introspect_only()));

    // Definitive-but-cacheable, for a caller that may not ask at all.
    CHECK(wire_error_kind(round_trip(*server, introspect_request("good"), make_auth("mallory"))) ==
          "introspection_refused");
    // Definitive and uniform, for a credential that did not resolve.
    CHECK(wire_error_kind(round_trip(*server, introspect_request("nope"), make_auth("proxy"))) ==
          "token_unresolved");
}

TEST_CASE("an unhosted identity method is absent rather than refusing") {
    // The deployment configured no minter, so `issue_grant` is not hosted: the
    // client is told the method does not exist, which is what makes reflection
    // an honest description of the worker.
    auto server = server_with(std::make_shared<IdentityImpl>(introspect_only()));
    auto request =
        identity_request("issue_grant", arrow::schema({}),
                         arrow::RecordBatch::Make(arrow::schema({}), 1, arrow::ArrayVector{}));
    auto contents = round_trip(*server, request, make_auth("alice", true, now_seconds()));
    REQUIRE(contents.batches.size() == 1);
    REQUIRE(contents.batches[0].custom_metadata != nullptr);
    const auto index = contents.batches[0].custom_metadata->FindKey(keys::LOG_MESSAGE);
    REQUIRE(index >= 0);
    CHECK_THAT(contents.batches[0].custom_metadata->value(index),
               ContainsSubstring("has no method 'issue_grant'"));
}

TEST_CASE("a worker without identity does not host the protocol at all") {
    auto server = server_with(nullptr);
    auto contents = round_trip(*server, introspect_request("good"), make_auth("proxy"));
    REQUIRE(contents.batches.size() == 1);
    REQUIRE(contents.batches[0].custom_metadata != nullptr);
    const auto index = contents.batches[0].custom_metadata->FindKey(keys::LOG_MESSAGE);
    REQUIRE(index >= 0);
    CHECK_THAT(contents.batches[0].custom_metadata->value(index),
               ContainsSubstring("does not host protocol"));
}

TEST_CASE("identity is describable through reflection when hosted") {
    // Registered after reflection precisely so it appears in reflection's
    // output: a client discovers that this worker resolves credentials without
    // calling anything and reading an error.
    auto describe = [](Server& server) {
        auto schema = arrow::schema({arrow::field("protocol", arrow::utf8())});
        arrow::StringBuilder builder;
        REQUIRE(builder.Append(kIdentityProtocolName).ok());
        std::shared_ptr<arrow::Array> column;
        REQUIRE(builder.Finish(&column).ok());
        auto md = std::make_shared<arrow::KeyValueMetadata>();
        md->Append(keys::METHOD, "describe");
        md->Append(keys::REQUEST_VERSION, REQUEST_VERSION_VALUE);
        md->Append(keys::PROTOCOL, kReflectionProtocolName);
        auto sink = arrow::io::BufferOutputStream::Create().ValueUnsafe();
        AnnotatedBatch ab;
        ab.batch = arrow::RecordBatch::Make(schema, 1, {column});
        ab.custom_metadata = md;
        write_ipc_stream(sink, schema, {ab});
        return round_trip(server, sink->Finish().ValueUnsafe(), AuthContext::anonymous());
    };

    auto hosted = server_with(std::make_shared<IdentityImpl>(introspect_only()));
    auto described = describe(*hosted);
    REQUIRE(described.batches.size() == 1);
    CHECK(described.batches[0].batch->schema()->field(0)->name() == "result");

    auto bare = server_with(nullptr);
    auto missing = describe(*bare);
    REQUIRE(missing.batches.size() == 1);
    REQUIRE(missing.batches[0].custom_metadata != nullptr);
    const auto index = missing.batches[0].custom_metadata->FindKey(keys::LOG_MESSAGE);
    REQUIRE(index >= 0);
    CHECK_THAT(missing.batches[0].custom_metadata->value(index),
               ContainsSubstring("does not host protocol"));
}
