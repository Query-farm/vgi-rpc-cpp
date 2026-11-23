// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/// The per-process object cache of protocol 3.4 (STATE_REF_V1.md §11).
///
/// Required framework behaviour, not wire protocol: nothing about it is
/// visible to a client.  It holds the verified bytes of objects fetched by id
/// -- argument references here, stored `StateRef`s in the SDKs that store
/// them -- so parallel scans, secondary inits and repeated calls on one large
/// argument cost one storage GET per process.
///
///   * **Keyed by `object_id`, never by a SHA-256.**  A grant mints an object
///     for a *claimed* hash without seeing the bytes, so a digest-keyed cache
///     would hand one principal's value to another who merely knows its hash.
///     An `object_id` only reaches a server inside a ref sealed for the caller.
///   * **Single-flight:** concurrent lookups of one id share one fetch.
///   * **Byte-bounded LRU** (`VGI_RPC_OBJECT_CACHE_BYTES`, default
///     `min(1 GiB, 40%` of the cgroup memory limit, else of physical memory`)`).
///     A value larger than the bound is returned but not retained.
///   * **One-day age rule:** an entry fetched more than a day ago is a miss,
///     so a cache never outlives the bucket lifecycle of the object it mirrors.
///
/// Callers that remember a value keep its `object_id` and look it up again;
/// holding the bytes elsewhere would defeat the bound.
#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

#include "vgi_rpc/export.h"

namespace vgi_rpc {

/// Environment variable overriding the default cache's byte bound.
inline constexpr const char* OBJECT_CACHE_BYTES_ENV = "VGI_RPC_OBJECT_CACHE_BYTES";
/// An entry fetched longer ago than this is treated as a miss (one day).
inline constexpr int64_t OBJECT_CACHE_MAX_AGE_SECONDS = 86400;

/// A snapshot of the cache's counters (the conformance `object_cache_stats`).
struct ObjectCacheStats {
    int64_t resident_bytes = 0;  // bytes of values currently retained
    int64_t entries = 0;         // retained values
    int64_t max_bytes = 0;       // the byte bound
    int64_t hits = 0;            // lookups answered from the cache
    int64_t misses = 0;          // lookups that fetched or joined a fetch (aged-out included)
    int64_t evictions = 0;       // entries dropped for the bound or aged out
    int64_t fetches = 0;         // fetches actually run (single-flight joins excluded)
};

/// Byte-bounded, single-flight LRU of verified object values keyed by
/// `object_id`.  Thread-safe.  Values are whatever the caller's fetch returns
/// (the verified bytes); the cache never inspects them.
class VGI_RPC_EXPORT ObjectCache {
public:
    using Clock = std::function<double()>;  // seconds since the Unix epoch

    /// `max_bytes = 0` retains nothing (every lookup fetches, still
    /// single-flight).  `clock` is injectable for tests.
    explicit ObjectCache(int64_t max_bytes,
                         int64_t max_age_seconds = OBJECT_CACHE_MAX_AGE_SECONDS,
                         Clock clock = {});

    ObjectCache(const ObjectCache&) = delete;
    ObjectCache& operator=(const ObjectCache&) = delete;

    int64_t max_bytes() const noexcept { return max_bytes_; }
    ObjectCacheStats stats() const;

    /// The value for `object_id`, running `fetch` at most once across threads
    /// per miss; concurrent callers wait for and share its result, or its
    /// exception.  The second member is true when the value came from the
    /// cache rather than from this call's fetch or a joined one.
    std::pair<std::shared_ptr<const std::string>, bool> get_or_fetch(
        const std::string& object_id, const std::function<std::string()>& fetch);

    /// Whether a fresh entry for `object_id` is retained.  Counters unchanged.
    bool contains(const std::string& object_id) const;

    /// Move every entry's fetch time `seconds` into the past and return how
    /// many moved.  A test hook: it proves the one-day rule without waiting a
    /// day (the conformance `object_cache_age`).
    int64_t age_entries(double seconds);

    /// Drop every entry.
    void clear();

private:
    struct Entry {
        std::shared_ptr<const std::string> value;
        double fetched_at = 0;
        std::list<std::string>::iterator lru;  // position in lru_ (front = oldest)
    };
    struct Flight {
        bool done = false;
        std::shared_ptr<const std::string> value;
        std::exception_ptr error;
        std::condition_variable cv;
    };

    double now() const;
    void erase_locked(std::unordered_map<std::string, Entry>::iterator it);
    void insert_locked(const std::string& object_id, std::shared_ptr<const std::string> value);

    const int64_t max_bytes_;
    const int64_t max_age_seconds_;
    Clock clock_;

    mutable std::mutex mutex_;
    std::unordered_map<std::string, Entry> entries_;
    std::list<std::string> lru_;
    std::unordered_map<std::string, std::shared_ptr<Flight>> flights_;
    int64_t resident_ = 0;
    int64_t hits_ = 0;
    int64_t misses_ = 0;
    int64_t evictions_ = 0;
    int64_t fetches_ = 0;
};

/// The default bound: `VGI_RPC_OBJECT_CACHE_BYTES` when set (a non-negative
/// integer, else `std::invalid_argument`), otherwise `min(1 GiB, 40%` of the
/// cgroup v2 `memory.max` / v1 `memory.limit_in_bytes`, else of physical
/// memory`)`.
VGI_RPC_EXPORT int64_t default_object_cache_bytes();

/// The process-wide cache, created on first use with
/// `default_object_cache_bytes()`.  Every server in the process shares it.
VGI_RPC_EXPORT ObjectCache& default_object_cache();

}  // namespace vgi_rpc
