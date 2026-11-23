// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/object_cache.h"

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string_view>

#if defined(_WIN32)
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <unistd.h>
#endif

namespace vgi_rpc {

namespace {

constexpr int64_t kOneGiB = int64_t{1} << 30;

double wall_seconds() {
    return std::chrono::duration<double>(std::chrono::system_clock::now().time_since_epoch())
        .count();
}

std::optional<int64_t> parse_non_negative(std::string_view raw) {
    while (!raw.empty() && (raw.front() == ' ' || raw.front() == '\t')) raw.remove_prefix(1);
    while (!raw.empty() &&
           (raw.back() == ' ' || raw.back() == '\t' || raw.back() == '\n' || raw.back() == '\r')) {
        raw.remove_suffix(1);
    }
    if (raw.empty()) return std::nullopt;
    int64_t value = 0;
    for (char c : raw) {
        if (c < '0' || c > '9') return std::nullopt;
        if (value > (INT64_MAX - (c - '0')) / 10) return std::nullopt;
        value = value * 10 + (c - '0');
    }
    return value;
}

std::optional<int64_t> cgroup_memory_limit() {
    for (const char* path :
         {"/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"}) {
        std::ifstream in(path);
        if (!in) continue;
        std::string raw;
        std::getline(in, raw);
        if (raw.empty() || raw == "max") return std::nullopt;
        const auto value = parse_non_negative(raw);
        if (!value) continue;
        // cgroup v1 reports "unlimited" as a huge page-aligned number.
        if (*value <= 0 || *value >= (int64_t{1} << 60)) return std::nullopt;
        return value;
    }
    return std::nullopt;
}

std::optional<int64_t> physical_memory() {
#if defined(_WIN32)
    MEMORYSTATUSEX status;
    status.dwLength = sizeof(status);
    if (!GlobalMemoryStatusEx(&status)) return std::nullopt;
    return static_cast<int64_t>(status.ullTotalPhys);
#else
    const long pages = sysconf(_SC_PHYS_PAGES);
    const long page_size = sysconf(_SC_PAGE_SIZE);
    if (pages <= 0 || page_size <= 0) return std::nullopt;
    return static_cast<int64_t>(pages) * static_cast<int64_t>(page_size);
#endif
}

}  // namespace

ObjectCache::ObjectCache(int64_t max_bytes, int64_t max_age_seconds, Clock clock)
    : max_bytes_(max_bytes < 0 ? 0 : max_bytes),
      max_age_seconds_(max_age_seconds),
      clock_(std::move(clock)) {}

double ObjectCache::now() const { return clock_ ? clock_() : wall_seconds(); }

ObjectCacheStats ObjectCache::stats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    ObjectCacheStats out;
    out.resident_bytes = resident_;
    out.entries = static_cast<int64_t>(entries_.size());
    out.max_bytes = max_bytes_;
    out.hits = hits_;
    out.misses = misses_;
    out.evictions = evictions_;
    out.fetches = fetches_;
    return out;
}

bool ObjectCache::contains(const std::string& object_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = entries_.find(object_id);
    return it != entries_.end() && now() - it->second.fetched_at <= max_age_seconds_;
}

int64_t ObjectCache::age_entries(double seconds) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& [_, entry] : entries_) entry.fetched_at -= seconds;
    return static_cast<int64_t>(entries_.size());
}

void ObjectCache::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    entries_.clear();
    lru_.clear();
    resident_ = 0;
}

void ObjectCache::erase_locked(std::unordered_map<std::string, Entry>::iterator it) {
    resident_ -= static_cast<int64_t>(it->second.value->size());
    lru_.erase(it->second.lru);
    entries_.erase(it);
}

void ObjectCache::insert_locked(const std::string& object_id,
                                std::shared_ptr<const std::string> value) {
    const auto size = static_cast<int64_t>(value->size());
    if (size > max_bytes_) {
        std::cerr << "vgi_rpc: object_cache: a " << size << "-byte value exceeds the cache bound ("
                  << max_bytes_ << " bytes, " << OBJECT_CACHE_BYTES_ENV
                  << "); resolved but not retained\n";
        return;
    }
    if (auto previous = entries_.find(object_id); previous != entries_.end()) {
        erase_locked(previous);
    }
    lru_.push_back(object_id);
    entries_.emplace(object_id, Entry{std::move(value), now(), std::prev(lru_.end())});
    resident_ += size;
    while (resident_ > max_bytes_ && !lru_.empty()) {
        erase_locked(entries_.find(lru_.front()));
        ++evictions_;
    }
}

std::pair<std::shared_ptr<const std::string>, bool> ObjectCache::get_or_fetch(
    const std::string& object_id, const std::function<std::string()>& fetch) {
    std::shared_ptr<Flight> flight;
    bool leader = false;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        if (auto it = entries_.find(object_id); it != entries_.end()) {
            if (now() - it->second.fetched_at <= max_age_seconds_) {
                lru_.splice(lru_.end(), lru_, it->second.lru);
                ++hits_;
                return {it->second.value, true};
            }
            // Aged out: the object may already be gone; never serve it.
            erase_locked(it);
            ++evictions_;
        }
        ++misses_;
        auto found = flights_.find(object_id);
        if (found == flights_.end()) {
            flight = std::make_shared<Flight>();
            flights_.emplace(object_id, flight);
            ++fetches_;
            leader = true;
        } else {
            flight = found->second;
        }
        if (!leader) {
            flight->cv.wait(lock, [&] { return flight->done; });
            if (flight->error) std::rethrow_exception(flight->error);
            return {flight->value, false};
        }
    }

    std::shared_ptr<const std::string> value;
    try {
        value = std::make_shared<const std::string>(fetch());
    } catch (...) {
        std::lock_guard<std::mutex> lock(mutex_);
        flight->error = std::current_exception();
        flight->done = true;
        flights_.erase(object_id);
        flight->cv.notify_all();
        throw;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    flight->value = value;
    flight->done = true;
    flights_.erase(object_id);
    insert_locked(object_id, value);
    flight->cv.notify_all();
    return {value, false};
}

int64_t default_object_cache_bytes() {
    if (const char* raw = std::getenv(OBJECT_CACHE_BYTES_ENV); raw != nullptr) {
        std::string_view text(raw);
        bool blank = true;
        for (char c : text) blank = blank && (c == ' ' || c == '\t');
        if (!blank) {
            const auto value = parse_non_negative(text);
            if (!value) {
                throw std::invalid_argument(std::string(OBJECT_CACHE_BYTES_ENV) +
                                            " must be a non-negative integer, got '" + raw + "'");
            }
            return *value;
        }
    }
    auto memory = cgroup_memory_limit();
    if (!memory) memory = physical_memory();
    if (!memory) return kOneGiB;
    return std::min(kOneGiB, (*memory / 100) * 40);
}

ObjectCache& default_object_cache() {
    // Created on first use and never destroyed: worker threads may still be
    // resolving while static destructors run at exit.
    static ObjectCache* cache = new ObjectCache(default_object_cache_bytes());
    return *cache;
}

}  // namespace vgi_rpc
