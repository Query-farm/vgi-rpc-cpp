// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/session.h"

#include <cstring>
#include <vector>

namespace vgi_rpc {

namespace {

// Domain separator for the session/state envelope, matching the reference's
// v4 AAD prefix.  Distinct from every other keyed use in the framework so a
// token from one cannot be replayed into another.
constexpr const char kAadPrefix[] = "vgi_rpc.state.v4";

void put_u64_le(std::string& out, uint64_t v) {
    for (int i = 0; i < 8; ++i) out.push_back(static_cast<char>(v >> (i * 8)));
}

bool take_u64_le(const std::string& in, size_t& off, uint64_t* out) {
    if (in.size() - off < 8) return false;
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v |= static_cast<uint64_t>(static_cast<uint8_t>(in[off + i])) << (i * 8);
    }
    off += 8;
    *out = v;
    return true;
}

uint64_t unix_now() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(
                                     std::chrono::system_clock::now().time_since_epoch())
                                     .count());
}

}  // namespace

std::string session_aad(const std::string& domain, const std::string& principal,
                        bool authenticated) {
    std::string aad(kAadPrefix, sizeof(kAadPrefix) - 1);
    aad.push_back('\0');
    if (authenticated) {
        aad.push_back('\x01');
        aad += domain;
        aad.push_back('\0');
        aad += principal;
    } else {
        aad.push_back('\0');
        aad += "anonymous";
    }
    return aad;
}

SessionRegistry::SessionRegistry(std::array<uint8_t, crypto::kAeadKeyBytes> key,
                                 std::string server_id, int default_ttl_seconds)
    : key_(key),
      server_id_(std::move(server_id)),
      default_ttl_seconds_(default_ttl_seconds > 0 ? default_ttl_seconds : 300) {}

std::string SessionRegistry::open(std::shared_ptr<SessionState> state, const std::string& aad,
                                  std::optional<int> ttl_seconds) {
    const int ttl = ttl_seconds.value_or(default_ttl_seconds_);
    auto id_bytes = crypto::random_bytes(12);
    const std::string session_id = crypto::hex_encode(id_bytes.data(), id_bytes.size());

    // created_at | server_id_len | server_id | session_id | expires_at
    const uint64_t now = unix_now();
    std::string frame;
    put_u64_le(frame, now);
    frame.push_back(static_cast<char>(server_id_.size() & 0xFF));
    frame += server_id_;
    frame += session_id;
    put_u64_le(frame, now + static_cast<uint64_t>(ttl));

    auto entry = std::make_shared<Entry>();
    entry->state = std::move(state);
    entry->expires_at = std::chrono::steady_clock::now() + std::chrono::seconds(ttl);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        entries_[session_id] = std::move(entry);
    }

    std::string sealed = crypto::aead_seal(key_, frame, aad);
    // Version byte outside the ciphertext: it selects the algorithm the
    // recipient opens with, so a tampered one fails decryption anyway.
    std::string envelope;
    envelope.push_back('\x01');
    envelope += sealed;
    return crypto::base64url_encode(envelope);
}

SessionLookup SessionRegistry::unseal(const std::string& token, const std::string& aad,
                                      std::string* out_session_id) const {
    if (token.empty()) return SessionLookup::NOT_PRESENTED;

    auto raw = crypto::base64url_decode(token);
    if (!raw || raw->size() < 2 || (*raw)[0] != '\x01') return SessionLookup::UNSEALABLE;

    auto frame = crypto::aead_open(key_, raw->substr(1), aad);
    if (!frame) return SessionLookup::UNSEALABLE;

    size_t off = 0;
    uint64_t created_at = 0;
    if (!take_u64_le(*frame, off, &created_at)) return SessionLookup::UNSEALABLE;
    if (frame->size() - off < 1) return SessionLookup::UNSEALABLE;
    const size_t id_len = static_cast<uint8_t>((*frame)[off++]);
    if (frame->size() - off < id_len + 24 + 8) return SessionLookup::UNSEALABLE;

    const std::string minted_by = frame->substr(off, id_len);
    off += id_len;
    const std::string session_id = frame->substr(off, 24);
    off += 24;
    uint64_t expires_at = 0;
    if (!take_u64_le(*frame, off, &expires_at)) return SessionLookup::UNSEALABLE;

    // A peer sharing our key decrypts fine; only the id says it was not us.
    // Compared after the open, never instead of it — the AEAD is the control.
    if (minted_by != server_id_) return SessionLookup::WRONG_WORKER;
    if (unix_now() > expires_at) return SessionLookup::EXPIRED;

    *out_session_id = session_id;
    return SessionLookup::OK;
}

SessionLookup SessionRegistry::resolve(const std::string& token, const std::string& aad,
                                       std::shared_ptr<SessionState>* out_state,
                                       std::string* out_session_id,
                                       std::unique_lock<std::recursive_mutex>* out_dispatch_lock) {
    std::string session_id;
    const SessionLookup status = unseal(token, aad, &session_id);
    if (status != SessionLookup::OK) return status;

    std::shared_ptr<Entry> entry;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = entries_.find(session_id);
        if (it == entries_.end()) return SessionLookup::MISSING;
        entry = it->second;
    }

    // Never wait for a session while holding the registry mutex: a handler
    // already holding this per-session lock may legitimately open or close a
    // session, both of which need the registry briefly.
    std::unique_lock<std::recursive_mutex> dispatch_lock(entry->dispatch_mutex);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = entries_.find(session_id);
        if (it == entries_.end() || it->second != entry) return SessionLookup::MISSING;
        if (std::chrono::steady_clock::now() > entry->expires_at) {
            entries_.erase(it);
        } else {
            *out_state = entry->state;
            *out_session_id = session_id;
            if (out_dispatch_lock) *out_dispatch_lock = std::move(dispatch_lock);
            return SessionLookup::OK;
        }
    }

    if (entry->state) entry->state->close();
    return SessionLookup::EXPIRED;
}

bool SessionRegistry::close(const std::string& token, const std::string& aad) {
    std::string session_id;
    if (unseal(token, aad, &session_id) != SessionLookup::OK) return false;

    std::shared_ptr<Entry> entry;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = entries_.find(session_id);
        if (it == entries_.end()) return false;
        entry = it->second;
    }

    std::unique_lock<std::recursive_mutex> dispatch_lock(entry->dispatch_mutex);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = entries_.find(session_id);
        if (it == entries_.end() || it->second != entry) return false;
        entries_.erase(it);
    }
    if (entry->state) entry->state->close();
    return true;
}

void SessionRegistry::sweep() {
    std::vector<std::pair<std::string, std::shared_ptr<Entry>>> candidates;
    const auto now = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& [session_id, entry] : entries_) {
            if (now > entry->expires_at) candidates.emplace_back(session_id, entry);
        }
    }

    for (auto& [session_id, entry] : candidates) {
        std::unique_lock<std::recursive_mutex> dispatch_lock(entry->dispatch_mutex);
        bool removed = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = entries_.find(session_id);
            if (it != entries_.end() && it->second == entry &&
                std::chrono::steady_clock::now() > entry->expires_at) {
                entries_.erase(it);
                removed = true;
            }
        }
        if (removed && entry->state) entry->state->close();
    }
}

void SessionRegistry::shutdown() {
    std::vector<std::shared_ptr<Entry>> live;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& [_, entry] : entries_) live.push_back(entry);
        entries_.clear();
    }
    for (auto& entry : live) {
        std::unique_lock<std::recursive_mutex> dispatch_lock(entry->dispatch_mutex);
        if (entry->state) entry->state->close();
    }
}

}  // namespace vgi_rpc
