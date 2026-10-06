// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/// The traceback policy of the dispatch running on this thread.
///
/// WIRE_PROTOCOL.md §8: when a server's traceback setting is on, *every*
/// EXCEPTION batch it writes carries a non-empty `log_extra.traceback` --
/// routing refusals and version mismatches as much as a raising handler.
/// Those are written from dozens of sites through `make_error_metadata`, so
/// rather than threading the setting through each one, the dispatch entry
/// points (pipe/unix/tcp `serve_one_with_state`, HTTP `handle_rpc`) install
/// this scope and `make_error_metadata` synthesizes a trace for any batch
/// written without one.  Outside a dispatch -- client code, unit tests -- no
/// scope is installed and nothing is synthesized.
#pragma once

#include <string>

namespace vgi_rpc::detail {

struct TracebackScope {
    bool include = true;
    // `protocol/method`, once dispatch has resolved them; empty before.
    std::string where;
};

TracebackScope* current_traceback_scope() noexcept;

class ScopedTracebackPolicy {
public:
    explicit ScopedTracebackPolicy(bool include);
    ~ScopedTracebackPolicy();
    ScopedTracebackPolicy(const ScopedTracebackPolicy&) = delete;
    ScopedTracebackPolicy& operator=(const ScopedTracebackPolicy&) = delete;

    void set_where(std::string where) { scope_.where = std::move(where); }

private:
    TracebackScope scope_;
    TracebackScope* previous_;
};

}  // namespace vgi_rpc::detail
