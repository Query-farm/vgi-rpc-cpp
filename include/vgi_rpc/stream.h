// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <functional>
#include <memory>

#include <arrow/type.h>

#include "vgi_rpc/annotated_batch.h"
#include "vgi_rpc/call_context.h"
#include "vgi_rpc/output_collector.h"

namespace vgi_rpc {

// Base class for stream state (mirrors Python's StreamState)
class StreamState {
public:
    virtual ~StreamState() noexcept = default;

    // Called once per input batch (tick for producer, data for exchange)
    virtual void process(const AnnotatedBatch& input,
                        OutputCollector& out, CallContext& ctx) = 0;

    // Called when the client cancels the stream (sends a batch carrying
    // vgi_rpc.cancel metadata).  Default is a no-op.  Implementations may
    // override to release resources or record cancellation.
    virtual void on_cancel(CallContext& /*ctx*/) {}
};

// Producer stream state — ignores input tick, just produces output
class ProducerState : public StreamState {
public:
    // Override this to produce output
    virtual void produce(OutputCollector& out, CallContext& ctx) = 0;

    // Delegates to produce(), ignoring tick input
    void process(const AnnotatedBatch& /*input*/,
                OutputCollector& out, CallContext& ctx) override {
        produce(out, ctx);
    }
};

// Exchange stream state — processes input, must emit exactly one output batch
class ExchangeState : public StreamState {
public:
    // Override this to process input and emit output
    virtual void exchange(const AnnotatedBatch& input,
                         OutputCollector& out, CallContext& ctx) = 0;

    void process(const AnnotatedBatch& input,
                OutputCollector& out, CallContext& ctx) override {
        exchange(input, out, ctx);
    }
};

// Stream result from a factory function (mirrors Python's Stream)
struct Stream {
    std::shared_ptr<arrow::Schema> output_schema;
    std::shared_ptr<arrow::Schema> input_schema;  // empty_schema() for producer
    std::shared_ptr<StreamState> state;
    std::shared_ptr<arrow::RecordBatch> header;  // nullptr if no header
};

// Factory function types
using ProducerFactory = std::function<Stream(const Request&, CallContext&)>;
using ExchangeFactory = std::function<Stream(const Request&, CallContext&)>;

}  // namespace vgi_rpc
