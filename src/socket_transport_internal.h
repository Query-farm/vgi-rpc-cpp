// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#pragma once

#ifndef _WIN32

#include <arrow/buffer.h>
#include <arrow/io/interfaces.h>
#include <arrow/result.h>
#include <arrow/status.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <deque>
#include <fcntl.h>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <poll.h>
#include <span>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unordered_set>
#include <unistd.h>
#include <utility>
#include <vector>

#include "vgi_rpc/wire.h"

namespace vgi_rpc::socket_detail {

inline void ensure_nonblocking(int fd) {
    int flags;
    do {
        flags = ::fcntl(fd, F_GETFL);
    } while (flags < 0 && errno == EINTR);
    if (flags < 0)
        throw std::runtime_error(std::string("cannot inspect socket flags: ") +
                                 std::strerror(errno));
    if ((flags & O_NONBLOCK) != 0) return;

    int result;
    do {
        result = ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    } while (result < 0 && errno == EINTR);
    if (result < 0)
        throw std::runtime_error(std::string("cannot make deadline socket nonblocking: ") +
                                 std::strerror(errno));
}

inline std::chrono::steady_clock::time_point deadline_after(
    std::chrono::steady_clock::time_point start, std::chrono::milliseconds timeout) noexcept {
    const auto available = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::time_point::max() - start);
    if (timeout >= available) return std::chrono::steady_clock::time_point::max();
    return start + timeout;
}

struct AcceptedPeer {
    std::string address;
    std::string endpoint;
    std::chrono::steady_clock::time_point accepted_at = std::chrono::steady_clock::now();
};

// Socket-only input adapter with an absolute deadline for the complete first
// VGI frame, then a fresh bounded deadline for each subsequent read.  The
// absolute first-frame deadline prevents a slow peer from extending setup by
// sending one byte just before each timeout.
class DeadlineFdInputStream final : public arrow::io::InputStream {
public:
    DeadlineFdInputStream(int fd, std::chrono::steady_clock::time_point first_frame_deadline,
                          std::chrono::milliseconds idle_read_timeout)
        : fd_(fd),
          first_frame_deadline_(first_frame_deadline),
          idle_read_timeout_(idle_read_timeout) {
        ensure_nonblocking(fd_);
    }

    arrow::Status Close() override {
        closed_ = true;
        return arrow::Status::OK();
    }
    bool closed() const override { return closed_; }
    arrow::Result<int64_t> Tell() const override { return position_; }

    arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
        if (closed_) return arrow::Status::IOError("socket input is closed");
        if (nbytes < 0) return arrow::Status::Invalid("negative socket read size");
        const auto deadline =
            first_frame_complete_
                ? deadline_after(std::chrono::steady_clock::now(), idle_read_timeout_)
                : first_frame_deadline_;
        auto* bytes = static_cast<uint8_t*>(out);
        int64_t total = 0;
        while (total < nbytes) {
            const auto now = std::chrono::steady_clock::now();
            if (now >= deadline) return arrow::Status::IOError("socket read deadline exceeded");
            const auto remaining =
                std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
            pollfd descriptor{fd_, POLLIN, 0};
            int ready;
            do {
                ready = ::poll(&descriptor, 1,
                               static_cast<int>(std::clamp<int64_t>(
                                   remaining.count(), 1, std::numeric_limits<int>::max())));
            } while (ready < 0 && errno == EINTR);
            if (ready == 0) return arrow::Status::IOError("socket read deadline exceeded");
            if (ready < 0)
                return arrow::Status::IOError("socket poll failed: ", std::strerror(errno));

            const int64_t chunk = std::min<int64_t>(nbytes - total, kMaxIoChunk);
            ssize_t count;
            do {
                count = ::recv(fd_, bytes + total, static_cast<size_t>(chunk), MSG_DONTWAIT);
            } while (count < 0 && errno == EINTR);
            if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) continue;
            if (count < 0)
                return arrow::Status::IOError("socket read failed: ", std::strerror(errno));
            if (count == 0) break;
            total += count;
        }
        position_ += total;
        return total;
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
        ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes));
        ARROW_ASSIGN_OR_RAISE(const int64_t count, Read(nbytes, buffer->mutable_data()));
        if (count < nbytes) RETURN_NOT_OK(buffer->Resize(count, false));
        return std::shared_ptr<arrow::Buffer>(std::move(buffer));
    }

    void mark_first_frame_complete() noexcept { first_frame_complete_ = true; }

private:
    int fd_;
    std::chrono::steady_clock::time_point first_frame_deadline_;
    std::chrono::milliseconds idle_read_timeout_;
    bool first_frame_complete_ = false;
    bool closed_ = false;
    int64_t position_ = 0;
};

// Output counterpart: one absolute budget for each Arrow Write call. This
// bounds the common stopped-reader case once the peer's receive window fills.
// Long-lived streams remain usable because later writes receive fresh budgets.
class DeadlineFdOutputStream final : public arrow::io::OutputStream {
public:
    DeadlineFdOutputStream(int fd, std::chrono::milliseconds write_timeout)
        : fd_(fd), write_timeout_(write_timeout) {
        ensure_nonblocking(fd_);
    }

    arrow::Status Close() override {
        closed_ = true;
        return arrow::Status::OK();
    }
    bool closed() const override { return closed_; }
    arrow::Result<int64_t> Tell() const override { return position_; }

    arrow::Status Write(const void* data, int64_t nbytes) override {
        if (closed_) return arrow::Status::IOError("socket output is closed");
        if (nbytes < 0) return arrow::Status::Invalid("negative socket write size");
        const auto deadline = deadline_after(std::chrono::steady_clock::now(), write_timeout_);
        const auto* bytes = static_cast<const uint8_t*>(data);
        int64_t written = 0;
        while (written < nbytes) {
            const auto now = std::chrono::steady_clock::now();
            if (now >= deadline) return arrow::Status::IOError("socket write deadline exceeded");
            const auto remaining =
                std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
            pollfd descriptor{fd_, POLLOUT, 0};
            int ready;
            do {
                ready = ::poll(&descriptor, 1,
                               static_cast<int>(std::clamp<int64_t>(
                                   remaining.count(), 1, std::numeric_limits<int>::max())));
            } while (ready < 0 && errno == EINTR);
            if (ready == 0) return arrow::Status::IOError("socket write deadline exceeded");
            if (ready < 0)
                return arrow::Status::IOError("socket poll failed: ", std::strerror(errno));

            const int64_t chunk = std::min<int64_t>(nbytes - written, kMaxIoChunk);
            ssize_t count;
            do {
                count = ::send(fd_, bytes + written, static_cast<size_t>(chunk),
                               MSG_NOSIGNAL | MSG_DONTWAIT);
            } while (count < 0 && errno == EINTR);
            if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) continue;
            if (count < 0)
                return arrow::Status::IOError("socket write failed: ", std::strerror(errno));
            if (count == 0) return arrow::Status::IOError("socket write made no progress");
            written += count;
        }
        position_ += written;
        return arrow::Status::OK();
    }

    arrow::Status Flush() override { return arrow::Status::OK(); }

private:
    int fd_;
    std::chrono::milliseconds write_timeout_;
    bool closed_ = false;
    int64_t position_ = 0;
};

enum class ConnectionFailure { REJECTED, CLOSED_AFTER_ERROR };

// A bounded executor for persistent accepted sockets. submit() never waits:
// at saturation ownership stays with the caller, which can close immediately
// and keep accept() responsive.  The admitted count includes active work and
// its bounded handoff queue.
class SocketConnectionPool {
public:
    using Serve = std::function<void(int, const AcceptedPeer&)>;
    using LogFailure = std::function<void(ConnectionFailure)>;

    SocketConnectionPool(size_t maximum_active, size_t maximum_pending, Serve serve,
                         LogFailure log_failure)
        : serve_(std::move(serve)),
          log_failure_(std::move(log_failure)),
          maximum_admitted_(maximum_active + maximum_pending) {
        if (maximum_active == 0 || maximum_pending == 0 ||
            maximum_active > std::numeric_limits<size_t>::max() - maximum_pending)
            throw std::invalid_argument("socket admission limits must be positive");
        workers_.reserve(maximum_active);
        for (size_t index = 0; index < maximum_active; ++index)
            workers_.emplace_back([this] { worker_loop(); });
    }

    SocketConnectionPool(const SocketConnectionPool&) = delete;
    SocketConnectionPool& operator=(const SocketConnectionPool&) = delete;
    ~SocketConnectionPool() { stop(); }

    bool submit(int fd, AcceptedPeer peer) {
        std::lock_guard lock(mutex_);
        if (stopping_ || admitted_ == maximum_admitted_) return false;
        ++admitted_;
        pending_.push_back({fd, std::move(peer)});
        work_available_.notify_one();
        return true;
    }

    void stop() noexcept {
        {
            std::lock_guard lock(mutex_);
            if (stopping_) return;
            stopping_ = true;
            for (const auto& connection : pending_) ::close(connection.fd);
            admitted_ -= pending_.size();
            pending_.clear();
            // Workers own active descriptors. shutdown() only wakes their
            // reads/writes; they close and release the permit exactly once.
            // Holding the lock avoids an fd-close/reuse race with this walk.
            for (const int fd : active_) (void)::shutdown(fd, SHUT_RDWR);
        }
        work_available_.notify_all();
        for (auto& worker : workers_)
            if (worker.joinable()) worker.join();
    }

private:
    struct Connection {
        int fd = -1;
        AcceptedPeer peer;
    };

    void worker_loop() noexcept {
        while (true) {
            Connection connection;
            {
                std::unique_lock lock(mutex_);
                work_available_.wait(lock, [this] { return stopping_ || !pending_.empty(); });
                if (stopping_ && pending_.empty()) return;
                connection = std::move(pending_.front());
                pending_.pop_front();
                active_.insert(connection.fd);
            }

            try {
                serve_(connection.fd, connection.peer);
            } catch (const std::exception&) {
                if (log_failure_) log_failure_(ConnectionFailure::REJECTED);
            } catch (...) {
                if (log_failure_) log_failure_(ConnectionFailure::REJECTED);
            }

            {
                std::lock_guard lock(mutex_);
                active_.erase(connection.fd);
                ::close(connection.fd);
                --admitted_;
            }
        }
    }

    Serve serve_;
    LogFailure log_failure_;
    size_t maximum_admitted_;
    size_t admitted_ = 0;
    std::mutex mutex_;
    std::condition_variable work_available_;
    std::deque<Connection> pending_;
    std::unordered_set<int> active_;
    bool stopping_ = false;
    std::vector<std::thread> workers_;
};

}  // namespace vgi_rpc::socket_detail

#endif  // !_WIN32
