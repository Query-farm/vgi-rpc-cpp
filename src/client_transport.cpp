// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

#include "vgi_rpc/client.h"

#include "vgi_rpc/wire.h"

#include <arrow/status.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <functional>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>

#ifndef _WIN32
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <signal.h>
#include <spawn.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

extern char** environ;
#else
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
// winsock2.h/afunix.h after windows.h is safe here because
// WIN32_LEAN_AND_MEAN above already excludes windows.h's own legacy
// winsock.h. Needed for connect_unix's real Windows implementation
// (genuine AF_UNIX, matching the canonical vgi_rpc Python launcher's
// behavior on Windows - see vgi-sqlite's launcher.h for the research).
#include <winsock2.h>

#include <afunix.h>
#include <fcntl.h>
#include <io.h>
#endif

namespace vgi_rpc {

namespace {

void require_ok(const arrow::Status& status, const char* operation) {
    if (!status.ok()) throw std::runtime_error(std::string(operation) + ": " + status.ToString());
}

#ifndef _WIN32

void close_fd(int& fd) noexcept {
    if (fd >= 0) {
        while (::close(fd) < 0 && errno == EINTR) {
        }
        fd = -1;
    }
}

void set_cloexec(int fd) {
    const int flags = ::fcntl(fd, F_GETFD);
    if (flags < 0 || ::fcntl(fd, F_SETFD, flags | FD_CLOEXEC) < 0) {
        throw std::runtime_error(std::string("cannot set close-on-exec: ") + std::strerror(errno));
    }
}

void make_pipe(int fds[2]) {
    if (::pipe(fds) < 0) {
        throw std::runtime_error(std::string("cannot create subprocess pipe: ") +
                                 std::strerror(errno));
    }
    try {
        set_cloexec(fds[0]);
        set_cloexec(fds[1]);
    } catch (...) {
        close_fd(fds[0]);
        close_fd(fds[1]);
        throw;
    }
}

bool wait_until(pid_t pid, std::chrono::steady_clock::time_point deadline) noexcept {
    while (true) {
        int status = 0;
        const pid_t result = ::waitpid(pid, &status, WNOHANG);
        if (result == pid || (result < 0 && errno == ECHILD)) return true;
        if (result < 0 && errno != EINTR) return false;
        if (std::chrono::steady_clock::now() >= deadline) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
}

struct ChildState {
    pid_t pid = -1;
    int stdin_fd = -1;
    int stdout_fd = -1;
    std::chrono::milliseconds close_grace{2000};
    std::chrono::milliseconds terminate_grace{2000};

    void close() noexcept {
        // Closing stdin is the cooperative shutdown request. Keep stdout open
        // during the grace period so a worker finishing its final flush is not
        // killed merely because its pipe filled.
        close_fd(stdin_fd);
        if (pid <= 0) {
            close_fd(stdout_fd);
            return;
        }

        if (!wait_until(pid, std::chrono::steady_clock::now() + close_grace)) {
            (void)::kill(pid, SIGTERM);
            if (!wait_until(pid, std::chrono::steady_clock::now() + terminate_grace)) {
                (void)::kill(pid, SIGKILL);
                while (::waitpid(pid, nullptr, 0) < 0 && errno == EINTR) {
                }
            }
        }
        pid = -1;
        close_fd(stdout_fd);
    }
};

void set_socket_timeout(int fd, int option, std::optional<std::chrono::milliseconds> timeout) {
    if (!timeout) return;
    if (*timeout <= std::chrono::milliseconds::zero()) {
        throw std::invalid_argument("socket I/O timeout must be positive");
    }
    const auto micros = std::chrono::duration_cast<std::chrono::microseconds>(*timeout).count();
    timeval value{};
    value.tv_sec = static_cast<decltype(value.tv_sec)>(micros / 1000000);
    value.tv_usec = static_cast<decltype(value.tv_usec)>(micros % 1000000);
    if (::setsockopt(fd, SOL_SOCKET, option, &value, sizeof(value)) < 0) {
        throw std::runtime_error(std::string("cannot set socket I/O timeout: ") +
                                 std::strerror(errno));
    }
}

bool connect_with_timeout(int fd, const sockaddr* address, socklen_t address_length,
                          std::optional<std::chrono::milliseconds> timeout, int* out_error) {
    if (!timeout) {
        if (::connect(fd, address, address_length) == 0) return true;
        *out_error = errno;
        return false;
    }
    if (*timeout <= std::chrono::milliseconds::zero()) {
        throw std::invalid_argument("socket connect timeout must be positive");
    }
    const int original_flags = ::fcntl(fd, F_GETFL);
    if (original_flags < 0 || ::fcntl(fd, F_SETFL, original_flags | O_NONBLOCK) < 0) {
        throw std::runtime_error(std::string("cannot make connecting socket nonblocking: ") +
                                 std::strerror(errno));
    }
    if (::connect(fd, address, address_length) == 0) {
        (void)::fcntl(fd, F_SETFL, original_flags);
        return true;
    }
    if (errno != EINPROGRESS) {
        *out_error = errno;
        (void)::fcntl(fd, F_SETFL, original_flags);
        return false;
    }

    const auto deadline = std::chrono::steady_clock::now() + *timeout;
    int poll_result = 0;
    do {
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
            deadline - std::chrono::steady_clock::now());
        if (remaining <= std::chrono::milliseconds::zero()) {
            *out_error = ETIMEDOUT;
            (void)::fcntl(fd, F_SETFL, original_flags);
            return false;
        }
        pollfd descriptor{fd, POLLOUT, 0};
        const auto bounded = std::min<int64_t>(remaining.count(), std::numeric_limits<int>::max());
        poll_result = ::poll(&descriptor, 1, static_cast<int>(bounded));
    } while (poll_result < 0 && errno == EINTR);

    int socket_error = 0;
    socklen_t error_size = sizeof(socket_error);
    if (poll_result <= 0 ||
        ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &socket_error, &error_size) < 0 ||
        socket_error != 0) {
        *out_error = poll_result == 0 ? ETIMEDOUT : (socket_error != 0 ? socket_error : errno);
        (void)::fcntl(fd, F_SETFL, original_flags);
        return false;
    }
    if (::fcntl(fd, F_SETFL, original_flags) < 0) {
        throw std::runtime_error(std::string("cannot restore connected socket flags: ") +
                                 std::strerror(errno));
    }
    return true;
}

int connect_tcp_fd(const std::string& host, uint16_t port, const SocketTransportOptions& options) {
    if (host.empty()) throw std::invalid_argument("TCP host must not be empty");

    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    addrinfo* addresses = nullptr;
    const std::string service = std::to_string(port);
    const int lookup = ::getaddrinfo(host.c_str(), service.c_str(), &hints, &addresses);
    if (lookup != 0) {
        throw std::runtime_error("cannot resolve TCP host '" + host +
                                 "': " + ::gai_strerror(lookup));
    }

    int fd = -1;
    int last_error = 0;
    for (const addrinfo* address = addresses; address; address = address->ai_next) {
        fd = ::socket(address->ai_family, address->ai_socktype, address->ai_protocol);
        if (fd < 0) {
            last_error = errno;
            continue;
        }
        if (connect_with_timeout(fd, address->ai_addr, address->ai_addrlen, options.connect_timeout,
                                 &last_error)) {
            break;
        }
        close_fd(fd);
    }
    ::freeaddrinfo(addresses);
    if (fd < 0) {
        throw std::runtime_error("cannot connect TCP socket " + host + ":" + std::to_string(port) +
                                 ": " + std::strerror(last_error));
    }

    try {
        set_cloexec(fd);
        set_socket_timeout(fd, SO_RCVTIMEO, options.read_timeout);
        set_socket_timeout(fd, SO_SNDTIMEO, options.write_timeout);
    } catch (...) {
        close_fd(fd);
        throw;
    }
    const int one = 1;
    (void)::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    return fd;
}

int connect_unix_fd(const std::string& path, const SocketTransportOptions& options) {
    if (path.empty()) throw std::invalid_argument("Unix socket path must not be empty");
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    if (path.size() >= sizeof(address.sun_path)) {
        throw std::invalid_argument("Unix socket path is too long: " + path);
    }
    std::memcpy(address.sun_path, path.c_str(), path.size() + 1);

    int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        throw std::runtime_error(std::string("cannot create Unix socket: ") + std::strerror(errno));
    }
    try {
        set_cloexec(fd);
        int connect_error = 0;
        if (!connect_with_timeout(fd, reinterpret_cast<const sockaddr*>(&address), sizeof(address),
                                  options.connect_timeout, &connect_error)) {
            throw std::runtime_error("cannot connect Unix socket " + path + ": " +
                                     std::strerror(connect_error));
        }
        set_socket_timeout(fd, SO_RCVTIMEO, options.read_timeout);
        set_socket_timeout(fd, SO_SNDTIMEO, options.write_timeout);
    } catch (...) {
        close_fd(fd);
        throw;
    }
    return fd;
}

#endif

#ifdef _WIN32

void close_handle(HANDLE& handle) noexcept {
    if (handle && handle != INVALID_HANDLE_VALUE) {
        (void)::CloseHandle(handle);
        handle = nullptr;
    }
}

std::wstring utf8_to_wide(const std::string& value) {
    if (value.empty()) return {};
    const int size = ::MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, value.data(),
                                           static_cast<int>(value.size()), nullptr, 0);
    if (size <= 0) throw std::invalid_argument("subprocess argv is not valid UTF-8");
    std::wstring result(static_cast<size_t>(size), L'\0');
    if (::MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, value.data(),
                              static_cast<int>(value.size()), result.data(), size) != size) {
        throw std::runtime_error("cannot convert subprocess argv to UTF-16");
    }
    return result;
}

std::wstring quote_windows_argument(const std::wstring& argument) {
    if (!argument.empty() && argument.find_first_of(L" \t\n\v\"") == std::wstring::npos) {
        return argument;
    }
    std::wstring quoted(1, L'\"');
    size_t backslashes = 0;
    for (const wchar_t character : argument) {
        if (character == L'\\') {
            ++backslashes;
        } else if (character == L'\"') {
            quoted.append(backslashes * 2 + 1, L'\\');
            quoted.push_back(L'\"');
            backslashes = 0;
        } else {
            quoted.append(backslashes, L'\\');
            backslashes = 0;
            quoted.push_back(character);
        }
    }
    quoted.append(backslashes * 2, L'\\');
    quoted.push_back(L'\"');
    return quoted;
}

std::wstring windows_command_line(const std::vector<std::string>& argv) {
    std::wstring result;
    for (size_t i = 0; i < argv.size(); ++i) {
        if (i > 0) result.push_back(L' ');
        result += quote_windows_argument(utf8_to_wide(argv[i]));
    }
    return result;
}

DWORD wait_milliseconds(std::chrono::milliseconds duration) noexcept {
    if (duration <= std::chrono::milliseconds::zero()) return 0;
    const auto count = duration.count();
    return count >= static_cast<int64_t>(INFINITE - 1) ? INFINITE - 1 : static_cast<DWORD>(count);
}

// Winsock must be initialized once per process before any socket() call.
// Reference-counted by the OS, so a redundant call from elsewhere in the
// process (e.g. vgi-sqlite's own launcher.cpp, which needs Winsock too for
// its own AF_UNIX liveness probe) is harmless - deliberately never paired
// with WSACleanup, since a library can't know whether something else in
// the process still needs sockets when it's done with its own.
void ensure_winsock_initialized() {
    static bool inited = [] {
        WSADATA data;
        const int rc = ::WSAStartup(MAKEWORD(2, 2), &data);
        if (rc != 0) {
            throw std::runtime_error("cannot initialize Winsock (WSAStartup returned " +
                                     std::to_string(rc) + ")");
        }
        return true;
    }();
    (void)inited;
}

// The Windows counterpart to set_socket_timeout (POSIX branch, above) -
// SO_RCVTIMEO/SO_SNDTIMEO take a DWORD count of milliseconds directly on
// Windows, not a timeval struct.
void set_socket_timeout_win(SOCKET sock, int option, std::optional<std::chrono::milliseconds> timeout) {
    if (!timeout) return;
    if (*timeout <= std::chrono::milliseconds::zero()) {
        throw std::invalid_argument("socket I/O timeout must be positive");
    }
    const DWORD millis = static_cast<DWORD>(timeout->count());
    if (::setsockopt(sock, SOL_SOCKET, option, reinterpret_cast<const char*>(&millis), sizeof(millis)) ==
        SOCKET_ERROR) {
        throw std::runtime_error("cannot set socket I/O timeout (WSAGetLastError=" +
                                 std::to_string(::WSAGetLastError()) + ")");
    }
}

// The Windows counterpart to connect_with_timeout (POSIX branch, above) -
// same shape (nonblocking connect, then wait for writability with a
// deadline), but ioctlsocket(FIONBIO)/WSAPoll/WSAEWOULDBLOCK/
// WSAGetLastError instead of fcntl/poll/EINPROGRESS/errno. WSAPoll (Vista+)
// is deliberately used over select() - it mirrors POSIX poll()'s pollfd
// shape almost exactly, keeping this close to its POSIX counterpart
// instead of diverging into select()'s fd_set-based API.
bool connect_with_timeout_win(SOCKET sock, const sockaddr* address, int address_length,
                              std::optional<std::chrono::milliseconds> timeout, int* out_error) {
    if (!timeout) {
        if (::connect(sock, address, address_length) == 0) return true;
        *out_error = ::WSAGetLastError();
        return false;
    }
    if (*timeout <= std::chrono::milliseconds::zero()) {
        throw std::invalid_argument("socket connect timeout must be positive");
    }
    u_long mode = 1;
    if (::ioctlsocket(sock, FIONBIO, &mode) != 0) {
        throw std::runtime_error("cannot make connecting socket nonblocking (WSAGetLastError=" +
                                 std::to_string(::WSAGetLastError()) + ")");
    }
    if (::connect(sock, address, address_length) == 0) {
        mode = 0;
        (void)::ioctlsocket(sock, FIONBIO, &mode);
        return true;
    }
    int err = ::WSAGetLastError();
    if (err != WSAEWOULDBLOCK) {
        *out_error = err;
        mode = 0;
        (void)::ioctlsocket(sock, FIONBIO, &mode);
        return false;
    }

    const auto deadline = std::chrono::steady_clock::now() + *timeout;
    WSAPOLLFD descriptor{};
    descriptor.fd = sock;
    descriptor.events = POLLOUT;
    int poll_result = 0;
    do {
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
            deadline - std::chrono::steady_clock::now());
        if (remaining <= std::chrono::milliseconds::zero()) {
            *out_error = WSAETIMEDOUT;
            mode = 0;
            (void)::ioctlsocket(sock, FIONBIO, &mode);
            return false;
        }
        const auto bounded = std::min<int64_t>(remaining.count(), std::numeric_limits<int>::max());
        poll_result = ::WSAPoll(&descriptor, 1, static_cast<int>(bounded));
    } while (poll_result == SOCKET_ERROR && ::WSAGetLastError() == WSAEINTR);

    int socket_error = 0;
    int error_size = sizeof(socket_error);
    if (poll_result <= 0 ||
        ::getsockopt(sock, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&socket_error), &error_size) ==
            SOCKET_ERROR ||
        socket_error != 0) {
        *out_error = poll_result == 0 ? WSAETIMEDOUT : (socket_error != 0 ? socket_error : ::WSAGetLastError());
        mode = 0;
        (void)::ioctlsocket(sock, FIONBIO, &mode);
        return false;
    }
    mode = 0;
    if (::ioctlsocket(sock, FIONBIO, &mode) != 0) {
        throw std::runtime_error("cannot restore connected socket to blocking mode (WSAGetLastError=" +
                                 std::to_string(::WSAGetLastError()) + ")");
    }
    return true;
}

// The Windows counterpart to connect_unix_fd (POSIX branch, above) - real
// AF_UNIX (afunix.h), not a named pipe. See vgi-sqlite's launcher.h for
// why AF_UNIX is the right Windows transport here: it's what the
// canonical vgi_rpc Python launcher actually uses on Windows, confirmed
// directly in its source, not a named pipe as vgi (the DuckDB C++
// extension)'s own Windows launcher uses.
SOCKET connect_unix_socket(const std::string& path, const SocketTransportOptions& options) {
    ensure_winsock_initialized();
    if (path.empty()) throw std::invalid_argument("Unix socket path must not be empty");
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    if (path.size() >= sizeof(address.sun_path)) {
        throw std::invalid_argument("Unix socket path is too long: " + path);
    }
    std::memcpy(address.sun_path, path.c_str(), path.size() + 1);

    SOCKET sock = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock == INVALID_SOCKET) {
        throw std::runtime_error("cannot create Unix socket (WSAGetLastError=" +
                                 std::to_string(::WSAGetLastError()) + ")");
    }
    try {
        int connect_error = 0;
        if (!connect_with_timeout_win(sock, reinterpret_cast<const sockaddr*>(&address), sizeof(address),
                                      options.connect_timeout, &connect_error)) {
            throw std::runtime_error("cannot connect Unix socket " + path + " (WSAGetLastError=" +
                                     std::to_string(connect_error) + ")");
        }
        set_socket_timeout_win(sock, SO_RCVTIMEO, options.read_timeout);
        set_socket_timeout_win(sock, SO_SNDTIMEO, options.write_timeout);
    } catch (...) {
        ::closesocket(sock);
        throw;
    }
    return sock;
}

// Opens an existing Windows named pipe (\\.\pipe\<name>) as a client and
// converts the resulting HANDLE into a CRT file descriptor via
// _open_osfhandle - the exact same bridge WindowsChildState (above) already
// uses for a spawned child's own stdin/stdout HANDLEs, letting this reuse
// FdInputStream/FdOutputStream directly instead of needing yet another
// stream-class pair (unlike AF_UNIX's SOCKET, a named-pipe HANDLE opened
// GENERIC_READ|GENERIC_WRITE is _read()/_write()-compatible once bridged
// this way). One handle serves both directions, same as a socket fd - no
// separate read/write handles needed.
int connect_pipe_fd(const std::string& pipe_name, const SocketTransportOptions& options) {
    if (pipe_name.empty()) throw std::invalid_argument("pipe name must not be empty");
    const auto deadline = options.connect_timeout
                              ? std::chrono::steady_clock::now() + *options.connect_timeout
                              : std::chrono::steady_clock::time_point::max();
    HANDLE handle = INVALID_HANDLE_VALUE;
    for (;;) {
        handle = ::CreateFileA(pipe_name.c_str(), GENERIC_READ | GENERIC_WRITE, 0, nullptr,
                               OPEN_EXISTING, 0, nullptr);
        if (handle != INVALID_HANDLE_VALUE) break;
        const DWORD err = ::GetLastError();
        if (err != ERROR_PIPE_BUSY) {
            throw std::runtime_error("cannot connect named pipe " + pipe_name +
                                     " (GetLastError=" + std::to_string(err) + ")");
        }
        // Every instance is momentarily busy - wait for one to free up, per
        // the standard Windows named-pipe client retry pattern (the server
        // side creates with PIPE_UNLIMITED_INSTANCES per docs/launcher-
        // protocol.md, so this is just a transient race, not a capacity
        // limit).
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            throw std::runtime_error("timed out connecting named pipe " + pipe_name +
                                     " (all instances busy)");
        }
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
        const DWORD wait_ms = options.connect_timeout
                                  ? static_cast<DWORD>(std::min<int64_t>(remaining.count(),
                                                                          std::numeric_limits<int32_t>::max()))
                                  : NMPWAIT_WAIT_FOREVER;
        if (!::WaitNamedPipeA(pipe_name.c_str(), wait_ms)) {
            throw std::runtime_error("cannot connect named pipe " + pipe_name +
                                     " (WaitNamedPipe GetLastError=" +
                                     std::to_string(::GetLastError()) + ")");
        }
    }
    const int fd = ::_open_osfhandle(reinterpret_cast<intptr_t>(handle), _O_BINARY);
    if (fd < 0) {
        close_handle(handle);
        throw std::runtime_error("cannot convert named pipe handle to a file descriptor");
    }
    return fd;
}

struct WindowsChildState {
    HANDLE process = nullptr;
    int stdin_fd = -1;
    int stdout_fd = -1;
    std::chrono::milliseconds close_grace{2000};
    std::chrono::milliseconds terminate_grace{2000};

    void close() noexcept {
        if (stdin_fd >= 0) {
            (void)::_close(stdin_fd);
            stdin_fd = -1;
        }
        if (!process) {
            if (stdout_fd >= 0) (void)::_close(stdout_fd);
            stdout_fd = -1;
            return;
        }
        if (::WaitForSingleObject(process, wait_milliseconds(close_grace)) == WAIT_TIMEOUT) {
            // Windows has no safe SIGTERM equivalent for a non-console child.
            // TerminateProcess is the escalation; the second bounded wait is
            // solely for kernel teardown before the process handle is reaped.
            (void)::TerminateProcess(process, 1);
            (void)::WaitForSingleObject(process, wait_milliseconds(terminate_grace));
        }
        close_handle(process);
        if (stdout_fd >= 0) (void)::_close(stdout_fd);
        stdout_fd = -1;
    }
};

#endif

}  // namespace

class ClientTransport::Impl {
public:
    Impl(std::shared_ptr<arrow::io::InputStream> input,
         std::shared_ptr<arrow::io::OutputStream> output, std::function<void()> close_fn)
        : input(std::move(input)), output(std::move(output)), close_fn(std::move(close_fn)) {}

    ~Impl() {
        try {
            close();
        } catch (...) {
        }
    }

    void close() {
        std::lock_guard<std::mutex> lock(mutex);
        if (closed) return;
        closed = true;
        close_fn();
    }

    std::shared_ptr<arrow::io::InputStream> input;
    std::shared_ptr<arrow::io::OutputStream> output;
    std::function<void()> close_fn;
    mutable std::mutex mutex;
    bool closed = false;
};

ClientTransport::ClientTransport(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

ClientTransport::~ClientTransport() = default;
ClientTransport::ClientTransport(ClientTransport&&) noexcept = default;
ClientTransport& ClientTransport::operator=(ClientTransport&&) noexcept = default;

ClientTransport ClientTransport::from_streams(std::shared_ptr<arrow::io::InputStream> input,
                                              std::shared_ptr<arrow::io::OutputStream> output) {
    if (!input || !output) throw std::invalid_argument("client transport streams must not be null");
    auto close_fn = [input, output]() {
        // Close the write side first to signal EOF to a peer that is waiting
        // for another request, then release the read side.
        const auto output_status = output->Close();
        const auto input_status = input->Close();
        require_ok(output_status, "close client transport output");
        require_ok(input_status, "close client transport input");
    };
    return ClientTransport(
        std::make_unique<Impl>(std::move(input), std::move(output), std::move(close_fn)));
}

ClientTransport ClientTransport::spawn(const std::vector<std::string>& argv,
                                       const SubprocessTransportOptions& options) {
    if (argv.empty() || argv.front().empty()) {
        throw std::invalid_argument("subprocess argv must name a program");
    }
    for (const auto& argument : argv) {
        if (argument.find('\0') != std::string::npos) {
            throw std::invalid_argument("subprocess argv must not contain NUL bytes");
        }
    }
    if (options.close_grace < std::chrono::milliseconds::zero() ||
        options.terminate_grace < std::chrono::milliseconds::zero()) {
        throw std::invalid_argument("subprocess shutdown grace periods must not be negative");
    }
#ifdef _WIN32
    SECURITY_ATTRIBUTES security{};
    security.nLength = sizeof(security);
    security.bInheritHandle = TRUE;
    HANDLE child_stdin = nullptr;
    HANDLE parent_stdin = nullptr;
    HANDLE parent_stdout = nullptr;
    HANDLE child_stdout = nullptr;
    HANDLE null_stderr = nullptr;
    if (!::CreatePipe(&child_stdin, &parent_stdin, &security, 0) ||
        !::SetHandleInformation(parent_stdin, HANDLE_FLAG_INHERIT, 0) ||
        !::CreatePipe(&parent_stdout, &child_stdout, &security, 0) ||
        !::SetHandleInformation(parent_stdout, HANDLE_FLAG_INHERIT, 0)) {
        close_handle(child_stdin);
        close_handle(parent_stdin);
        close_handle(parent_stdout);
        close_handle(child_stdout);
        throw std::runtime_error("cannot create inheritable subprocess pipes");
    }

    STARTUPINFOW startup{};
    startup.cb = sizeof(startup);
    startup.dwFlags = STARTF_USESTDHANDLES;
    startup.hStdInput = child_stdin;
    startup.hStdOutput = child_stdout;
    startup.hStdError = ::GetStdHandle(STD_ERROR_HANDLE);
    if (options.stderr_mode == ClientStderrMode::DISCARD) {
        null_stderr = ::CreateFileW(L"NUL", GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE,
                                    &security, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
        if (null_stderr == INVALID_HANDLE_VALUE) {
            null_stderr = nullptr;
            close_handle(child_stdin);
            close_handle(parent_stdin);
            close_handle(parent_stdout);
            close_handle(child_stdout);
            throw std::runtime_error("cannot open NUL for child stderr");
        }
        startup.hStdError = null_stderr;
    }

    std::wstring command_line = windows_command_line(argv);
    command_line.push_back(L'\0');
    PROCESS_INFORMATION process_info{};
    const BOOL created = ::CreateProcessW(nullptr, command_line.data(), nullptr, nullptr, TRUE, 0,
                                          nullptr, nullptr, &startup, &process_info);
    close_handle(child_stdin);
    close_handle(child_stdout);
    close_handle(null_stderr);
    if (!created) {
        close_handle(parent_stdin);
        close_handle(parent_stdout);
        throw std::runtime_error("cannot spawn subprocess (CreateProcessW error " +
                                 std::to_string(::GetLastError()) + ")");
    }
    close_handle(process_info.hThread);

    const int stdin_fd = ::_open_osfhandle(reinterpret_cast<intptr_t>(parent_stdin), _O_BINARY);
    if (stdin_fd < 0) {
        close_handle(parent_stdin);
        close_handle(parent_stdout);
        (void)::TerminateProcess(process_info.hProcess, 1);
        (void)::WaitForSingleObject(process_info.hProcess, 1000);
        close_handle(process_info.hProcess);
        throw std::runtime_error("cannot convert child stdin handle to binary file descriptor");
    }
    parent_stdin = nullptr;  // owned by stdin_fd
    const int stdout_fd = ::_open_osfhandle(reinterpret_cast<intptr_t>(parent_stdout), _O_BINARY);
    if (stdout_fd < 0) {
        (void)::_close(stdin_fd);
        close_handle(parent_stdout);
        (void)::TerminateProcess(process_info.hProcess, 1);
        (void)::WaitForSingleObject(process_info.hProcess, 1000);
        close_handle(process_info.hProcess);
        throw std::runtime_error("cannot convert child stdout handle to binary file descriptor");
    }
    parent_stdout = nullptr;  // owned by stdout_fd

    auto child = std::make_shared<WindowsChildState>();
    child->process = process_info.hProcess;
    child->stdin_fd = stdin_fd;
    child->stdout_fd = stdout_fd;
    child->close_grace = options.close_grace;
    child->terminate_grace = options.terminate_grace;
    auto input = std::make_shared<FdInputStream>(stdout_fd);
    auto output = std::make_shared<FdOutputStream>(stdin_fd);
    return ClientTransport(std::make_unique<Impl>(input, output, [child]() { child->close(); }));
#else
    int child_stdin[2] = {-1, -1};
    int child_stdout[2] = {-1, -1};
    make_pipe(child_stdin);
    try {
        make_pipe(child_stdout);
    } catch (...) {
        close_fd(child_stdin[0]);
        close_fd(child_stdin[1]);
        throw;
    }

    posix_spawn_file_actions_t actions;
    const int actions_result = ::posix_spawn_file_actions_init(&actions);
    if (actions_result != 0) {
        close_fd(child_stdin[0]);
        close_fd(child_stdin[1]);
        close_fd(child_stdout[0]);
        close_fd(child_stdout[1]);
        throw std::runtime_error("cannot initialize subprocess file actions: " +
                                 std::string(std::strerror(actions_result)));
    }

    int null_fd = -1;
    auto destroy_actions = [&]() { (void)::posix_spawn_file_actions_destroy(&actions); };
    auto add_action = [&](int result, const char* operation) {
        if (result != 0) {
            throw std::runtime_error(std::string(operation) + ": " + std::strerror(result));
        }
    };

    pid_t pid = -1;
    try {
        add_action(::posix_spawn_file_actions_adddup2(&actions, child_stdin[0], STDIN_FILENO),
                   "cannot route child stdin");
        add_action(::posix_spawn_file_actions_adddup2(&actions, child_stdout[1], STDOUT_FILENO),
                   "cannot route child stdout");
        add_action(::posix_spawn_file_actions_addclose(&actions, child_stdin[1]),
                   "cannot close child stdin writer");
        add_action(::posix_spawn_file_actions_addclose(&actions, child_stdout[0]),
                   "cannot close child stdout reader");

        if (options.stderr_mode == ClientStderrMode::DISCARD) {
            null_fd = ::open("/dev/null", O_WRONLY | O_CLOEXEC);
            if (null_fd < 0) {
                throw std::runtime_error(std::string("cannot open /dev/null: ") +
                                         std::strerror(errno));
            }
            add_action(::posix_spawn_file_actions_adddup2(&actions, null_fd, STDERR_FILENO),
                       "cannot route child stderr");
        }

        std::vector<char*> raw_argv;
        raw_argv.reserve(argv.size() + 1);
        for (const auto& argument : argv) raw_argv.push_back(const_cast<char*>(argument.c_str()));
        raw_argv.push_back(nullptr);
        const int spawn_result =
            ::posix_spawnp(&pid, raw_argv[0], &actions, nullptr, raw_argv.data(), environ);
        if (spawn_result != 0) {
            throw std::runtime_error("cannot spawn '" + argv.front() +
                                     "': " + std::strerror(spawn_result));
        }
    } catch (...) {
        destroy_actions();
        close_fd(null_fd);
        close_fd(child_stdin[0]);
        close_fd(child_stdin[1]);
        close_fd(child_stdout[0]);
        close_fd(child_stdout[1]);
        throw;
    }
    destroy_actions();
    close_fd(null_fd);
    close_fd(child_stdin[0]);
    close_fd(child_stdout[1]);

    auto child = std::make_shared<ChildState>();
    child->pid = pid;
    child->stdin_fd = child_stdin[1];
    child->stdout_fd = child_stdout[0];
    child->close_grace = options.close_grace;
    child->terminate_grace = options.terminate_grace;

    auto input = std::make_shared<FdInputStream>(child->stdout_fd);
    auto output = std::make_shared<FdOutputStream>(child->stdin_fd);
    return ClientTransport(std::make_unique<Impl>(input, output, [child]() { child->close(); }));
#endif
}

ClientTransport ClientTransport::connect_unix(const std::string& path,
                                              const SocketTransportOptions& options) {
#ifdef _WIN32
    auto sock = std::make_shared<SOCKET>(connect_unix_socket(path, options));
    auto input = std::make_shared<SocketInputStream>(static_cast<std::uintptr_t>(*sock));
    auto output = std::make_shared<SocketOutputStream>(static_cast<std::uintptr_t>(*sock));
    return ClientTransport(std::make_unique<Impl>(input, output, [sock]() {
        if (*sock != INVALID_SOCKET) {
            (void)::shutdown(*sock, SD_BOTH);
            ::closesocket(*sock);
            *sock = INVALID_SOCKET;
        }
    }));
#else
    auto fd = std::make_shared<int>(connect_unix_fd(path, options));
    auto input = std::make_shared<FdInputStream>(*fd);
    auto output = std::make_shared<FdOutputStream>(*fd);
    return ClientTransport(std::make_unique<Impl>(input, output, [fd]() {
        if (*fd >= 0) (void)::shutdown(*fd, SHUT_RDWR);
        close_fd(*fd);
    }));
#endif
}

ClientTransport ClientTransport::connect_pipe(const std::string& pipe_name,
                                              const SocketTransportOptions& options) {
#ifdef _WIN32
    auto fd = std::make_shared<int>(connect_pipe_fd(pipe_name, options));
    auto input = std::make_shared<FdInputStream>(*fd);
    auto output = std::make_shared<FdOutputStream>(*fd);
    return ClientTransport(std::make_unique<Impl>(input, output, [fd]() {
        if (*fd >= 0) (void)::_close(*fd);
        *fd = -1;
    }));
#else
    (void)pipe_name;
    (void)options;
    throw std::runtime_error(
        "Named-pipe client transport is Windows-only - connect_unix (genuine AF_UNIX) "
        "already covers the equivalent need on POSIX");
#endif
}

ClientTransport ClientTransport::connect_tcp(const std::string& host, uint16_t port,
                                             const SocketTransportOptions& options) {
#ifdef _WIN32
    (void)host;
    (void)port;
    (void)options;
    throw std::runtime_error("TCP client transport is not yet available on Windows");
#else
    auto fd = std::make_shared<int>(connect_tcp_fd(host, port, options));
    auto input = std::make_shared<FdInputStream>(*fd);
    auto output = std::make_shared<FdOutputStream>(*fd);
    return ClientTransport(std::make_unique<Impl>(input, output, [fd]() {
        if (*fd >= 0) (void)::shutdown(*fd, SHUT_RDWR);
        close_fd(*fd);
    }));
#endif
}

bool ClientTransport::is_open() const noexcept {
    return impl_ && !impl_->closed;
}

void ClientTransport::close() {
    if (impl_) impl_->close();
}

std::shared_ptr<arrow::io::InputStream> ClientTransport::input() const {
    if (!is_open()) throw std::runtime_error("client transport is closed");
    return impl_->input;
}

std::shared_ptr<arrow::io::OutputStream> ClientTransport::output() const {
    if (!is_open()) throw std::runtime_error("client transport is closed");
    return impl_->output;
}

}  // namespace vgi_rpc
