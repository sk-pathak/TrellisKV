#include "trelliskv/network_manager.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>

#include "trelliskv/json_serializer.h"
#include "trelliskv/logger.h"
#include "trelliskv/storage_engine.h"

namespace trelliskv {

NetworkManager::NetworkManager() {
    thread_pool_ = std::make_unique<ThreadPool>(4);
}

NetworkManager::~NetworkManager() { stop(); }

bool NetworkManager::start(uint16_t port, StorageEngine* storage,
                           const std::string& node_id) {
    if (running_) return true;

    storage_ = storage;
    node_id_ = node_id;
    if (!storage_) {
        LOG_ERROR("Cannot start NetworkManager without StorageEngine");
        return false;
    }

    running_ = true;
    accept_thread_ = std::thread([this, port]() { accept_loop(port); });
    return true;
}

void NetworkManager::stop() {
    if (!running_) return;
    running_ = false;

    close_socket(listen_fd_);

    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }

    if (thread_pool_) {
        thread_pool_.reset();
    }
}

void NetworkManager::close_socket(int& fd) {
    if (fd >= 0) {
        ::shutdown(fd, SHUT_RDWR);
        ::close(fd);
        fd = -1;
    }
}

void NetworkManager::accept_loop(uint16_t port) {
    // Create socket
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        LOG_ERROR("Failed to create socket: " + std::string(strerror(errno)));
        running_ = false;
        return;
    }

    int opt = 1;
    if (::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) <
        0) {
        LOG_WARN("Failed to set SO_REUSEADDR: " + std::string(strerror(errno)));
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) <
        0) {
        LOG_ERROR("bind() failed");
        ::close(listen_fd_);
        listen_fd_ = -1;
        running_ = false;
        return;
    }

    if (::listen(listen_fd_, 16) < 0) {
        LOG_ERROR("bind() failed on port " + std::to_string(port) + ": " +
                  std::string(strerror(errno)));
        close_socket(listen_fd_);
        running_ = false;
        return;
    }

    LOG_INFO("NetworkManager listening on port " + std::to_string(port));

    while (running_) {
        sockaddr_in client_addr{};
        socklen_t len = sizeof(client_addr);
        int client_fd = ::accept(
            listen_fd_, reinterpret_cast<sockaddr*>(&client_addr), &len);
        if (client_fd < 0) {
            if (!running_) {
                break;
            }

            if (errno == EINTR) {
                LOG_DEBUG("accept() interrupted, retrying");
                continue;
            } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                continue;
            } else {
                LOG_ERROR("accept() failed: " + std::string(strerror(errno)));
                continue;
            }
        }

        thread_pool_->submit([this, client_fd]() { handle_client(client_fd); });
    }
}

void NetworkManager::handle_client(int client_fd) {
    char buffer[4096];
    std::memset(buffer, 0, sizeof(buffer));

    ssize_t n = ::recv(client_fd, buffer, sizeof(buffer), 0);
    if (n <= 0) {
        if (n < 0) {
            LOG_DEBUG("recv() error: " + std::string(strerror(errno)));
        }
        close_socket(client_fd);
        return;
    }

    std::string request_json(buffer, buffer + n);

    std::string response_json = process_request(request_json);

    if (!response_json.empty()) {
        ssize_t sent =
            ::send(client_fd, response_json.data(), response_json.size(), 0);
        if (sent < 0) {
            LOG_DEBUG("send() error: " + std::string(strerror(errno)));
        }
    }

    close_socket(client_fd);
}

std::string NetworkManager::process_request(const std::string& request_json) {
    try {
        Request req = JsonSerializer::deserialize_request(request_json);

        Response resp;

        switch (req.type) {
            case RequestType::GET: {
                if (storage_->contains(req.key)) {
                    Result<VersionedValue> val = storage_->get(req.key);
                    resp.success = true;
                    resp.value = val.value().value;
                } else {
                    resp.success = false;
                    resp.error = "Key not found";
                }
                break;
            }

            case RequestType::PUT: {
                VersionedValue versioned_value(req.value, node_id_);
                storage_->put(req.key, versioned_value);
                resp.success = true;
                break;
            }

            case RequestType::DELETE_KEY: {
                if (storage_->contains(req.key)) {
                    storage_->remove(req.key);
                    resp.success = true;
                } else {
                    resp.success = false;
                    resp.error = "Key not found";
                }
                break;
            }

            default:
                resp.success = false;
                resp.error = "Unknown request type";
                break;
        }

        return JsonSerializer::serialize(resp);

    } catch (const std::exception& e) {
        Response error_resp;
        error_resp.success = false;
        error_resp.error = std::string("Error: ") + e.what();
        return JsonSerializer::serialize(error_resp);
    }
}

}  // namespace trelliskv
