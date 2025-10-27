#include "trelliskv/network_manager.h"
#include "trelliskv/json_serializer.h"
#include "trelliskv/logger.h"
#include "trelliskv/storage_engine.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <iostream>

namespace trelliskv {

NetworkManager::NetworkManager() {
    thread_pool_ = std::make_unique<ThreadPool>(4);
}

NetworkManager::~NetworkManager() {
    stop();
}

bool NetworkManager::start(uint16_t port, StorageEngine *storage) {
    if (running_)
        return true;

    storage_ = storage;
    if (!storage_) {
        Logger::instance().error("Cannot start NetworkManager without StorageEngine");
        return false;
    }

    running_ = true;
    accept_thread_ = std::thread([this, port]() { accept_loop(port); });
    return true;
}

void NetworkManager::stop() {
    if (!running_)
        return;
    running_ = false;

    if (listen_fd_ >= 0) {
        ::shutdown(listen_fd_,SHUT_RDWR);
        ::close(listen_fd_);
        listen_fd_ = -1;
    }

    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }
}

void NetworkManager::accept_loop(uint16_t port) {
    // Create socket
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        Logger::instance().error("Failed to create socket");
        running_ = false;
        return;
    }

    int opt = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    if (::bind(listen_fd_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
        Logger::instance().error("bind() failed");
        ::close(listen_fd_);
        listen_fd_ = -1;
        running_ = false;
        return;
    }

    if (::listen(listen_fd_, 16) < 0) {
        Logger::instance().error("listen() failed");
        ::close(listen_fd_);
        listen_fd_ = -1;
        running_ = false;
        return;
    }

    Logger::instance().info("NetworkManager listening on port " + std::to_string(port));

    while (running_) {
        sockaddr_in client_addr{};
        socklen_t len = sizeof(client_addr);
        int client_fd = ::accept(listen_fd_, reinterpret_cast<sockaddr *>(&client_addr), &len);
        if (client_fd < 0) {
            if (!running_)
                break;
            Logger::instance().error("accept() failed");
            continue;
        }

        thread_pool_->submit([this, client_fd]() {
            handle_client(client_fd);
        });
    }
}

void NetworkManager::handle_client(int client_fd) {
    char buffer[4096];
    std::memset(buffer, 0, sizeof(buffer));

    ssize_t n = ::recv(client_fd, buffer, sizeof(buffer), 0);
    if (n <= 0) {
        ::shutdown(client_fd, SHUT_RDWR);
        ::close(client_fd);
        return;
    }

    std::string request_json(buffer, buffer + n);

    std::string response_json = process_request(request_json);

    if (!response_json.empty()) {
        ::send(client_fd, response_json.data(), response_json.size(), 0);
    }

    ::shutdown(client_fd, SHUT_RDWR);
    ::close(client_fd);
}

std::string NetworkManager::process_request(const std::string &request_json) {
    try {
        Request req = JsonSerializer::deserialize_request(request_json);

        Response resp;

        switch (req.type) {
        case RequestType::GET: {
            std::string value = storage_->get(req.key);
            if (!value.empty() || storage_->contains(req.key)) {
                resp.success = true;
                resp.value = value;
            } else {
                resp.success = false;
                resp.error = "Key not found";
            }
            break;
        }

        case RequestType::PUT: {
            storage_->put(req.key, req.value);
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

    } catch (const std::exception &e) {
        Response error_resp;
        error_resp.success = false;
        error_resp.error = std::string("Error: ") + e.what();
        return JsonSerializer::serialize(error_resp);
    }
}

} // namespace trelliskv
