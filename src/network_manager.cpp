#include "trelliskv/network_manager.h"
#include "trelliskv/logger.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <iostream>

namespace trelliskv {

NetworkManager::NetworkManager() {}
NetworkManager::~NetworkManager() { stop(); }

bool NetworkManager::start(uint16_t port, RequestHandler handler) {
    if (running_)
        return true;
    handler_ = handler;
    running_ = true;

    accept_thread_ = std::thread([this, port]() {
        accept_loop(port);
    });
    return true;
}

void NetworkManager::stop() {
    if (!running_)
        return;
    running_ = false;

    if (listen_fd_ >= 0) {
        ::shutdown(listen_fd_, SHUT_RDWR);
        ::close(listen_fd_);
        listen_fd_ = -1;
    }

    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }
}

void NetworkManager::accept_loop(uint16_t port) {
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        trelliskv::Logger::instance().error("Failed to create socket");
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
        trelliskv::Logger::instance().error("bind() failed");
        ::close(listen_fd_);
        listen_fd_ = -1;
        running_ = false;
        return;
    }

    if (::listen(listen_fd_, 16) < 0) {
        trelliskv::Logger::instance().error("listen() failed");
        ::close(listen_fd_);
        listen_fd_ = -1;
        running_ = false;
        return;
    }

    trelliskv::Logger::instance().info("NetworkManager listening on port " + std::to_string(port));

    while (running_) {
        sockaddr_in client_addr{};
        socklen_t len = sizeof(client_addr);
        int client_fd = ::accept(listen_fd_, reinterpret_cast<sockaddr *>(&client_addr), &len);
        if (client_fd < 0) {
            if (!running_)
                break;
            trelliskv::Logger::instance().warn("accept() failed");
            continue;
        }

        std::string response = handle_connection(client_fd);
        (void)response;

        ::close(client_fd);
    }
}

std::string NetworkManager::handle_connection(int client_fd) {
    char buffer[4096];
    std::memset(buffer, 0, sizeof(buffer));

    ssize_t n = ::recv(client_fd, buffer, sizeof(buffer), 0);
    if (n <= 0) {
        return {};
    }

    std::string request(buffer, buffer + n);

    std::string output;
    if (handler_) {
        output = handler_(request);
    } else {
        output = request;
    }

    if (!output.empty()) {
        ::send(client_fd, output.data(), output.size(), 0);
    }

    return output;
}

} // namespace trelliskv
