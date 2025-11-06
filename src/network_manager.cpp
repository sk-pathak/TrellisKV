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
    // Deserialize request
    auto req_result = JsonSerializer::deserialize_request(request_json);
    if (!req_result) {
        Response error_resp = Response::error(
            "Failed to deserialize request: " + req_result.error());
        error_resp.responder_id = node_id_;
        auto resp_str = JsonSerializer::serialize_response(error_resp);
        return resp_str.value_or("");
    }

    std::unique_ptr<Request>& req = req_result.value();
    Response resp;
    resp.request_id = req->request_id;
    resp.responder_id = node_id_;

    if (auto* get_req = dynamic_cast<GetRequest*>(req.get())) {
        if (storage_->contains(get_req->key)) {
            auto val_result = storage_->get(get_req->key);
            if (val_result) {
                resp = Response::success(val_result.value().value,
                                         val_result.value().version);
            } else {
                resp = Response::error("Failed to retrieve value");
            }
        } else {
            resp = Response::not_found();
        }
    } else if (auto* put_req = dynamic_cast<PutRequest*>(req.get())) {
        VersionedValue versioned_value;
        versioned_value.value = put_req->value;
        versioned_value.version = TimestampVersion::now(node_id_);

        storage_->put(put_req->key, versioned_value);
        resp = Response::success(put_req->value, versioned_value.version);
    } else if (auto* del_req = dynamic_cast<DeleteRequest*>(req.get())) {
        if (storage_->contains(del_req->key)) {
            storage_->remove(del_req->key);
            resp = Response::success();
        } else {
            resp = Response::not_found();
        }
    } else {
        resp = Response::error("Unknown request type");
    }

    resp.request_id = req->request_id;
    resp.responder_id = node_id_;

    // Serialize response
    auto resp_result = JsonSerializer::serialize_response(resp);
    if (!resp_result) {
        LOG_ERROR("Failed to serialize response: " + resp_result.error());
        return "";
    }

    return resp_result.value();
}

}  // namespace trelliskv
