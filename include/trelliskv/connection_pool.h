#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "result.h"

namespace trelliskv {

struct NodeAddress;
struct Request;
struct Response;

class ConnectionPool {
   public:
    explicit ConnectionPool(size_t max_connections_per_node = 10);
    ~ConnectionPool();

    Result<std::unique_ptr<Response>> send_request(
        const NodeAddress& target, const Request& request,
        std::chrono::milliseconds timeout);

    void close_connections_to_node(const NodeAddress& target);

    void close_all_connections();

    struct PoolStats {
        size_t total_connections = 0;
        size_t active_requests = 0;
        size_t failed_connections = 0;
    };

    PoolStats get_stats() const;

   private:
    struct Connection {
        int socket_fd;
        std::chrono::system_clock::time_point last_used;
        std::atomic<bool> in_use;

        Connection(int fd)
            : socket_fd(fd),
              last_used(std::chrono::system_clock::now()),
              in_use(false) {}
    };

    struct NodeConnectionPool {
        std::mutex mutex;
        std::condition_variable cv;
        std::vector<std::unique_ptr<Connection>> connections;
        size_t active_count = 0;
    };

    mutable std::mutex pool_mutex_;
    std::unordered_map<std::string, std::unique_ptr<NodeConnectionPool>>
        node_pools_;
    size_t max_connections_per_node_;

    mutable std::atomic<size_t> total_connections_{0};
    mutable std::atomic<size_t> active_requests_{0};
    mutable std::atomic<size_t> failed_connections_{0};

    std::string address_to_key(const NodeAddress& address) const;
    Result<Connection*> get_or_create_connection(
        const NodeAddress& target, std::chrono::milliseconds timeout);
    void return_connection(const NodeAddress& target, Connection* conn,
                           bool valid);
    Result<std::unique_ptr<Connection>> create_new_connection(
        const NodeAddress& target);
    bool is_connection_valid(Connection* conn);
    NodeConnectionPool* get_node_pool(const std::string& key);
    void close_socket_fd(Connection* conn);
};
}  // namespace trelliskv