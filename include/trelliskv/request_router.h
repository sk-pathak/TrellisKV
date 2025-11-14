#pragma once

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "result.h"
#include "types.h"

namespace trelliskv {

class ConnectionPool;
class HashRing;
struct Request;
struct Response;
struct GetRequest;
struct PutRequest;
struct DeleteRequest;
struct NodeAddress;

class CircuitBreaker {
   public:
    enum State { CLOSED, OPEN, HALF_OPEN };

    CircuitBreaker(
        size_t failure_threshold = 5,
        std::chrono::seconds open_duration = std::chrono::seconds(30))
        : state_(CLOSED),
          failure_count_(0),
          failure_threshold_(failure_threshold),
          open_duration_(open_duration) {}

    bool should_attempt() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (state_ == OPEN) {
            if (std::chrono::steady_clock::now() - open_time_ >
                open_duration_) {
                state_ = HALF_OPEN;
                return true;
            }
            return false;
        }
        return true;
    }

    void record_success() {
        std::lock_guard<std::mutex> lock(mutex_);
        failure_count_ = 0;
        state_ = CLOSED;
    }

    void record_failure() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (++failure_count_ >= failure_threshold_) {
            state_ = OPEN;
            open_time_ = std::chrono::steady_clock::now();
        }
    }

    State get_state() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return state_;
    }

   private:
    mutable std::mutex mutex_;
    State state_;
    size_t failure_count_;
    size_t failure_threshold_;
    std::chrono::seconds open_duration_;
    std::chrono::steady_clock::time_point open_time_;
};

class RequestRouter {
   public:
    RequestRouter(const NodeId& node_id, HashRing* hash_ring,
                  ConnectionPool* connection_pool, size_t replication_factor);

    bool is_key_local(const std::string& key) const;

    NodeId get_primary_node_for_key(const std::string& key) const;

    std::vector<NodeId> get_replica_nodes_for_key(const std::string& key) const;

    Result<std::unique_ptr<Response>> forward_request_to_node(
        const NodeId& target_node_id, const Request& request);

    Result<std::unique_ptr<Response>> forward_get_request(
        const GetRequest& request);
    Result<std::unique_ptr<Response>> forward_put_request(
        const PutRequest& request);
    Result<std::unique_ptr<Response>> forward_delete_request(
        const DeleteRequest& request);

    struct RoutingStats {
        size_t total_requests_forwarded = 0;
        size_t failed_forwards = 0;
        size_t successful_failovers = 0;
    };

    RoutingStats get_stats() const;

   private:
    NodeId node_id_;
    HashRing* hash_ring_;
    ConnectionPool* connection_pool_;
    size_t replication_factor_;

    mutable std::mutex stats_mutex_;
    RoutingStats stats_;

    mutable std::mutex circuit_breakers_mutex_;
    std::unordered_map<NodeId, CircuitBreaker> circuit_breakers_;

    Result<std::unique_ptr<Response>> forward_request_with_failover(
        const Request& request, const std::string& key);

    NodeAddress get_node_address(const NodeId& node_id) const;
};

}  // namespace trelliskv