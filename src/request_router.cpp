#include "trelliskv/request_router.h"

#include <algorithm>
#include <chrono>

#include "trelliskv/connection_pool.h"
#include "trelliskv/hash_ring.h"
#include "trelliskv/messages.h"
#include "trelliskv/node_info.h"

namespace trelliskv {

RequestRouter::RequestRouter(const NodeId& node_id, HashRing* hash_ring,
                             ConnectionPool* connection_pool,
                             size_t replication_factor)
    : node_id_(node_id),
      hash_ring_(hash_ring),
      connection_pool_(connection_pool),
      replication_factor_(replication_factor) {}

bool RequestRouter::is_key_local(const std::string& key) const {
    NodeId primary_node = hash_ring_->get_primary_node(key);
    return primary_node == node_id_;
}

NodeId RequestRouter::get_primary_node_for_key(const std::string& key) const {
    return hash_ring_->get_primary_node(key);
}

std::vector<NodeId> RequestRouter::get_replica_nodes_for_key(
    const std::string& key) const {
    return hash_ring_->get_replica_nodes(key, replication_factor_);
}

Result<std::unique_ptr<Response>> RequestRouter::forward_request_to_node(
    const NodeId& target_node_id, const Request& request) {
    NodeAddress target_address = get_node_address(target_node_id);
    if (target_address.hostname.empty()) {
        return Result<std::unique_ptr<Response>>::error("Unknown node: " +
                                                        target_node_id);
    }

    std::chrono::milliseconds timeout(2500);
    std::unique_ptr<Request> forwarded_req;
    if (auto get_req = dynamic_cast<const GetRequest*>(&request)) {
        timeout = std::chrono::milliseconds(2500);
        auto copy = std::make_unique<GetRequest>(*get_req);
        copy->sender_id = node_id_;
        forwarded_req = std::move(copy);
    } else if (auto put_req = dynamic_cast<const PutRequest*>(&request)) {
        timeout = std::chrono::milliseconds(2500);
        auto copy = std::make_unique<PutRequest>(*put_req);
        copy->sender_id = node_id_;
        forwarded_req = std::move(copy);
    } else if (dynamic_cast<const DeleteRequest*>(&request)) {
        timeout = std::chrono::milliseconds(2500);
        auto del = static_cast<const DeleteRequest*>(&request);
        auto copy = std::make_unique<DeleteRequest>(*del);
        copy->sender_id = node_id_;
        forwarded_req = std::move(copy);
    } else {
        forwarded_req = std::unique_ptr<Request>(nullptr);
    }

    const Request& to_send = forwarded_req ? *forwarded_req : request;
    auto res = connection_pool_->send_request(target_address, to_send, timeout);
    return res;
}

Result<std::unique_ptr<Response>> RequestRouter::forward_get_request(
    const GetRequest& request) {
    return forward_request_with_failover(request, request.key);
}

Result<std::unique_ptr<Response>> RequestRouter::forward_put_request(
    const PutRequest& request) {
    return forward_request_with_failover(request, request.key);
}

Result<std::unique_ptr<Response>> RequestRouter::forward_delete_request(
    const DeleteRequest& request) {
    return forward_request_with_failover(request, request.key);
}

RequestRouter::RoutingStats RequestRouter::get_stats() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    return stats_;
}

Result<std::unique_ptr<Response>> RequestRouter::forward_request_with_failover(
    const Request& request, const std::string& key) {
    auto replica_nodes = get_replica_nodes_for_key(key);
    replica_nodes.erase(
        std::remove(replica_nodes.begin(), replica_nodes.end(), node_id_),
        replica_nodes.end());
    if (replica_nodes.empty()) {
        return Result<std::unique_ptr<Response>>::error(
            "No nodes available for key: " + key);
    }

    std::string last_error;
    size_t attempts = 0;
    const size_t max_attempts =
        std::min(replica_nodes.size(), static_cast<size_t>(3));

    for (size_t i = 0; i < replica_nodes.size() && attempts < max_attempts;
         ++i) {
        const auto& node_id = replica_nodes[i];

        {
            std::lock_guard<std::mutex> lock(circuit_breakers_mutex_);
            auto& cb = circuit_breakers_[node_id];
            if (!cb.should_attempt()) {
                last_error = "Circuit breaker open for node " + node_id;
                continue;
            }
        }

        attempts++;
        auto result = forward_request_to_node(node_id, request);

        if (result.is_success()) {
            {
                std::lock_guard<std::mutex> lock(circuit_breakers_mutex_);
                circuit_breakers_[node_id].record_success();
            }

            std::lock_guard<std::mutex> lock(stats_mutex_);
            stats_.total_requests_forwarded++;
            if (attempts > 1) {
                stats_.successful_failovers++;
            }
            return result;
        }

        {
            std::lock_guard<std::mutex> lock(circuit_breakers_mutex_);
            circuit_breakers_[node_id].record_failure();
        }

        last_error = result.error();
    }

    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        stats_.failed_forwards++;
    }

    return Result<std::unique_ptr<Response>>::error(
        "All replica nodes failed after " + std::to_string(attempts) +
        " attempts. Last error: " + last_error);
}

NodeAddress RequestRouter::get_node_address(const NodeId& node_id) const {
    auto node_info = hash_ring_->get_node_info(node_id);
    if (node_info) {
        return node_info->address;
    }
    return NodeAddress{};
}

}  // namespace trelliskv