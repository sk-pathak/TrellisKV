#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>

#include "cluster_state.h"
#include "messages.h"
#include "node_config.h"
#include "node_info.h"
#include "result.h"
#include "types.h"

namespace trelliskv {

class NetworkManager;
class StorageEngine;
class HashRing;
class ConnectionPool;
class RequestRouter;

class TrellisNode {
   public:
    struct ClusterStats {
        size_t total_nodes = 0;
        size_t active_connections = 0;
        size_t total_requests_handled = 0;
        size_t local_keys = 0;
    };

    explicit TrellisNode(const NodeId& node_id, const NodeConfig& config);
    ~TrellisNode();

    static NodeConfig create_default_config(const std::string& hostname,
                                            uint16_t port);

    Result<void> start();
    void stop();
    bool is_running() const;

    const NodeConfig& get_config() const;
    const NodeId& get_node_id() const;
    NodeInfo get_node_info() const;

    Result<void> join_cluster();
    void add_node(const NodeInfo& node);
    void remove_node(const NodeId& node_id);

    ClusterStats get_stats() const;
    std::unique_ptr<Response> handle_request(const Request& request);

   private:
    Response handle_get_request(const GetRequest& request);
    Response handle_put_request(const PutRequest& request);
    Response handle_delete_request(const DeleteRequest& request);
    std::string generate_request_id() const;
    Result<void> discover_cluster_from_seeds();
    Result<ClusterState> contact_seed_node(const NodeAddress& seed_address);
    void merge_discovered_cluster_state(const ClusterState& discovered_state);
    Result<void> bootstrap_from_cluster();
    std::unique_ptr<Response> handle_cluster_discovery_request(
        const ClusterDiscoveryRequest& request);
    std::unique_ptr<Response> handle_bootstrap_request(
        const BootstrapRequest& request);

    NodeId node_id_;
    NodeConfig config_;
    std::atomic<bool> running_;
    std::chrono::steady_clock::time_point start_time_;

    std::unique_ptr<NetworkManager> network_manager_;
    std::unique_ptr<StorageEngine> storage_engine_;
    std::unique_ptr<HashRing> hash_ring_;
    std::unique_ptr<ConnectionPool> connection_pool_;
    std::unique_ptr<RequestRouter> request_router_;

    mutable std::mutex stats_mutex_;
    ClusterStats stats_;
};

}  // namespace trelliskv