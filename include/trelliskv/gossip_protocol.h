#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <optional>
#include <shared_mutex>
#include <thread>
#include <vector>

#include "cluster_state.h"
#include "gossip_message.h"
#include "node_info.h"
#include "types.h"

namespace trelliskv {

class NetworkManager;
class HashRing;
struct NodeAddress;
struct HeartbeatRequest;

class GossipProtocol {
   public:
    using NodeFailureCallback = std::function<void(const NodeId&)>;
    using NodeRecoveryCallback = std::function<void(const NodeId&)>;
    using ClusterChangeCallback = std::function<void(const ClusterState&)>;
    using NodeJoinCallback = std::function<void(const NodeInfo&)>;

    GossipProtocol(const NodeId& node_id, const NodeInfo& node_info,
                   NetworkManager* network_manager,
                   std::chrono::milliseconds heartbeat_interval =
                       std::chrono::milliseconds(1000),
                   std::chrono::milliseconds failure_timeout =
                       std::chrono::milliseconds(5000));

    ~GossipProtocol();

    void start();
    void stop();
    bool is_running() const;

    void add_node(const NodeInfo& node_info);
    void remove_node(const NodeId& node_id);
    void mark_node_failed(const NodeId& node_id);
    void mark_node_recovered(const NodeId& node_id);

    void send_heartbeat();
    void handle_heartbeat(const HeartbeatRequest& heartbeat,
                          const NodeAddress& sender_address);

    void handle_gossip_message(const GossipMessage& message,
                               const NodeAddress& sender_address);
    void exchange_metadata(const NodeId& peer_id);

    void set_hash_ring(HashRing* hash_ring);

    ClusterState get_cluster_state() const;
    std::vector<NodeId> get_active_nodes() const;
    std::vector<NodeId> get_failed_nodes() const;
    std::optional<NodeInfo> get_node_info(const NodeId& node_id) const;
    size_t get_active_node_count() const;
    bool is_node_active(const NodeId& node_id) const;

    void set_node_failure_callback(NodeFailureCallback callback);
    void set_node_recovery_callback(NodeRecoveryCallback callback);
    void set_cluster_change_callback(ClusterChangeCallback callback);
    void set_node_join_callback(NodeJoinCallback callback);

    void set_heartbeat_interval(std::chrono::milliseconds interval);
    void set_failure_timeout(std::chrono::milliseconds timeout);

    void handle_network_partition();

   private:
    void heartbeat_loop();
    void failure_detection_loop();

    void detect_failures();
    void process_node_failure(const NodeId& node_id);
    void process_node_recovery(const NodeId& node_id);

    void update_cluster_state(const ClusterState& remote_state);
    void merge_node_info(const NodeInfo& remote_node_info);

    std::vector<NodeId> select_gossip_targets(size_t count = 3) const;
    GossipMessage create_gossip_message() const;

    void notify_cluster_change();

    NodeId local_node_id_;
    NodeInfo local_node_info_;
    std::chrono::milliseconds heartbeat_interval_;
    std::chrono::milliseconds failure_timeout_;

    ClusterState cluster_state_;
    mutable std::shared_mutex cluster_state_mutex_;

    std::atomic<bool> running_;
    std::thread heartbeat_thread_;
    std::thread failure_detection_thread_;

    NetworkManager* network_manager_;

    HashRing* hash_ring_;

    NodeFailureCallback on_node_failure_;
    NodeRecoveryCallback on_node_recovery_;
    ClusterChangeCallback on_cluster_change_;
    NodeJoinCallback on_node_join_;

    std::atomic<uint64_t> local_heartbeat_sequence_;
};

}  // namespace trelliskv