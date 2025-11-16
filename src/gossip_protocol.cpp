#include "trelliskv/gossip_protocol.h"

#include <algorithm>
#include <mutex>
#include <optional>
#include <random>
#include <shared_mutex>
#include <thread>

#include "trelliskv/cluster_state.h"
#include "trelliskv/gossip_message.h"
#include "trelliskv/hash_ring.h"
#include "trelliskv/json_serializer.h"
#include "trelliskv/logger.h"
#include "trelliskv/messages.h"
#include "trelliskv/network_manager.h"
#include "trelliskv/node_info.h"

namespace trelliskv {

void ClusterState::update_node_heartbeat(const NodeId& node_id,
                                         const HeartbeatRequest& heartbeat) {
    last_heartbeat[node_id] = heartbeat.timestamp;
    heartbeat_sequence[node_id] = heartbeat.sequence_number;
    last_updated = std::chrono::system_clock::now();

    // Update node info if it exists
    auto it = nodes.find(node_id);
    if (it != nodes.end()) {
        it->second.last_seen = heartbeat.timestamp;
        it->second.state = heartbeat.sender_state;
    }
}

std::vector<NodeId> ClusterState::get_failed_nodes(
    std::chrono::milliseconds timeout) const {
    std::vector<NodeId> failed_nodes;
    auto now = std::chrono::system_clock::now();

    for (const auto& [node_id, node_info] : nodes) {
        // First check if node is explicitly marked as failed
        if (node_info.is_failed()) {
            failed_nodes.push_back(node_id);
            continue;
        }

        // Then check for timeout-based failures
        auto last_heartbeat_it = last_heartbeat.find(node_id);
        if (last_heartbeat_it != last_heartbeat.end()) {
            auto time_since_heartbeat =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    now - last_heartbeat_it->second);
            if (time_since_heartbeat > timeout) {
                failed_nodes.push_back(node_id);
            }
        } else {
            // No heartbeat recorded, consider failed
            failed_nodes.push_back(node_id);
        }
    }

    return failed_nodes;
}

std::vector<NodeId> ClusterState::get_active_nodes() const {
    std::vector<NodeId> active_nodes;
    for (const auto& [node_id, node_info] : nodes) {
        if (node_info.is_active()) {
            active_nodes.push_back(node_id);
        }
    }
    return active_nodes;
}

size_t ClusterState::active_node_count() const {
    return std::count_if(nodes.begin(), nodes.end(), [](const auto& pair) {
        return pair.second.is_active();
    });
}

size_t ClusterState::cleanup_old_nodes(std::chrono::hours tombstone_ttl) {
    auto now = std::chrono::system_clock::now();
    std::vector<NodeId> to_remove;

    for (const auto& [node_id, node_info] : nodes) {
        if (node_info.is_failed()) {
            auto time_since_seen =
                std::chrono::duration_cast<std::chrono::hours>(
                    now - node_info.last_seen);
            if (time_since_seen > tombstone_ttl) {
                to_remove.push_back(node_id);
            }
        }
    }

    for (const auto& node_id : to_remove) {
        nodes.erase(node_id);
        last_heartbeat.erase(node_id);
        heartbeat_sequence.erase(node_id);
    }

    return to_remove.size();
}

// GossipProtocol implementation
GossipProtocol::GossipProtocol(const NodeId& node_id, const NodeInfo& node_info,
                               NetworkManager* network_manager,
                               std::chrono::milliseconds heartbeat_interval,
                               std::chrono::milliseconds failure_timeout)
    : local_node_id_(node_id),
      local_node_info_(node_info),
      heartbeat_interval_(heartbeat_interval),
      failure_timeout_(failure_timeout),
      running_(false),
      network_manager_(network_manager),
      hash_ring_(nullptr),
      local_heartbeat_sequence_(0) {
    // Add local node to cluster state
    std::unique_lock<std::shared_mutex> lock(cluster_state_mutex_);
    cluster_state_.nodes[local_node_id_] = local_node_info_;
    cluster_state_.last_heartbeat[local_node_id_] =
        std::chrono::system_clock::now();
    cluster_state_.heartbeat_sequence[local_node_id_] = 0;
}

GossipProtocol::~GossipProtocol() { stop(); }

void GossipProtocol::start() {
    if (running_.exchange(true)) {
        return;  // Already running
    }

    // Start heartbeat thread
    heartbeat_thread_ = std::thread([this] { heartbeat_loop(); });

    // Start failure detection thread
    failure_detection_thread_ =
        std::thread([this] { failure_detection_loop(); });
}

void GossipProtocol::stop() {
    if (!running_.exchange(false)) {
        return;  // Already stopped
    }

    // Wait for threads to finish
    if (heartbeat_thread_.joinable()) {
        heartbeat_thread_.join();
    }

    if (failure_detection_thread_.joinable()) {
        failure_detection_thread_.join();
    }
}

bool GossipProtocol::is_running() const { return running_.load(); }

void GossipProtocol::set_node_failure_callback(NodeFailureCallback callback) {
    on_node_failure_ = std::move(callback);
}

void GossipProtocol::set_node_recovery_callback(NodeRecoveryCallback callback) {
    on_node_recovery_ = std::move(callback);
}

void GossipProtocol::set_cluster_change_callback(
    ClusterChangeCallback callback) {
    on_cluster_change_ = std::move(callback);
}

void GossipProtocol::set_node_join_callback(NodeJoinCallback callback) {
    on_node_join_ = std::move(callback);
}

void GossipProtocol::set_heartbeat_interval(
    std::chrono::milliseconds interval) {
    heartbeat_interval_ = interval;
}

void GossipProtocol::set_failure_timeout(std::chrono::milliseconds timeout) {
    failure_timeout_ = timeout;
}

void GossipProtocol::add_node(const NodeInfo& node_info) {
    std::unique_lock<std::shared_mutex> lock(cluster_state_mutex_);
    cluster_state_.nodes[node_info.id] = node_info;
    cluster_state_.last_heartbeat[node_info.id] =
        std::chrono::system_clock::now();
    cluster_state_.heartbeat_sequence[node_info.id] = 0;
    lock.unlock();

    notify_cluster_change();
}

void GossipProtocol::remove_node(const NodeId& node_id) {
    std::unique_lock<std::shared_mutex> lock(cluster_state_mutex_);
    cluster_state_.nodes.erase(node_id);
    cluster_state_.last_heartbeat.erase(node_id);
    cluster_state_.heartbeat_sequence.erase(node_id);
    lock.unlock();

    notify_cluster_change();
}

void GossipProtocol::mark_node_failed(const NodeId& node_id) {
    bool should_notify = false;

    {
        std::unique_lock<std::shared_mutex> lock(cluster_state_mutex_);
        auto it = cluster_state_.nodes.find(node_id);
        if (it != cluster_state_.nodes.end() &&
            it->second.state != NodeState::FAILED) {
            it->second.state = NodeState::FAILED;
            should_notify = true;
        }
    }  // Lock released here

    if (should_notify) {
        process_node_failure(node_id);
    }
}

void GossipProtocol::mark_node_recovered(const NodeId& node_id) {
    bool should_notify = false;

    {
        std::unique_lock<std::shared_mutex> lock(cluster_state_mutex_);
        auto it = cluster_state_.nodes.find(node_id);
        if (it != cluster_state_.nodes.end() &&
            it->second.state == NodeState::FAILED) {
            it->second.state = NodeState::ACTIVE;
            it->second.update_last_seen();
            cluster_state_.last_heartbeat[node_id] =
                std::chrono::system_clock::now();
            should_notify = true;
        }
    }  // Lock released here

    if (should_notify) {
        process_node_recovery(node_id);
    }
}

void GossipProtocol::send_heartbeat() {
    // Get list of active nodes to send heartbeat to
    auto active_nodes = get_active_nodes();

    // Send heartbeat to all active nodes (except self)
    for (const auto& node_id : active_nodes) {
        if (node_id == local_node_id_) {
            continue;
        }

        auto node_info = get_node_info(node_id);
        if (node_info) {
            try {
                // Create HeartbeatRequest using the new Request type
                auto sequence_num = local_heartbeat_sequence_.fetch_add(1);
                HeartbeatRequest heartbeat_req(local_node_id_, sequence_num,
                                               local_node_info_.state);
                heartbeat_req.request_id =
                    "hb_" + local_node_id_ + "_" + std::to_string(sequence_num);

                // Send via network manager with short timeout (heartbeats
                // should be fast)
                auto result = network_manager_->send_request(
                    node_info->address, heartbeat_req,
                    std::chrono::milliseconds(
                        500)  // Short timeout for heartbeats
                );

                // Process the response if successful
                if (result.is_success()) {
                    auto response = std::move(result.value());
                    if (auto* hb_response =
                            dynamic_cast<HeartbeatResponse*>(response.get())) {
                        // Update that we received a response from this node
                        std::unique_lock<std::shared_mutex> lock(
                            cluster_state_mutex_);
                        cluster_state_.last_heartbeat[node_id] =
                            hb_response->timestamp;
                        cluster_state_.heartbeat_sequence[node_id] =
                            hb_response->sequence_number;

                        // Update node state if needed
                        auto it = cluster_state_.nodes.find(node_id);
                        if (it != cluster_state_.nodes.end()) {
                            it->second.state = hb_response->responder_state;
                            it->second.last_seen = hb_response->timestamp;
                        }
                    }
                }
                // Ignore failures - heartbeats are best-effort
            } catch (const std::exception& e) {
                // Ignore errors but continue with other nodes
            }
        }
    }

    // Update local heartbeat timestamp
    std::unique_lock<std::shared_mutex> lock(cluster_state_mutex_);
    cluster_state_.last_heartbeat[local_node_id_] =
        std::chrono::system_clock::now();
}

void GossipProtocol::handle_heartbeat(const HeartbeatRequest& heartbeat,
                                      const NodeAddress& sender_address) {
    bool is_recovery = false;
    bool is_new_node = false;

    {
        std::unique_lock<std::shared_mutex> lock(cluster_state_mutex_);

        auto it = cluster_state_.nodes.find(heartbeat.sender_id);
        if (it == cluster_state_.nodes.end()) {
            // New node discovered
            NodeInfo new_node(heartbeat.sender_id, sender_address,
                              heartbeat.sender_state);
            new_node.last_seen = heartbeat.timestamp;
            cluster_state_.nodes[heartbeat.sender_id] = new_node;
            cluster_state_.last_heartbeat[heartbeat.sender_id] =
                heartbeat.timestamp;
            cluster_state_.heartbeat_sequence[heartbeat.sender_id] =
                heartbeat.sequence_number;
            is_new_node = true;
        } else {
            // Check for recovery BEFORE updating state
            is_recovery = (it->second.state == NodeState::FAILED &&
                           heartbeat.sender_state == NodeState::ACTIVE);

            // Now update the node info
            it->second.address = sender_address;
            it->second.state = heartbeat.sender_state;
            it->second.last_seen = heartbeat.timestamp;

            // Update heartbeat tracking
            cluster_state_.last_heartbeat[heartbeat.sender_id] =
                heartbeat.timestamp;
            cluster_state_.heartbeat_sequence[heartbeat.sender_id] =
                heartbeat.sequence_number;
        }
    }  // Release lock before calling callbacks

    // Process events outside lock
    if (is_new_node) {
        notify_cluster_change();
    }

    if (is_recovery) {
        process_node_recovery(heartbeat.sender_id);
    }
}

void GossipProtocol::handle_gossip_message(
    const GossipMessage& message, const NodeAddress& /* sender_address */) {
    // Create a temporary cluster state from the gossip message
    ClusterState remote_state;
    remote_state.nodes.clear();

    for (const auto& node_info : message.known_nodes) {
        remote_state.nodes[node_info.id] = node_info;
    }

    // Merge the remote state with local state
    update_cluster_state(remote_state);
}

void GossipProtocol::exchange_metadata(const NodeId& peer_id) {
    auto node_info = get_node_info(peer_id);
    if (!node_info) {
        return;
    }

    try {
        auto gossip_message = create_gossip_message();
        auto json_message_result =
            JsonSerializer::serialize_gossip_message(gossip_message);

        if (!json_message_result) {
            LOG_WARN("Failed to serialize gossip message: " +
                     json_message_result.error());
            return;
        }

        // Send gossip message to peer
        network_manager_->send_message_async(node_info->address,
                                             json_message_result.value());
    } catch (const std::exception& e) {
        // Log error but continue
        LOG_WARN("Couldn't send gossip message");
    }
}

ClusterState GossipProtocol::get_cluster_state() const {
    std::shared_lock<std::shared_mutex> lock(cluster_state_mutex_);
    return cluster_state_;
}

std::vector<NodeId> GossipProtocol::get_active_nodes() const {
    std::shared_lock<std::shared_mutex> lock(cluster_state_mutex_);
    return cluster_state_.get_active_nodes();
}

std::vector<NodeId> GossipProtocol::get_failed_nodes() const {
    std::shared_lock<std::shared_mutex> lock(cluster_state_mutex_);
    return cluster_state_.get_failed_nodes(failure_timeout_);
}

std::optional<NodeInfo> GossipProtocol::get_node_info(
    const NodeId& node_id) const {
    std::shared_lock<std::shared_mutex> lock(cluster_state_mutex_);
    auto it = cluster_state_.nodes.find(node_id);
    if (it != cluster_state_.nodes.end()) {
        return it->second;
    }
    return std::nullopt;
}

size_t GossipProtocol::get_active_node_count() const {
    std::shared_lock<std::shared_mutex> lock(cluster_state_mutex_);
    return cluster_state_.active_node_count();
}

bool GossipProtocol::is_node_active(const NodeId& node_id) const {
    std::shared_lock<std::shared_mutex> lock(cluster_state_mutex_);
    auto it = cluster_state_.nodes.find(node_id);
    return it != cluster_state_.nodes.end() && it->second.is_active();
}

// Private method implementations
void GossipProtocol::heartbeat_loop() {
    while (running_.load()) {
        // Send heartbeats using the public method
        send_heartbeat();

        // Sleep for heartbeat interval
        std::this_thread::sleep_for(heartbeat_interval_);
    }
}

void GossipProtocol::failure_detection_loop() {
    // Run failure detection at half the heartbeat interval for more responsive
    // detection
    auto detection_interval = heartbeat_interval_ / 2;

    // Track last cleanup time
    auto last_cleanup = std::chrono::steady_clock::now();
    constexpr auto cleanup_interval = std::chrono::hours(1);

    while (running_.load()) {
        detect_failures();

        // Periodic cleanup of old tombstoned nodes (every hour)
        auto now = std::chrono::steady_clock::now();
        if (now - last_cleanup > cleanup_interval) {
            {
                std::unique_lock<std::shared_mutex> lock(cluster_state_mutex_);
                cluster_state_.cleanup_old_nodes();
            }
            last_cleanup = now;
        }

        // Sleep for detection interval
        std::this_thread::sleep_for(detection_interval);
    }
}

void GossipProtocol::detect_failures() {
    auto now = std::chrono::system_clock::now();
    std::vector<NodeId> newly_failed;

    {
        std::shared_lock<std::shared_mutex> lock(cluster_state_mutex_);

        for (const auto& [node_id, node_info] : cluster_state_.nodes) {
            // Skip self and already failed nodes
            if (node_id == local_node_id_ || node_info.is_failed()) {
                continue;
            }

            // Check if we have a heartbeat record
            auto it = cluster_state_.last_heartbeat.find(node_id);
            if (it != cluster_state_.last_heartbeat.end()) {
                auto elapsed =
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        now - it->second);

                // Mark as failed if heartbeat timeout exceeded
                if (elapsed > failure_timeout_) {
                    newly_failed.push_back(node_id);
                }
            } else {
                // No heartbeat record - should not happen, but mark as failed
                newly_failed.push_back(node_id);
            }
        }
    }

    // Mark failed nodes outside the lock to avoid deadlocks
    for (const auto& node_id : newly_failed) {
        mark_node_failed(node_id);
    }
}

void GossipProtocol::process_node_failure(const NodeId& node_id) {
    if (hash_ring_) {
        hash_ring_->remove_node(node_id);
    }

    if (on_node_failure_) {
        on_node_failure_(node_id);
    }

    notify_cluster_change();
}

void GossipProtocol::process_node_recovery(const NodeId& node_id) {
    std::shared_lock<std::shared_mutex> lock(cluster_state_mutex_);
    auto it = cluster_state_.nodes.find(node_id);
    if (it != cluster_state_.nodes.end() && hash_ring_) {
        hash_ring_->add_node(it->second);
    }
    lock.unlock();

    if (on_node_recovery_) {
        on_node_recovery_(node_id);
    }

    notify_cluster_change();
}

void GossipProtocol::update_cluster_state(const ClusterState& remote_state) {
    bool cluster_changed = false;

    std::unique_lock<std::shared_mutex> lock(cluster_state_mutex_);

    // Merge remote node information
    for (const auto& [node_id, remote_node_info] : remote_state.nodes) {
        auto local_it = cluster_state_.nodes.find(node_id);

        if (local_it == cluster_state_.nodes.end()) {
            // New node discovered
            cluster_state_.nodes[node_id] = remote_node_info;
            cluster_state_.last_heartbeat[node_id] = remote_node_info.last_seen;
            cluster_changed = true;
        } else {
            // Update existing node if remote info is newer
            if (remote_node_info.last_seen > local_it->second.last_seen) {
                merge_node_info(remote_node_info);
                cluster_changed = true;
            }
        }
    }

    lock.unlock();

    if (cluster_changed) {
        notify_cluster_change();
    }
}

void GossipProtocol::merge_node_info(const NodeInfo& remote_node_info) {
    auto& local_node_info = cluster_state_.nodes[remote_node_info.id];

    // Update with newer information
    if (remote_node_info.last_seen > local_node_info.last_seen) {
        local_node_info.address = remote_node_info.address;
        local_node_info.state = remote_node_info.state;
        local_node_info.last_seen = remote_node_info.last_seen;
        cluster_state_.last_heartbeat[remote_node_info.id] =
            remote_node_info.last_seen;
    }
}

std::vector<NodeId> GossipProtocol::select_gossip_targets(size_t count) const {
    std::shared_lock<std::shared_mutex> lock(cluster_state_mutex_);
    auto active_nodes = cluster_state_.get_active_nodes();

    // Remove self from targets
    active_nodes.erase(
        std::remove(active_nodes.begin(), active_nodes.end(), local_node_id_),
        active_nodes.end());

    // If we have fewer nodes than requested, return all
    if (active_nodes.size() <= count) {
        return active_nodes;
    }

    // Randomly select nodes for gossip
    std::random_device rd;
    std::mt19937 gen(rd());
    std::shuffle(active_nodes.begin(), active_nodes.end(), gen);

    active_nodes.resize(count);
    return active_nodes;
}

GossipMessage GossipProtocol::create_gossip_message() const {
    GossipMessage gossip(local_node_id_);

    std::shared_lock<std::shared_mutex> cluster_lock(cluster_state_mutex_);

    // Include all known nodes
    for (const auto& [node_id, node_info] : cluster_state_.nodes) {
        gossip.known_nodes.push_back(node_info);
    }

    return gossip;
}

void GossipProtocol::notify_cluster_change() {
    if (on_cluster_change_) {
        std::shared_lock<std::shared_mutex> lock(cluster_state_mutex_);
        auto current_state = cluster_state_;
        lock.unlock();
        on_cluster_change_(current_state);
    }
}

void GossipProtocol::set_hash_ring(HashRing* hash_ring) {
    hash_ring_ = hash_ring;
}

void GossipProtocol::handle_network_partition() {
    std::unique_lock<std::shared_mutex> lock(cluster_state_mutex_);

    auto now = std::chrono::system_clock::now();
    std::vector<NodeId> potentially_failed_nodes;

    for (const auto& [node_id, last_heartbeat_time] :
         cluster_state_.last_heartbeat) {
        if (node_id == local_node_id_) {
            continue;
        }

        auto time_since_heartbeat =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                now - last_heartbeat_time);

        if (time_since_heartbeat > (failure_timeout_ * 3)) {
            potentially_failed_nodes.push_back(node_id);
        }
    }

    lock.unlock();

    for (const auto& node_id : potentially_failed_nodes) {
        mark_node_failed(node_id);
    }
}

}  // namespace trelliskv