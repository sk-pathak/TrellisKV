#pragma once

#include "node_info.h"
#include "types.h"
#include <chrono>
#include <unordered_map>
#include <vector>

namespace trelliskv {

struct HeartbeatRequest;

struct ClusterState {
    std::unordered_map<NodeId, NodeInfo> nodes;
    std::unordered_map<NodeId, Timestamp> last_heartbeat;
    std::unordered_map<NodeId, uint64_t> heartbeat_sequence;
    Timestamp last_updated;

    ClusterState() : last_updated(std::chrono::system_clock::now()) {}

    void update_node_heartbeat(const NodeId &node_id, const HeartbeatRequest &heartbeat);
    std::vector<NodeId> get_failed_nodes(std::chrono::milliseconds timeout) const;
    std::vector<NodeId> get_active_nodes() const;
    size_t active_node_count() const;

    size_t cleanup_old_nodes(std::chrono::hours tombstone_ttl = std::chrono::hours(24));
};

} // namespace trelliskv