#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "trelliskv/node_info.h"

namespace trelliskv {

struct NodeConfig {
    NodeAddress address;
    std::vector<NodeAddress> seed_nodes;
    size_t replication_factor = 3;
    size_t virtual_nodes_per_physical = 50;

    NodeConfig() = default;
    NodeConfig(const std::string& host, uint16_t port) : address(host, port) {}
};

struct ClusterConfig {
    std::vector<NodeAddress> initial_nodes;
    size_t min_cluster_size = 3;
    size_t max_cluster_size = 100;
    size_t default_replication_factor = 3;

    ClusterConfig() = default;

    bool is_valid() const {
        return min_cluster_size > 0 && max_cluster_size >= min_cluster_size &&
               default_replication_factor <= max_cluster_size &&
               !initial_nodes.empty();
    }

    size_t get_recommended_virtual_nodes() const {
        if (initial_nodes.size() <= 3) return 200;
        if (initial_nodes.size() <= 10) return 150;
        return 100;
    }
};

}  // namespace trelliskv
