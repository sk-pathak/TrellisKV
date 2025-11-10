#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "node_info.h"
#include "types.h"

namespace trelliskv {

class HashRing {
   public:
    explicit HashRing(size_t virtual_nodes_per_physical = 50);
    void add_node(const NodeInfo& node);
    bool remove_node(const NodeId& node_id);
    NodeId get_primary_node(const std::string& key) const;

    std::vector<NodeId> get_replica_nodes(const std::string& key,
                                          size_t count) const;
    std::vector<NodeId> get_all_nodes() const;
    std::optional<NodeInfo> get_node_info(const NodeId& node_id) const;

    bool empty() const;
    size_t size() const;
    size_t virtual_node_count() const;

   private:
    uint64_t hash_key(const std::string& key) const;
    uint64_t hash_node(const NodeId& node_id, size_t virtual_index) const;
    std::vector<NodeId> find_next_nodes(uint64_t start_position,
                                        size_t count) const;

   private:
    std::map<uint64_t, NodeId> ring_;
    std::unordered_map<NodeId, NodeInfo> nodes_;
    size_t virtual_nodes_per_physical_;
    mutable std::mutex mutex_;
    std::hash<std::string> hasher_;
};

}  // namespace trelliskv