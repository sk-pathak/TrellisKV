#include "trelliskv/hash_ring.h"

#include <cstdint>
#include <unordered_set>

namespace trelliskv {

HashRing::HashRing(size_t virtual_nodes_per_physical)
    : virtual_nodes_per_physical_(virtual_nodes_per_physical) {}

void HashRing::add_node(const NodeInfo& node) {
    std::lock_guard<std::mutex> lock(mutex_);

    nodes_[node.id] = node;

    for (size_t i = 0; i < virtual_nodes_per_physical_; ++i) {
        uint64_t hash = hash_node(node.id, i);
        ring_[hash] = node.id;
    }
}

bool HashRing::remove_node(const NodeId& node_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto node_it = nodes_.find(node_id);
    if (node_it == nodes_.end()) {
        return false;
    }

    for (size_t i = 0; i < virtual_nodes_per_physical_; ++i) {
        uint64_t hash = hash_node(node_id, i);
        ring_.erase(hash);
    }

    nodes_.erase(node_it);

    return true;
}

NodeId HashRing::get_primary_node(const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);

    if (ring_.empty()) {
        return "";
    }

    uint64_t hash = hash_key(key);

    auto it = ring_.lower_bound(hash);

    if (it == ring_.end()) {
        it = ring_.begin();
    }

    return it->second;
}

std::vector<NodeId> HashRing::get_replica_nodes(const std::string& key,
                                                size_t count) const {
    std::lock_guard<std::mutex> lock(mutex_);

    if (ring_.empty() || count == 0) {
        return {};
    }

    uint64_t hash = hash_key(key);
    return find_next_nodes(hash, count);
}

std::vector<NodeId> HashRing::get_all_nodes() const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<NodeId> result;
    result.reserve(nodes_.size());

    for (const auto& pair : nodes_) {
        result.push_back(pair.first);
    }

    return result;
}

std::optional<NodeInfo> HashRing::get_node_info(const NodeId& node_id) const {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = nodes_.find(node_id);
    if (it != nodes_.end()) {
        return it->second;
    }

    return std::nullopt;
}

bool HashRing::empty() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return nodes_.empty();
}

size_t HashRing::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return nodes_.size();
}

size_t HashRing::virtual_node_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return ring_.size();
}

uint64_t HashRing::hash_key(const std::string& key) const {
    static constexpr uint64_t FNV_OFFSET_BASIS = 1469598103934665603ULL;
    static constexpr uint64_t FNV_PRIME = 1099511628211ULL;
    uint64_t hash = FNV_OFFSET_BASIS;
    for (unsigned char c : key) {
        hash ^= static_cast<uint64_t>(c);
        hash *= FNV_PRIME;
    }
    return hash;
}

uint64_t HashRing::hash_node(const NodeId& node_id,
                             size_t virtual_index) const {
    static constexpr uint64_t FNV_OFFSET_BASIS = 1469598103934665603ULL;
    static constexpr uint64_t FNV_PRIME = 1099511628211ULL;
    std::string virtual_node_key =
        node_id + ":" + std::to_string(virtual_index);
    uint64_t hash = FNV_OFFSET_BASIS;
    for (unsigned char c : virtual_node_key) {
        hash ^= static_cast<uint64_t>(c);
        hash *= FNV_PRIME;
    }
    return hash;
}

std::vector<NodeId> HashRing::find_next_nodes(uint64_t start_position,
                                              size_t count) const {
    if (ring_.empty()) {
        return {};
    }

    std::vector<NodeId> result;
    std::unordered_set<NodeId> seen_nodes;

    auto it = ring_.lower_bound(start_position);

    if (it == ring_.end()) {
        it = ring_.begin();
    }

    auto start_it = it;
    bool wrapped = false;
    size_t iterations = 0;
    const size_t max_iterations = ring_.size() * 2;

    while (result.size() < count && iterations < max_iterations) {
        const NodeId& node_id = it->second;

        if (seen_nodes.find(node_id) == seen_nodes.end()) {
            result.push_back(node_id);
            seen_nodes.insert(node_id);
        }

        ++it;
        iterations++;

        if (it == ring_.end()) {
            it = ring_.begin();
            wrapped = true;
        }

        if (wrapped && it == start_it) {
            break;
        }

        if (seen_nodes.size() == nodes_.size()) {
            break;
        }
    }

    return result;
}

}  // namespace trelliskv