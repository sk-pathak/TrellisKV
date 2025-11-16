#pragma once

#include <string>

#include "trelliskv/types.h"

namespace trelliskv {

struct NodeAddress {
    std::string hostname;
    uint16_t port;

    NodeAddress() = default;
    NodeAddress(const std::string& host, uint16_t p)
        : hostname(host), port(p) {}

    std::string to_string() const {
        return hostname + ":" + std::to_string(port);
    }

    bool operator==(const NodeAddress& other) const {
        return hostname == other.hostname && port == other.port;
    }
};

struct NodeInfo {
    NodeId id;
    NodeAddress address;
    NodeState state;
    Timestamp last_seen;

    NodeInfo() = default;
    NodeInfo(const NodeId& node_id, const NodeAddress& addr,
             NodeState s = NodeState::ACTIVE)
        : id(node_id),
          address(addr),
          state(s),
          last_seen(std::chrono::system_clock::now()) {}

    void update_last_seen() { last_seen = std::chrono::system_clock::now(); }

    bool is_active() const { return state == NodeState::ACTIVE; }

    bool is_failed() const { return state == NodeState::FAILED; }
};

}  // namespace trelliskv