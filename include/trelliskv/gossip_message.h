#pragma once

#include <chrono>
#include <vector>

#include "nlohmann/json.hpp"
#include "node_info.h"
#include "types.h"

namespace trelliskv {

struct GossipMessage {
    NodeId sender_id;
    std::vector<NodeInfo> known_nodes;
    Timestamp timestamp;

    GossipMessage() = default;
    GossipMessage(const NodeId& sender)
        : sender_id(sender), timestamp(std::chrono::system_clock::now()) {}

    void to_json(nlohmann::json& json) const;
    void from_json(const nlohmann::json& json);
};

}  // namespace trelliskv