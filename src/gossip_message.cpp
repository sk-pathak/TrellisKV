#include "trelliskv/gossip_message.h"

#include "nlohmann/json.hpp"
#include "trelliskv/json_serializer.h"

namespace trelliskv {

void GossipMessage::to_json(nlohmann::json& json) const {
    json["sender_id"] = sender_id;
    json["timestamp"] = JsonSerializer::serialize_timestamp(timestamp);

    nlohmann::json nodes_array = nlohmann::json::array();
    for (const auto& node : known_nodes) {
        nodes_array.push_back(JsonSerializer::serialize_node_info(node));
    }
    json["known_nodes"] = nodes_array;
}

void GossipMessage::from_json(const nlohmann::json& json) {
    if (json.contains("sender_id")) {
        sender_id = json["sender_id"];
    }
    if (json.contains("timestamp")) {
        auto result = JsonSerializer::deserialize_timestamp(json["timestamp"]);
        if (result) {
            timestamp = result.value();
        }
    }
    if (json.contains("known_nodes") && json["known_nodes"].is_array()) {
        known_nodes.clear();
        for (const auto& node_json : json["known_nodes"]) {
            auto result = JsonSerializer::deserialize_node_info(node_json);
            if (result) {
                known_nodes.push_back(result.value());
            }
        }
    }
}

}  // namespace trelliskv
