#include "trelliskv/json_serializer.h"

#include "trelliskv/cluster_state.h"
#include "trelliskv/gossip_message.h"
#include "trelliskv/messages.h"

namespace trelliskv {

// Request serialization
Result<std::string> JsonSerializer::serialize_request(const Request& request) {
    try {
        nlohmann::json json;
        request.to_json(json);
        return Result<std::string>::success(json.dump());
    } catch (const std::exception& e) {
        return Result<std::string>::error("Serialization error: " +
                                          std::string(e.what()));
    }
}

Result<std::unique_ptr<Request>> JsonSerializer::deserialize_request(
    const std::string& json_str) {
    try {
        auto json = nlohmann::json::parse(json_str);

        if (!json.contains("type")) {
            return Result<std::unique_ptr<Request>>::error(
                "Missing type field");
        }

        std::string message_type = json["type"];
        std::unique_ptr<Request> request;

        // Create the appropriate request object based on type
        if (message_type == "GET") {
            request = std::make_unique<GetRequest>();
        } else if (message_type == "PUT") {
            request = std::make_unique<PutRequest>();
        } else if (message_type == "DELETE") {
            request = std::make_unique<DeleteRequest>();
        } else if (message_type == "CLUSTER_DISCOVERY") {
            request = std::make_unique<ClusterDiscoveryRequest>();
        } else if (message_type == "BOOTSTRAP") {
            request = std::make_unique<BootstrapRequest>();
        } else if (message_type == "HEARTBEAT") {
            request = std::make_unique<HeartbeatRequest>();
        } else if (message_type == "HEALTH_CHECK") {
            request = std::make_unique<HealthCheckRequest>();
        } else {
            return Result<std::unique_ptr<Request>>::error(
                "Unknown message type: " + message_type);
        }

        request->from_json(json);
        return Result<std::unique_ptr<Request>>::success(std::move(request));

    } catch (const std::exception& e) {
        return Result<std::unique_ptr<Request>>::error(
            "Deserialization error: " + std::string(e.what()));
    }
}

// Response serialization
Result<std::string> JsonSerializer::serialize_response(
    const Response& response) {
    try {
        nlohmann::json json;
        response.to_json(json);
        return Result<std::string>::success(json.dump());
    } catch (const std::exception& e) {
        return Result<std::string>::error("Serialization error: " +
                                          std::string(e.what()));
    }
}

Result<std::unique_ptr<Response>> JsonSerializer::deserialize_response(
    const std::string& json_str) {
    try {
        auto json = nlohmann::json::parse(json_str);

        std::string response_type = "RESPONSE";
        if (json.contains("type")) {
            response_type = json["type"];
        }

        std::unique_ptr<Response> response;

        if (response_type == "CLUSTER_DISCOVERY_RESPONSE") {
            response = std::make_unique<ClusterDiscoveryResponse>();
        } else if (response_type == "BOOTSTRAP_RESPONSE") {
            response = std::make_unique<BootstrapResponse>();
        } else if (response_type == "HEARTBEAT_RESPONSE") {
            response = std::make_unique<HeartbeatResponse>();
        } else if (response_type == "HEALTH_CHECK_RESPONSE") {
            response = std::make_unique<HealthCheckResponse>();
        } else {
            if (json.contains("cluster_state") ||
                json.contains("responding_node_id") ||
                json.contains("cluster_size")) {
                response = std::make_unique<ClusterDiscoveryResponse>();
            } else if (json.contains("recommended_peers")) {
                response = std::make_unique<BootstrapResponse>();
            } else {
                response = std::make_unique<Response>();
            }
        }

        response->from_json(json);

        return Result<std::unique_ptr<Response>>::success(std::move(response));

    } catch (const std::exception& e) {
        return Result<std::unique_ptr<Response>>::error(
            "Deserialization error: " + std::string(e.what()));
    }
}

// Gossip message serialization
Result<std::string> JsonSerializer::serialize_gossip_message(
    const GossipMessage& message) {
    try {
        nlohmann::json json;
        message.to_json(json);
        return Result<std::string>::success(json.dump());
    } catch (const std::exception& e) {
        return Result<std::string>::error("Serialization error: " +
                                          std::string(e.what()));
    }
}

nlohmann::json JsonSerializer::serialize_timestamp_version(
    const TimestampVersion& version) {
    return nlohmann::json{{"timestamp", version.timestamp},
                          {"last_writer", version.last_writer}};
}

Result<TimestampVersion> JsonSerializer::deserialize_timestamp_version(
    const nlohmann::json& json) {
    if (!json.is_object()) {
        return Result<TimestampVersion>::error("Version must be an object");
    }

    TimestampVersion version;

    if (json.contains("timestamp")) {
        if (json["timestamp"].is_number()) {
            version.timestamp = json["timestamp"].get<uint64_t>();
        }
    }

    if (json.contains("last_writer")) {
        version.last_writer = json["last_writer"];
    }

    return Result<TimestampVersion>::success(version);
}

nlohmann::json JsonSerializer::serialize_node_info(const NodeInfo& node) {
    return nlohmann::json{{"id", node.id},
                          {"address", serialize_node_address(node.address)},
                          {"state", node_state_to_string(node.state)},
                          {"last_seen", serialize_timestamp(node.last_seen)}};
}

Result<NodeInfo> JsonSerializer::deserialize_node_info(
    const nlohmann::json& json) {
    NodeInfo node;

    if (json.contains("id")) {
        node.id = json["id"];
    }

    if (json.contains("address")) {
        auto address_json = json["address"].get<nlohmann::json>();
        auto address_result =
            JsonSerializer::deserialize_node_address(address_json);
        if (address_result.is_success()) {
            node.address = address_result.value();
        }
    }

    if (json.contains("state")) {
        auto state_result = string_to_node_state(json["state"]);
        if (state_result) {
            node.state = state_result.value();
        }
    }

    if (json.contains("last_seen")) {
        auto timestamp_result = deserialize_timestamp(json["last_seen"]);
        if (timestamp_result) {
            node.last_seen = timestamp_result.value();
        }
    }

    return Result<NodeInfo>::success(node);
}

nlohmann::json JsonSerializer::serialize_node_address(
    const NodeAddress& address) {
    return nlohmann::json{{"hostname", address.hostname},
                          {"port", address.port}};
}

Result<NodeAddress> JsonSerializer::deserialize_node_address(
    const nlohmann::json& json) {
    NodeAddress address;

    if (json.contains("hostname")) {
        address.hostname = json["hostname"];
    }

    if (json.contains("port")) {
        address.port = json["port"];
    }

    return Result<NodeAddress>::success(address);
}

nlohmann::json JsonSerializer::serialize_timestamp(const Timestamp& timestamp) {
    auto duration = timestamp.time_since_epoch();
    auto millis =
        std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    return millis;
}

Result<Timestamp> JsonSerializer::deserialize_timestamp(
    const nlohmann::json& json) {
    if (!json.is_number()) {
        return Result<Timestamp>::error("Timestamp must be a number");
    }

    auto millis = json.get<int64_t>();
    auto duration = std::chrono::milliseconds(millis);
    auto timestamp = Timestamp(duration);

    return Result<Timestamp>::success(timestamp);
}

std::string JsonSerializer::consistency_level_to_string(
    ConsistencyLevel level) {
    assert(level ==
           ConsistencyLevel::EVENTUAL);  // No strong consistency for now
    return "eventual";
}

Result<ConsistencyLevel> JsonSerializer::string_to_consistency_level(
    const std::string& level) {
    assert(level == "eventual");  // No strong consistency for now
    return Result<ConsistencyLevel>::success(ConsistencyLevel::EVENTUAL);
}

std::string JsonSerializer::response_status_to_string(ResponseStatus status) {
    switch (status) {
        case ResponseStatus::OK:
            return "ok";
        case ResponseStatus::NOT_FOUND:
            return "not_found";
        case ResponseStatus::ERROR:
            return "error";
        case ResponseStatus::CONFLICT:
            return "conflict";
        default:
            return "error";
    }
}

Result<ResponseStatus> JsonSerializer::string_to_response_status(
    const std::string& str) {
    if (str == "ok") return Result<ResponseStatus>::success(ResponseStatus::OK);
    if (str == "not_found")
        return Result<ResponseStatus>::success(ResponseStatus::NOT_FOUND);
    if (str == "error")
        return Result<ResponseStatus>::success(ResponseStatus::ERROR);
    if (str == "conflict")
        return Result<ResponseStatus>::success(ResponseStatus::CONFLICT);
    return Result<ResponseStatus>::error("Unknown response status: " + str);
}

std::string JsonSerializer::node_state_to_string(NodeState state) {
    switch (state) {
        case NodeState::ACTIVE:
            return "active";
        case NodeState::FAILED:
            return "failed";
        default:
            return "failed";
    }
}

Result<NodeState> JsonSerializer::string_to_node_state(const std::string& str) {
    if (str == "active") return Result<NodeState>::success(NodeState::ACTIVE);
    if (str == "failed") return Result<NodeState>::success(NodeState::FAILED);
    return Result<NodeState>::error("Unknown node state: " + str);
}

// Cluster state
nlohmann::json JsonSerializer::serialize_cluster_state(
    const ClusterState& cluster_state) {
    nlohmann::json json;

    nlohmann::json nodes_json = nlohmann::json::object();
    for (const auto& [node_id, node_info] : cluster_state.nodes) {
        nodes_json[node_id] = serialize_node_info(node_info);
    }
    json["nodes"] = nodes_json;

    nlohmann::json last_heartbeat_json = nlohmann::json::object();
    for (const auto& [node_id, timestamp] : cluster_state.last_heartbeat) {
        last_heartbeat_json[node_id] = serialize_timestamp(timestamp);
    }
    json["last_heartbeat"] = last_heartbeat_json;

    nlohmann::json heartbeat_sequence_json = nlohmann::json::object();
    for (const auto& [node_id, sequence] : cluster_state.heartbeat_sequence) {
        heartbeat_sequence_json[node_id] = sequence;
    }
    json["heartbeat_sequence"] = heartbeat_sequence_json;

    json["last_updated"] = serialize_timestamp(cluster_state.last_updated);

    return json;
}

Result<ClusterState> JsonSerializer::deserialize_cluster_state(
    const nlohmann::json& json) {
    ClusterState cluster_state;

    if (json.contains("nodes") && json["nodes"].is_object()) {
        for (const auto& [node_id, node_json] : json["nodes"].items()) {
            auto node_result = deserialize_node_info(node_json);
            if (node_result) {
                cluster_state.nodes[node_id] = node_result.value();
            }
        }
    }

    if (json.contains("last_heartbeat") && json["last_heartbeat"].is_object()) {
        for (const auto& [node_id, timestamp_json] :
             json["last_heartbeat"].items()) {
            auto timestamp_result = deserialize_timestamp(timestamp_json);
            if (timestamp_result) {
                cluster_state.last_heartbeat[node_id] =
                    timestamp_result.value();
            }
        }
    }

    if (json.contains("heartbeat_sequence") &&
        json["heartbeat_sequence"].is_object()) {
        for (const auto& [node_id, sequence_json] :
             json["heartbeat_sequence"].items()) {
            if (sequence_json.is_number()) {
                cluster_state.heartbeat_sequence[node_id] =
                    sequence_json.get<uint64_t>();
            }
        }
    }

    if (json.contains("last_updated")) {
        auto timestamp_result = deserialize_timestamp(json["last_updated"]);
        if (timestamp_result) {
            cluster_state.last_updated = timestamp_result.value();
        }
    }

    return Result<ClusterState>::success(cluster_state);
}

}  // namespace trelliskv