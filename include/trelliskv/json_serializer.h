#pragma once

#include <string>

#include "messages.h"
#include "nlohmann/json.hpp"
#include "result.h"
#include "types.h"

namespace trelliskv {

struct Request;
struct Response;
struct GetRequest;
struct PutRequest;
struct DeleteRequest;
struct ClusterDiscoveryRequest;
struct BootstrapRequest;
struct HeartbeatRequest;
struct HealthCheckRequest;
struct GossipMessage;
struct NodeInfo;
struct NodeAddress;
struct TimestampVersion;
struct ClusterState;

class JsonSerializer {
   public:
    // Request
    static Result<std::string> serialize_request(const Request& request);
    static Result<std::unique_ptr<Request>> deserialize_request(
        const std::string& json_str);

    // Response
    static Result<std::string> serialize_response(const Response& response);
    static Result<std::unique_ptr<Response>> deserialize_response(
        const std::string& json_str);

    // Gossip message
    static Result<std::string> serialize_gossip_message(
        const GossipMessage& message);

    // Helper methods for common types
    static nlohmann::json serialize_node_info(const NodeInfo& node);
    static Result<NodeInfo> deserialize_node_info(const nlohmann::json& json);

    static nlohmann::json serialize_node_address(const NodeAddress& address);
    static Result<NodeAddress> deserialize_node_address(
        const nlohmann::json& json);
    static nlohmann::json serialize_timestamp_version(
        const TimestampVersion& version);
    static Result<TimestampVersion> deserialize_timestamp_version(
        const nlohmann::json& json);

    static nlohmann::json serialize_timestamp(const Timestamp& timestamp);
    static Result<Timestamp> deserialize_timestamp(const nlohmann::json& json);

    // Enum
    static std::string response_status_to_string(ResponseStatus status);
    static Result<ResponseStatus> string_to_response_status(
        const std::string& str);

    static std::string consistency_level_to_string(ConsistencyLevel level);
    static Result<ConsistencyLevel> string_to_consistency_level(
        const std::string& str);

    static std::string node_state_to_string(NodeState state);
    static Result<NodeState> string_to_node_state(const std::string& str);

    static nlohmann::json serialize_cluster_state(
        const ClusterState& cluster_state);
    static Result<ClusterState> deserialize_cluster_state(
        const nlohmann::json& json);
};

}  // namespace trelliskv
