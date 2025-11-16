#include "trelliskv/trellis_node.h"

#include <random>
#include <sstream>

#include "trelliskv/connection_pool.h"
#include "trelliskv/gossip_protocol.h"
#include "trelliskv/hash_ring.h"
#include "trelliskv/logger.h"
#include "trelliskv/messages.h"
#include "trelliskv/network_manager.h"
#include "trelliskv/node_config.h"
#include "trelliskv/node_info.h"
#include "trelliskv/request_router.h"
#include "trelliskv/storage_engine.h"

namespace trelliskv {

TrellisNode::TrellisNode(const NodeId& node_id, const NodeConfig& config)
    : node_id_(node_id), config_(config), running_(false) {
    network_manager_ = std::make_unique<NetworkManager>(config_.address.port);
    storage_engine_ = std::make_unique<StorageEngine>();
    hash_ring_ = std::make_unique<HashRing>();
    connection_pool_ = std::make_unique<ConnectionPool>(10);

    request_router_ = std::make_unique<RequestRouter>(
        node_id_, hash_ring_.get(), connection_pool_.get(),
        config_.replication_factor);

    NodeInfo local_node_info(node_id_, config_.address, NodeState::ACTIVE);
    gossip_protocol_ = std::make_unique<GossipProtocol>(
        node_id_, local_node_info, network_manager_.get(),
        config_.heartbeat_interval, config_.failure_timeout);

    network_manager_->set_message_handler(
        [this](const Request& request) -> std::unique_ptr<Response> {
            return handle_request(request);
        });
}

TrellisNode::~TrellisNode() {
    if (gossip_protocol_) {
        gossip_protocol_->stop();
    }

    if (gossip_protocol_) {
        gossip_protocol_->set_node_failure_callback(nullptr);
        gossip_protocol_->set_node_recovery_callback(nullptr);
        gossip_protocol_->set_cluster_change_callback(nullptr);
        gossip_protocol_->set_node_join_callback(nullptr);
    }

    if (network_manager_) {
        network_manager_->stop_server();
    }

    if (connection_pool_) {
        connection_pool_->close_all_connections();
    }
}

NodeConfig TrellisNode::create_default_config(const std::string& hostname,
                                              uint16_t port) {
    NodeConfig config;
    config.address = NodeAddress(hostname, port);
    config.replication_factor = 3;
    config.virtual_nodes_per_physical = 50;
    config.default_consistency = ConsistencyLevel::EVENTUAL;
    config.heartbeat_interval = std::chrono::milliseconds(1000);
    config.failure_timeout = std::chrono::milliseconds(5000);
    return config;
}

Result<void> TrellisNode::start() {
    if (running_.load()) {
        return Result<void>::error("Node is already running");
    }

    auto start_result = network_manager_->start_server(config_.address.port);
    if (!start_result) {
        return Result<void>::error("Failed to start network manager: " +
                                   start_result.error());
    }

    NodeInfo self_info;
    self_info.id = node_id_;
    self_info.address = config_.address;
    hash_ring_->add_node(self_info);

    gossip_protocol_->set_node_failure_callback([this](const NodeId& node_id) {
        hash_ring_->remove_node(node_id);
        auto node_info = hash_ring_->get_node_info(node_id);
        if (node_info) {
            connection_pool_->close_connections_to_node(node_info->address);
        }
    });

    gossip_protocol_->set_node_join_callback(
        [this](const NodeInfo& node_info) { hash_ring_->add_node(node_info); });

    gossip_protocol_->set_hash_ring(hash_ring_.get());

    gossip_protocol_->set_cluster_change_callback(
        [this](const ClusterState& state) {
            for (const auto& [id, info] : state.nodes) {
                if (id == node_id_) continue;
                if (info.is_active()) {
                    if (!hash_ring_->get_node_info(id).has_value()) {
                        hash_ring_->add_node(info);
                    }
                } else if (info.is_failed()) {
                    hash_ring_->remove_node(id);
                }
            }
        });

    gossip_protocol_->start();

    running_.store(true);
    start_time_ = std::chrono::steady_clock::now();
    return Result<void>::success();
}

void TrellisNode::stop() {
    if (!running_.load()) {
        return;
    }

    running_.store(false);

    if (gossip_protocol_) {
        gossip_protocol_->stop();
    }

    if (network_manager_) {
        network_manager_->stop_server();
    }

    if (connection_pool_) {
        connection_pool_->close_all_connections();
    }
}

bool TrellisNode::is_running() const { return running_.load(); }

const NodeConfig& TrellisNode::get_config() const { return config_; }

const NodeId& TrellisNode::get_node_id() const { return node_id_; }

NodeInfo TrellisNode::get_node_info() const {
    NodeInfo info;
    info.id = node_id_;
    info.address = config_.address;
    return info;
}

Result<void> TrellisNode::join_cluster() {
    if (config_.seed_nodes.empty()) {
        return Result<void>::success();
    }

    auto discovery_result = discover_cluster_from_seeds();
    if (!discovery_result) {
        return Result<void>::error("Failed to discover cluster: " +
                                   discovery_result.error());
    }

    auto bootstrap_result = bootstrap_from_cluster();
    if (!bootstrap_result) {
        return Result<void>::error("Failed to bootstrap from cluster: " +
                                   bootstrap_result.error());
    }

    NodeInfo local_info = get_node_info();
    gossip_protocol_->add_node(local_info);

    return Result<void>::success();
}

void TrellisNode::add_node(const NodeInfo& node) {
    if (hash_ring_) {
        hash_ring_->add_node(node);
    }
    if (gossip_protocol_) {
        gossip_protocol_->add_node(node);
    }
}

void TrellisNode::remove_node(const NodeId& node_id) {
    if (hash_ring_) {
        hash_ring_->remove_node(node_id);
    }
    if (gossip_protocol_) {
        gossip_protocol_->remove_node(node_id);
    }

    auto node_info = hash_ring_->get_node_info(node_id);
    if (node_info) {
        connection_pool_->close_connections_to_node(node_info->address);
    }
}

TrellisNode::ClusterStats TrellisNode::get_stats() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    auto stats = stats_;
    stats.total_nodes = hash_ring_->size();
    stats.active_connections = connection_pool_->get_stats().total_connections;
    stats.local_keys = storage_engine_->size();
    return stats;
}

std::unique_ptr<Response> TrellisNode::handle_request(const Request& request) {
    if (!running_.load()) {
        return std::make_unique<Response>(
            Response::error("Node is not running"));
    }

    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        stats_.total_requests_handled++;
    }

    if (const auto* get_req = dynamic_cast<const GetRequest*>(&request)) {
        auto resp = handle_get_request(*get_req);
        return std::make_unique<Response>(resp);
    }
    if (const auto* put_req = dynamic_cast<const PutRequest*>(&request)) {
        auto resp = handle_put_request(*put_req);
        return std::make_unique<Response>(resp);
    }
    if (const auto* delete_req = dynamic_cast<const DeleteRequest*>(&request)) {
        auto resp = handle_delete_request(*delete_req);
        return std::make_unique<Response>(resp);
    }
    if (const auto* discovery_req =
            dynamic_cast<const ClusterDiscoveryRequest*>(&request)) {
        auto resp = handle_cluster_discovery_request(*discovery_req);
        return resp;
    }
    if (const auto* bootstrap_req =
            dynamic_cast<const BootstrapRequest*>(&request)) {
        auto resp = handle_bootstrap_request(*bootstrap_req);
        return resp;
    }
    if (const auto* heartbeat_req =
            dynamic_cast<const HeartbeatRequest*>(&request)) {
        auto resp = std::make_unique<HeartbeatResponse>(
            heartbeat_req->sequence_number, NodeState::ACTIVE);
        resp->request_id = heartbeat_req->request_id;
        resp->responder_id = node_id_;

        auto sender_info =
            gossip_protocol_->get_node_info(heartbeat_req->sender_id);
        if (sender_info) {
            gossip_protocol_->handle_heartbeat(*heartbeat_req,
                                               sender_info->address);
        }

        return resp;
    }
    if (const auto* health_req =
            dynamic_cast<const HealthCheckRequest*>(&request)) {
        return handle_health_check_request(*health_req);
    }
    LOG_WARN("Unknown request type received");
    return std::make_unique<Response>(Response::error("Unknown request type"));
}

Response TrellisNode::handle_get_request(const GetRequest& request) {
    const bool is_local = request_router_->is_key_local(request.key);

    if (!is_local) {
        auto forward_result = request_router_->forward_get_request(request);
        if (forward_result) {
            return *forward_result.value();
        } else {
            Response response = Response::error("Failed to forward request: " +
                                                forward_result.error());
            response.request_id = request.request_id;
            response.responder_id = node_id_;
            return response;
        }
    }

    auto result = storage_engine_->get(request.key);

    if (result) {
        const auto& versioned_value = result.value();
        Response response =
            Response::success(versioned_value.value, versioned_value.version);
        response.request_id = request.request_id;
        response.responder_id = node_id_;
        return response;
    } else {
        if (is_local && request.consistency == ConsistencyLevel::EVENTUAL) {
            auto forward_result = request_router_->forward_get_request(request);
            if (forward_result && forward_result.value()->is_success()) {
                return *forward_result.value();
            }
            try {
                std::vector<NodeId> tried =
                    request_router_->get_replica_nodes_for_key(request.key);
                tried.push_back(
                    request_router_->get_primary_node_for_key(request.key));
                auto active = gossip_protocol_->get_active_nodes();
                for (const auto& nid : active) {
                    if (nid == node_id_) continue;
                    if (std::find(tried.begin(), tried.end(), nid) !=
                        tried.end())
                        continue;
                    auto alt =
                        request_router_->forward_request_to_node(nid, request);
                    if (alt && alt.value()->is_success()) {
                        return *alt.value();
                    }
                }
            } catch (...) {
                // Best-effort fallback,just ignore errors
            }
        }
        Response response = Response::not_found();
        response.request_id = request.request_id;
        response.responder_id = node_id_;
        return response;
    }
}

Response TrellisNode::handle_put_request(const PutRequest& request) {
    const bool is_local = request_router_->is_key_local(request.key);

    if (!is_local) {
        auto forward_result = request_router_->forward_put_request(request);
        if (forward_result) {
            return *forward_result.value();
        } else {
            Response response = Response::error("Failed to forward request: " +
                                                forward_result.error());
            response.request_id = request.request_id;
            response.responder_id = node_id_;
            return response;
        }
    }

    TimestampVersion new_version = TimestampVersion::now(node_id_);
    VersionedValue versioned_value(request.value, new_version, node_id_);

    if (request.expected_version.has_value()) {
        auto current = storage_engine_->get(request.key);
        if (current) {
            if (!current.value().version.equals(
                    request.expected_version.value())) {
                Response response = Response::conflict();
                response.request_id = request.request_id;
                response.responder_id = node_id_;
                response.error_message = "Version mismatch";
                return response;
            }
        }
    }

    auto result = storage_engine_->put(request.key, versioned_value);
    if (result) {
        Response response = Response::success("", new_version);
        response.request_id = request.request_id;
        response.responder_id = node_id_;
        return response;
    } else {
        Response response =
            Response::error("Failed to write value: " + result.error());
        response.request_id = request.request_id;
        response.responder_id = node_id_;
        return response;
    }
}

Response TrellisNode::handle_delete_request(const DeleteRequest& request) {
    const bool is_local = request_router_->is_key_local(request.key);

    if (!is_local) {
        auto forward_result = request_router_->forward_delete_request(request);
        if (forward_result) {
            return *forward_result.value();
        } else {
            Response response = Response::error("Failed to forward request: " +
                                                forward_result.error());
            response.request_id = request.request_id;
            response.responder_id = node_id_;
            return response;
        }
    }

    auto result = storage_engine_->remove(request.key);
    if (result) {
        Response response =
            result.value() ? Response::success() : Response::not_found();
        response.request_id = request.request_id;
        response.responder_id = node_id_;
        return response;
    } else {
        Response response =
            Response::error("Failed to delete value: " + result.error());
        response.request_id = request.request_id;
        response.responder_id = node_id_;
        return response;
    }
}

std::unique_ptr<Response> TrellisNode::handle_health_check_request(
    const HealthCheckRequest& request) {
    auto response = std::make_unique<HealthCheckResponse>();

    response->request_id = request.request_id;
    response->responder_id = node_id_;
    response->status = ResponseStatus::OK;
    response->node_id = node_id_;
    response->node_state =
        running_.load() ? NodeState::ACTIVE : NodeState::FAILED;
    response->is_healthy = running_.load();

    if (running_.load()) {
        auto now = std::chrono::steady_clock::now();
        auto uptime_duration =
            std::chrono::duration_cast<std::chrono::seconds>(now - start_time_);
        auto hours =
            std::chrono::duration_cast<std::chrono::hours>(uptime_duration)
                .count();
        auto minutes = std::chrono::duration_cast<std::chrono::minutes>(
                           uptime_duration % std::chrono::hours(1))
                           .count();
        auto seconds = (uptime_duration % std::chrono::minutes(1)).count();

        std::stringstream uptime_ss;
        uptime_ss << hours << "h " << minutes << "m " << seconds << "s";
        response->uptime = uptime_ss.str();
    } else {
        response->uptime = "0s";
    }

    if (request.include_details) {
        response->total_nodes = hash_ring_->size();
        response->active_connections =
            connection_pool_->get_stats().total_connections;
        response->local_keys = storage_engine_->size();

        std::lock_guard<std::mutex> lock(stats_mutex_);
        response->total_requests = stats_.total_requests_handled;
    }

    return response;
}

std::string TrellisNode::generate_request_id() const {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_int_distribution<uint64_t> dis;

    uint64_t id = dis(gen);
    std::stringstream ss;
    ss << std::hex << id;
    return ss.str();
}

Result<void> TrellisNode::discover_cluster_from_seeds() {
    std::vector<ClusterState> discovered_states;
    size_t successful_contacts = 0;

    for (const auto& seed_address : config_.seed_nodes) {
        auto cluster_state_result = contact_seed_node(seed_address);
        if (cluster_state_result) {
            discovered_states.push_back(cluster_state_result.value());
            successful_contacts++;
        }
    }

    if (successful_contacts == 0) {
        return Result<void>::error(
            "Failed to contact any seed nodes for cluster discovery");
    }

    for (const auto& state : discovered_states) {
        merge_discovered_cluster_state(state);
    }

    return Result<void>::success();
}

Result<ClusterState> TrellisNode::contact_seed_node(
    const NodeAddress& seed_address) {
    try {
        ClusterDiscoveryRequest discovery_request;
        discovery_request.requesting_node_id = node_id_;
        discovery_request.requesting_node_address = config_.address;
        discovery_request.request_id = generate_request_id();

        auto response_result = connection_pool_->send_request(
            seed_address, discovery_request, std::chrono::milliseconds(5000));

        if (!response_result) {
            return Result<ClusterState>::error(
                "Failed to send discovery request: " + response_result.error());
        }

        const auto& response = response_result.value();
        if (response->status != ResponseStatus::OK) {
            return Result<ClusterState>::error("Discovery request failed: " +
                                               response->error_message);
        }

        if (const auto* discovery_response =
                dynamic_cast<const ClusterDiscoveryResponse*>(response.get())) {
            if (discovery_response->cluster_state) {
                return Result<ClusterState>::success(
                    *discovery_response->cluster_state);
            } else {
                return Result<ClusterState>::error(
                    "Null cluster state in discovery response");
            }
        } else {
            return Result<ClusterState>::error(
                "Invalid response type for cluster discovery");
        }

    } catch (const std::exception& e) {
        return Result<ClusterState>::error(
            "Exception during seed node contact: " + std::string(e.what()));
    }
}

void TrellisNode::merge_discovered_cluster_state(
    const ClusterState& discovered_state) {
    for (const auto& [node_id, node_info] : discovered_state.nodes) {
        if (node_id == node_id_) {
            continue;
        }

        if (node_info.is_active()) {
            hash_ring_->add_node(node_info);
        }
    }
}

Result<void> TrellisNode::bootstrap_from_cluster() {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    auto active_nodes = hash_ring_->get_all_nodes();
    if (active_nodes.empty()) {
        return Result<void>::success();
    }

    size_t successful_syncs = 0;
    const size_t min_successful_syncs =
        std::min(static_cast<size_t>(2), active_nodes.size());
    const size_t max_retry_attempts = 3;

    for (const auto& node_id : active_nodes) {
        if (node_id == node_id_) {
            continue;
        }

        bool node_success = false;
        for (size_t attempt = 0; attempt < max_retry_attempts && !node_success;
             ++attempt) {
            try {
                if (attempt > 0) {
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(1000 * attempt));
                }

                BootstrapRequest bootstrap_request;
                bootstrap_request.requesting_node_id = node_id_;
                bootstrap_request.requesting_node_address = config_.address;
                bootstrap_request.request_id = generate_request_id();

                auto node_info = hash_ring_->get_node_info(node_id);
                if (!node_info) {
                    break;
                }

                auto timeout = (attempt == 0) ? std::chrono::milliseconds(10000)
                                              : std::chrono::milliseconds(5000);
                auto response_result = connection_pool_->send_request(
                    node_info->address, bootstrap_request, timeout);

                if (response_result) {
                    const auto& response = response_result.value();
                    if (response->status == ResponseStatus::OK) {
                        successful_syncs++;
                        node_success = true;

                        if (const auto* bootstrap_response =
                                dynamic_cast<const BootstrapResponse*>(
                                    response.get())) {
                            if (bootstrap_response->cluster_state) {
                                merge_discovered_cluster_state(
                                    *bootstrap_response->cluster_state);

                                for (const auto& peer_id :
                                     bootstrap_response->recommended_peers) {
                                    auto peer_info =
                                        bootstrap_response->cluster_state->nodes
                                            .find(peer_id);
                                    if (peer_info !=
                                        bootstrap_response->cluster_state->nodes
                                            .end()) {
                                    }
                                }
                            }
                        }
                    }
                }

            } catch (const std::exception& e) {
            }
        }

        if (successful_syncs >= min_successful_syncs) {
            break;
        }
    }

    if (successful_syncs == 0) {
        return Result<void>::error(
            "Failed to bootstrap from any cluster nodes after retries");
    }

    return Result<void>::success();
}

std::unique_ptr<Response> TrellisNode::handle_cluster_discovery_request(
    const ClusterDiscoveryRequest& request) {
    try {
        if (request.requesting_node_id.empty()) {
            return std::make_unique<Response>(Response::error(
                "Invalid cluster discovery request: missing node ID"));
        }

        if (request.requesting_node_address.hostname.empty() ||
            request.requesting_node_address.port == 0) {
            return std::make_unique<Response>(Response::error(
                "Invalid cluster discovery request: invalid node address"));
        }

        ClusterState cluster_state;
        auto all_nodes = hash_ring_->get_all_nodes();
        for (const auto& node_id : all_nodes) {
            auto node_info_opt = hash_ring_->get_node_info(node_id);
            if (node_info_opt) {
                cluster_state.nodes[node_id] = *node_info_opt;
            }
        }

        auto requesting_node_it =
            cluster_state.nodes.find(request.requesting_node_id);
        if (requesting_node_it == cluster_state.nodes.end()) {
            NodeInfo requesting_node(request.requesting_node_id,
                                     request.requesting_node_address,
                                     NodeState::ACTIVE);
            cluster_state.nodes[request.requesting_node_id] = requesting_node;
            hash_ring_->add_node(requesting_node);
        } else {
            requesting_node_it->second.address =
                request.requesting_node_address;
            requesting_node_it->second.update_last_seen();
        }

        auto cluster_state_ptr = std::make_shared<ClusterState>(cluster_state);
        ClusterDiscoveryResponse discovery_response(cluster_state_ptr, node_id_,
                                                    cluster_state.nodes.size());
        discovery_response.status = ResponseStatus::OK;
        discovery_response.request_id = request.request_id;
        discovery_response.responder_id = node_id_;

        return std::make_unique<ClusterDiscoveryResponse>(
            std::move(discovery_response));

    } catch (const std::exception& e) {
        return std::make_unique<Response>(
            Response::error("Failed to process cluster discovery request: " +
                            std::string(e.what())));
    }
}

std::unique_ptr<Response> TrellisNode::handle_bootstrap_request(
    const BootstrapRequest& request) {
    try {
        if (request.requesting_node_id.empty()) {
            return std::make_unique<Response>(
                Response::error("Invalid bootstrap request: missing node ID"));
        }

        if (request.requesting_node_address.hostname.empty() ||
            request.requesting_node_address.port == 0) {
            return std::make_unique<Response>(Response::error(
                "Invalid bootstrap request: invalid node address"));
        }

        ClusterState cluster_state;
        auto all_nodes = hash_ring_->get_all_nodes();
        for (const auto& node_id : all_nodes) {
            auto node_info_opt = hash_ring_->get_node_info(node_id);
            if (node_info_opt) {
                cluster_state.nodes[node_id] = *node_info_opt;
            }
        }

        auto requesting_node_it =
            cluster_state.nodes.find(request.requesting_node_id);
        if (requesting_node_it == cluster_state.nodes.end()) {
            NodeInfo requesting_node(request.requesting_node_id,
                                     request.requesting_node_address,
                                     NodeState::ACTIVE);
            cluster_state.nodes[request.requesting_node_id] = requesting_node;
            hash_ring_->add_node(requesting_node);
        } else {
            requesting_node_it->second.address =
                request.requesting_node_address;
            requesting_node_it->second.state = NodeState::ACTIVE;
            requesting_node_it->second.update_last_seen();
        }

        auto cluster_state_ptr = std::make_shared<ClusterState>(cluster_state);
        BootstrapResponse bootstrap_response(cluster_state_ptr, node_id_);
        bootstrap_response.status = ResponseStatus::OK;
        bootstrap_response.request_id = request.request_id;
        bootstrap_response.responder_id = node_id_;

        auto active_nodes = hash_ring_->get_all_nodes();
        size_t max_recommended_peers =
            std::min(static_cast<size_t>(5), active_nodes.size());
        size_t added_peers = 0;

        for (const auto& node_id : active_nodes) {
            if (node_id != node_id_ && node_id != request.requesting_node_id &&
                added_peers < max_recommended_peers) {
                bootstrap_response.recommended_peers.push_back(node_id);
                added_peers++;
            }
        }

        return std::make_unique<BootstrapResponse>(
            std::move(bootstrap_response));

    } catch (const std::exception& e) {
        return std::make_unique<Response>(Response::error(
            "Failed to process bootstrap request: " + std::string(e.what())));
    }
}

};  // namespace trelliskv
