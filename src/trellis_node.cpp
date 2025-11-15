#include "trelliskv/trellis_node.h"

#include <random>
#include <sstream>

#include "trelliskv/connection_pool.h"
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

    network_manager_->set_message_handler(
        [this](const Request& request) -> std::unique_ptr<Response> {
            return handle_request(request);
        });
}

TrellisNode::~TrellisNode() {
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

    running_.store(true);
    start_time_ = std::chrono::steady_clock::now();
    return Result<void>::success();
}

void TrellisNode::stop() {
    if (!running_.load()) {
        return;
    }

    running_.store(false);

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

    return Result<void>::success();
}

void TrellisNode::add_node(const NodeInfo& node) {
    if (hash_ring_) {
        hash_ring_->add_node(node);
    }
}

void TrellisNode::remove_node(const NodeId& node_id) {
    if (hash_ring_) {
        hash_ring_->remove_node(node_id);
    }
}

std::unique_ptr<Response> TrellisNode::handle_request(const Request& request) {
    if (!running_.load()) {
        return std::make_unique<Response>(
            Response::error("Node is not running"));
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
        if (result.error().find("not found") != std::string::npos) {
            Response response = Response::not_found();
            response.request_id = request.request_id;
            response.responder_id = node_id_;
            return response;
        } else {
            Response response =
                Response::error("Failed to read value: " + result.error());
            response.request_id = request.request_id;
            response.responder_id = node_id_;
            return response;
        }
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

    // Contact each seed node to discover cluster state
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
        // Create a cluster discovery request
        ClusterDiscoveryRequest discovery_request;
        discovery_request.requesting_node_id = node_id_;
        discovery_request.requesting_node_address = config_.address;
        discovery_request.request_id = generate_request_id();

        // Send discovery request to seed node
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

        // Parse cluster state from response
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
    // Small delay to ensure seed nodes are fully ready
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Get list of active nodes for bootstrap
    auto active_nodes = hash_ring_->get_all_nodes();
    if (active_nodes.empty()) {
        return Result<void>::success();
    }

    size_t successful_syncs = 0;
    const size_t min_successful_syncs =
        std::min(static_cast<size_t>(2), active_nodes.size());
    const size_t max_retry_attempts = 3;

    // Contact multiple nodes to get a complete view of the cluster
    for (const auto& node_id : active_nodes) {
        if (node_id == node_id_) {
            continue;  // Skip ourselves
        }

        bool node_success = false;
        for (size_t attempt = 0; attempt < max_retry_attempts && !node_success;
             ++attempt) {
            try {
                if (attempt > 0) {
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(1000 * attempt));
                }

                // Create bootstrap request
                BootstrapRequest bootstrap_request;
                bootstrap_request.requesting_node_id = node_id_;
                bootstrap_request.requesting_node_address = config_.address;
                bootstrap_request.request_id = generate_request_id();

                // Get node address
                auto node_info = hash_ring_->get_node_info(node_id);
                if (!node_info) {
                    break;  // No point retrying if we don't have the address
                }

                // Send bootstrap request with shorter timeout for retries
                auto timeout = (attempt == 0) ? std::chrono::milliseconds(10000)
                                              : std::chrono::milliseconds(5000);
                auto response_result = connection_pool_->send_request(
                    node_info->address, bootstrap_request, timeout);

                if (response_result) {
                    const auto& response = response_result.value();
                    if (response->status == ResponseStatus::OK) {
                        successful_syncs++;
                        node_success = true;

                        // Process bootstrap response
                        if (const auto* bootstrap_response =
                                dynamic_cast<const BootstrapResponse*>(
                                    response.get())) {
                            // Update our cluster state with the bootstrap
                            // information
                            if (bootstrap_response->cluster_state) {
                                merge_discovered_cluster_state(
                                    *bootstrap_response->cluster_state);

                                // Add recommended peers to our known nodes
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

        // Build cluster state from hash ring
        ClusterState cluster_state;
        auto all_nodes = hash_ring_->get_all_nodes();
        for (const auto& node_id : all_nodes) {
            auto node_info_opt = hash_ring_->get_node_info(node_id);
            if (node_info_opt) {
                cluster_state.nodes[node_id] = *node_info_opt;
            }
        }

        // Add the requesting node to our hash ring if not already present
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

        // Build cluster state from hash ring
        ClusterState cluster_state;
        auto all_nodes = hash_ring_->get_all_nodes();
        for (const auto& node_id : all_nodes) {
            auto node_info_opt = hash_ring_->get_node_info(node_id);
            if (node_info_opt) {
                cluster_state.nodes[node_id] = *node_info_opt;
            }
        }

        // Add the requesting node to our hash ring if not already present
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

        // Get active nodes from hash ring for peer recommendations
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
