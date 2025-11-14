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

Result<void> TrellisNode::join_cluster() { return Result<void>::success(); }

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

}  // namespace trelliskv
