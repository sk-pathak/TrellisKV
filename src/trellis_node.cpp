#include "trelliskv/trellis_node.h"

#include "trelliskv/logger.h"

namespace trelliskv {

TrellisNode::TrellisNode(const NodeConfig& config)
    : config_(config),
      storage_(std::make_unique<StorageEngine>()),
      network_(std::make_unique<NetworkManager>()) {
    LOG_INFO("TrellisNode created with node_id: " + config_.node_id);
    LOG_INFO("Configuration - Host: " + config_.hostname +
             ", Port: " + std::to_string(config_.port));

    network_->set_message_handler(
        [this](const Request& request) -> std::unique_ptr<Response> {
            return handle_request(request);
        });
}

TrellisNode::~TrellisNode() { stop(); }

Result<void> TrellisNode::start() {
    LOG_INFO("Starting TrellisNode on " + config_.hostname + ":" +
             std::to_string(config_.port));

    if (!network_->start_server(config_.port)) {
        return Result<void>::error("Failed to start network manager");
    }

    LOG_INFO("TrellisNode successfully started");
    return Result<void>::success();
}

void TrellisNode::stop() {
    LOG_INFO("Stopping TrellisNode");

    if (network_) {
        network_->stop_server();
    }

    LOG_INFO("TrellisNode stopped");
}

bool TrellisNode::is_running() const {
    return network_ && network_->is_server_running();
}

std::unique_ptr<Response> TrellisNode::handle_request(
    const Request& request_json) {
    LOG_INFO("TrellisNode handling request");
    // handle request_json
    (void)request_json;
    std::unique_ptr<Response> res;
    return res;
}

}  // namespace trelliskv
