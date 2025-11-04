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
}

TrellisNode::~TrellisNode() { stop(); }

Result<void> TrellisNode::start() {
    LOG_INFO("Starting TrellisNode on " + config_.hostname + ":" +
             std::to_string(config_.port));

    if (!network_->start(config_.port, storage_.get(), config_.node_id)) {
        return Result<void>::error("Failed to start network manager");
    }

    LOG_INFO("TrellisNode successfully started");
    return Result<void>::success();
}

void TrellisNode::stop() {
    LOG_INFO("Stopping TrellisNode");

    if (network_) {
        network_->stop();
    }

    LOG_INFO("TrellisNode stopped");
}

bool TrellisNode::is_running() const {
    return network_ && network_->is_running();
}

std::string TrellisNode::handle_request(const std::string& request_json) {
    LOG_INFO("TrellisNode handling request");
    // handle request_json
    (void)request_json;  // Mark as intentionally unused
    return "";
}

}  // namespace trelliskv
