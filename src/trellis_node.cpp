#include "trelliskv/trellis_node.h"

#include "trelliskv/logger.h"

namespace trelliskv {

TrellisNode::TrellisNode(const NodeConfig& config)
    : config_(config),
      storage_(std::make_unique<StorageEngine>()),
      network_(std::make_unique<NetworkManager>()) {
    auto& logger = Logger::instance();
    logger.info("TrellisNode created with node_id: " + config_.node_id);
    logger.info("Configuration - Host: " + config_.hostname +
                ", Port: " + std::to_string(config_.port));
}

TrellisNode::~TrellisNode() { stop(); }

bool TrellisNode::start() {
    auto& logger = Logger::instance();
    logger.info("Starting TrellisNode on " + config_.hostname + ":" +
                std::to_string(config_.port));

    if (!network_->start(config_.port, storage_.get(), config_.node_id)) {
        logger.error("Failed to start network manager");
        return false;
    }

    logger.info("TrellisNode successfully started");
    return true;
}

void TrellisNode::stop() {
    auto& logger = Logger::instance();
    logger.info("Stopping TrellisNode");

    if (network_) {
        network_->stop();
    }

    logger.info("TrellisNode stopped");
}

bool TrellisNode::is_running() const {
    return network_ && network_->is_running();
}

std::string TrellisNode::handle_request(const std::string& request_json) {
    auto& logger = Logger::instance();
    logger.info("TrellisNode handling request");
    // handle request_json
    (void)request_json;  // Mark as intentionally unused
    return "";
}

}  // namespace trelliskv
