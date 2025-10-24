#include "trelliskv/trellis_node.h"
#include "trelliskv/json_serializer.h"
#include "trelliskv/logger.h"
#include "trelliskv/messages.h"

namespace trelliskv {

TrellisNode::TrellisNode()
    : storage_(std::make_unique<StorageEngine>()),
      network_(std::make_unique<NetworkManager>()) {
    auto &logger = Logger::instance();
    logger.info("TrellisNode created");
}

TrellisNode::~TrellisNode() {
    stop();
}

bool TrellisNode::start(uint16_t port) {
    auto &logger = Logger::instance();
    logger.info("Starting TrellisNode on port " + std::to_string(port));

    if (!network_->start(port, storage_.get())) {
        logger.error("Failed to start network manager");
        return false;
    }

    logger.info("TrellisNode successfully started");
    return true;
}

void TrellisNode::stop() {
    auto &logger = Logger::instance();
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
    auto &logger = Logger::instance();
    logger.info("TrellisNode handling request");
    // handle request_json
    return "";
}

} // namespace trelliskv
