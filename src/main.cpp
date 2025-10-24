#include "trelliskv/logger.h"
#include "trelliskv/network_manager.h"
#include "trelliskv/storage_engine.h"
#include "trelliskv/trellis_node.h"

#include <chrono>
#include <csignal>
#include <thread>

static volatile std::sig_atomic_t g_stop = 0;

void handle_sigint(int) {
    g_stop = 1;
}

int main() {
    auto &logger = trelliskv::Logger::instance();
    logger.info("TrellisKV - Distributed Key-Value Store");

    std::signal(SIGINT, handle_sigint);

    trelliskv::TrellisNode node;
    const uint16_t port = 5000;

    if (!node.start(port)) {
        logger.error("Failed to start TrellisNode");
        return 1;
    }

    logger.info("Server started on port " + std::to_string(port));
    logger.info("Ready to handle GET/PUT/DELETE requests");
    logger.info("Press Ctrl+C to stop");

    while (!g_stop) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    logger.info("Shutting down...");
    node.stop();
    logger.info("Goodbye!");

    return 0;
}
