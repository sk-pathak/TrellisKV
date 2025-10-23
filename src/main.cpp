#include "trelliskv/logger.h"
#include "trelliskv/network_manager.h"
#include "trelliskv/storage_engine.h"

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

    trelliskv::StorageEngine storage;
    logger.info("StorageEngine initialized");

    trelliskv::NetworkManager net;
    const uint16_t port = 5000;

    if (!net.start(port, &storage)) {
        logger.error("Failed to start NetworkManager");
        return 1;
    }

    logger.info("Server started on port " + std::to_string(port));
    logger.info("Ready to handle GET/PUT/DELETE requests");
    logger.info("Press Ctrl+C to stop");

    while (!g_stop) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    logger.info("Shutting down...");
    net.stop();
    logger.info("Goodbye!");

    return 0;
}
