#include "trelliskv/logger.h"
#include "trelliskv/network_manager.h"
#include "trelliskv/storage_engine.h"
#include "trelliskv/trellis_node.h"

#include <chrono>
#include <csignal>
#include <cstring>
#include <iostream>
#include <thread>

static volatile std::sig_atomic_t g_stop = 0;

void handle_sigint(int) {
    g_stop = 1;
}

void print_usage(const char *program_name) {
    std::cout << "Usage: " << program_name << " [OPTIONS]\n";
    std::cout << "\nOptions:\n";
    std::cout << "  --host <hostname>    Hostname to bind to (default: localhost)\n";
    std::cout << "  --port <port>        Port to listen on (default: 5000)\n";
    std::cout << "  --node-id <id>       Node identifier (default: <host>:<port>)\n";
    std::cout << "  --help               Show this help message\n";
    std::cout << "\nExample:\n";
    std::cout << "  " << program_name << " --host 0.0.0.0 --port 5001 --node-id node1\n";
}

int main(int argc, char *argv[]) {
    auto &logger = trelliskv::Logger::instance();
    logger.info("TrellisKV - Distributed Key-Value Store");

    trelliskv::NodeConfig config;

    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--host") == 0 && i + 1 < argc) {
            config.hostname = argv[++i];
        } else if (std::strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            config.port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if (std::strcmp(argv[i], "--node-id") == 0 && i + 1 < argc) {
            config.node_id = argv[++i];
        } else if (std::strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        } else {
            std::cerr << "Unknown option: " << argv[i] << "\n";
            print_usage(argv[0]);
            return 1;
        }
    }

    if (config.node_id.empty()) {
        config.node_id = config.hostname + ":" + std::to_string(config.port);
    }

    std::signal(SIGINT, handle_sigint);

    trelliskv::TrellisNode node(config);

    if (!node.start()) {
        logger.error("Failed to start TrellisNode");
        return 1;
    }

    logger.info("Server started on " + config.hostname + ":" + std::to_string(config.port));
    logger.info("Node ID: " + config.node_id);
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
