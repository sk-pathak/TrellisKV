#include <chrono>
#include <csignal>
#include <cstring>
#include <iostream>
#include <thread>

#include "trelliskv/logger.h"
#include "trelliskv/trellis_node.h"

static volatile std::sig_atomic_t g_stop = 0;

void handle_sigint(int) { g_stop = 1; }

void print_usage(const char* program_name) {
    std::cout << "Usage: " << program_name << " [OPTIONS]\n";
    std::cout << "\nOptions:\n";
    std::cout
        << "  --host <hostname>    Hostname to bind to (default: localhost)\n";
    std::cout << "  --port <port>        Port to listen on (default: 5000)\n";
    std::cout
        << "  --node-id <id>       Node identifier (default: <host>:<port>)\n";
    std::cout << "  --help               Show this help message\n";
    std::cout << "\nExample:\n";
    std::cout << "  " << program_name
              << " --host 0.0.0.0 --port 5001 --node-id node1\n";
}

int main(int argc, char* argv[]) {
    LOG_INFO("TrellisKV - Distributed Key-Value Store");

    std::string hostname = "localhost";
    uint16_t port = 5000;
    std::string node_id;

    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--host") == 0 && i + 1 < argc) {
            hostname = argv[++i];
        } else if (std::strcmp(argv[i], "--port") == 0 && i + 1 < argc) {
            port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if (std::strcmp(argv[i], "--node-id") == 0 && i + 1 < argc) {
            node_id = argv[++i];
        } else if (std::strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        } else {
            std::cerr << "Unknown option: " << argv[i] << "\n";
            print_usage(argv[0]);
            return 1;
        }
    }

    if (node_id.empty()) {
        node_id = hostname + ":" + std::to_string(port);
    }

    std::signal(SIGINT, handle_sigint);

    auto config = trelliskv::TrellisNode::create_default_config(hostname, port);
    trelliskv::TrellisNode node(node_id, config);

    auto start_result = node.start();
    if (!start_result) {
        LOG_ERROR("Failed to start TrellisNode: " + start_result.error());
        return 1;
    }

    LOG_INFO("Server started on " + hostname + ":" + std::to_string(port));
    LOG_INFO("Node ID: " + node_id);
    LOG_INFO("Ready to handle GET/PUT/DELETE requests");
    LOG_INFO("Press Ctrl+C to stop");

    while (!g_stop) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    LOG_INFO("Shutting down...");
    node.stop();
    LOG_INFO("Goodbye!");

    return 0;
}
