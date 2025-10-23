#include "trelliskv/logger.h"
#include "trelliskv/network_manager.h"

#include <chrono>
#include <csignal>
#include <thread>

static volatile std::sig_atomic_t g_stop = 0;
static trelliskv::NetworkManager* g_net_ptr = nullptr;

void handle_sigint(int) {
    g_stop = 1;
    if (g_net_ptr) {
        g_net_ptr->stop();
    }
}

int main() {
    auto &logger = trelliskv::Logger::instance();
    logger.info("TrellisKV - TCP server");

    trelliskv::NetworkManager net;
    g_net_ptr = &net;
    
    std::signal(SIGINT, handle_sigint);

    const uint16_t port = 5000;

    net.start(port);

    logger.info("Press Ctrl+C to stop");
    while (!g_stop) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    logger.info("Shutting down...");
    net.stop();
    g_net_ptr = nullptr;

    return 0;
}
