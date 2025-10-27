#pragma once

#include "trelliskv/thread_pool.h"

#include <atomic>
#include <cstdint>
#include <functional>
#include <string>
#include <thread>
#include <vector>

namespace trelliskv {

class StorageEngine;

class NetworkManager {
  public:
    NetworkManager();
    ~NetworkManager();

    bool start(uint16_t port, StorageEngine* storage);

    void stop();

    bool is_running() const { return running_; }

  private:
    void accept_loop(uint16_t port);
    void handle_client(int client_fd);
    std::string process_request(const std::string& request_json);

    std::atomic<bool> running_{false};
    std::thread accept_thread_{};
    std::unique_ptr<ThreadPool> thread_pool_;
    StorageEngine* storage_{nullptr};
    int listen_fd_{-1};
};

} // namespace trelliskv
