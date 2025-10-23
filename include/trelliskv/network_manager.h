#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <string>
#include <thread>
#include <vector>
#include <storage_engine.h>

namespace trelliskv {

using RequestHandler = std::function<std::string(const std::string&)>;

class NetworkManager {
  public:
    NetworkManager();
    ~NetworkManager();

    bool start(uint16_t port, StorageEngine* storage);

    void stop();

    bool is_running() const { return running_; }

  private:
    void accept_loop(uint16_t port);
    std::string handle_connection(int client_fd);
    std::string process_request(const std::string& request_json);

    std::atomic<bool> running_{false};
    std::thread accept_thread_{};
    StorageEngine* storage_{nullptr};
    int listen_fd_{-1};
};

} // namespace trelliskv
