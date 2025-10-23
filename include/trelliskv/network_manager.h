#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <string>
#include <thread>
#include <vector>

namespace trelliskv {

using RequestHandler = std::function<std::string(const std::string&)>;

class NetworkManager {
  public:
    NetworkManager();
    ~NetworkManager();

    bool start(uint16_t port, RequestHandler handler = nullptr);

    void stop();

    bool is_running() const { return running_; }

  private:
    void accept_loop(uint16_t port);
    std::string handle_connection(int client_fd);

    std::atomic<bool> running_{false};
    std::thread accept_thread_{};
    RequestHandler handler_{}; // if null, echo back input
    int listen_fd_{-1};
};

} // namespace trelliskv
