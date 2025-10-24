#pragma once

#include "trelliskv/network_manager.h"
#include "trelliskv/storage_engine.h"

#include <cstdint>
#include <memory>
#include <string>

namespace trelliskv {

class TrellisNode {
  public:
    TrellisNode();
    ~TrellisNode();

    bool start(uint16_t port);

    void stop();

    bool is_running() const;

  private:
    std::string handle_request(const std::string& request_json);

    std::unique_ptr<StorageEngine> storage_;
    std::unique_ptr<NetworkManager> network_;
};

} // namespace trelliskv
