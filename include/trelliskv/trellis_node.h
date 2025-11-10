#pragma once

#include <memory>

#include "trelliskv/network_manager.h"
#include "trelliskv/node_config.h"
#include "trelliskv/result.h"
#include "trelliskv/storage_engine.h"

namespace trelliskv {

class TrellisNode {
   public:
    TrellisNode(const NodeConfig& config);
    ~TrellisNode();

    Result<void> start();

    void stop();

    bool is_running() const;

    const NodeConfig& get_config() const { return config_; }

   private:
    std::unique_ptr<Response> handle_request(const Request& request_json);

    NodeConfig config_;

    std::unique_ptr<StorageEngine> storage_;
    std::unique_ptr<NetworkManager> network_;
};

}  // namespace trelliskv
