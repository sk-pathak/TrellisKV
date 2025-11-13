#pragma once

#include <atomic>
#include <memory>

#include "trelliskv/messages.h"
#include "trelliskv/node_config.h"
#include "trelliskv/result.h"

namespace trelliskv {

class HashRing;
class NetworkManager;
class StorageEngine;

class TrellisNode {
   public:
    explicit TrellisNode(const NodeId& node_id, const NodeConfig& config);
    ~TrellisNode();

    static NodeConfig create_default_config(const std::string& hostname,
                                            uint16_t port);

    Result<void> start();
    void stop();
    bool is_running() const;

    const NodeConfig& get_config() const;
    const NodeId& get_node_id() const;
    NodeInfo get_node_info() const;
    std::unique_ptr<Response> handle_request(const Request& request);

   private:
    Response handle_get_request(const GetRequest& request);
    Response handle_put_request(const PutRequest& request);
    Response handle_delete_request(const DeleteRequest& request);
    std::string generate_request_id() const;

    NodeId node_id_;
    NodeConfig config_;
    std::atomic<bool> running_;
    std::chrono::steady_clock::time_point start_time_;

    std::unique_ptr<NetworkManager> network_manager_;
    std::unique_ptr<StorageEngine> storage_engine_;
    std::unique_ptr<HashRing> hash_ring_;
};

}  // namespace trelliskv
