#pragma once

#include <cstdint>
#include <string>

namespace trelliskv {

struct NodeConfig {
    std::string hostname;
    uint16_t port;

    std::string node_id;

    NodeConfig()
        : hostname("localhost"),
          port(5000),
          node_id("") {}

    NodeConfig(const std::string &host, uint16_t p, const std::string &id = "")
        : hostname(host),
          port(p),
          node_id(id.empty() ? (host + ":" + std::to_string(p)) : id) {}
};

} // namespace trelliskv
