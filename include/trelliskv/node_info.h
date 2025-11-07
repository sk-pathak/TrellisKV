#pragma once

#include <string>

#include "trelliskv/types.h"

namespace trelliskv {

struct NodeAddress {
    std::string hostname;
    uint16_t port;

    NodeAddress() = default;
    NodeAddress(const std::string& host, uint16_t p)
        : hostname(host), port(p) {}

    std::string to_string() const {
        return hostname + ":" + std::to_string(port);
    }

    bool operator==(const NodeAddress& other) const {
        return hostname == other.hostname && port == other.port;
    }
};

struct NodeInfo {
    NodeId node_id;
    std::string address;

    NodeInfo() = default;

    NodeInfo(const NodeId& id, const std::string& addr)
        : node_id(id), address(addr) {}

    std::pair<std::string, uint16_t> parse_address() const {
        size_t colon_pos = address.find(':');
        if (colon_pos == std::string::npos) {
            return {"", 0};
        }
        std::string host = address.substr(0, colon_pos);
        uint16_t port =
            static_cast<uint16_t>(std::stoi(address.substr(colon_pos + 1)));
        return {host, port};
    }
};

}  // namespace trelliskv
