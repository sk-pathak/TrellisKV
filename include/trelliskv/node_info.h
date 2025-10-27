#pragma once

#include "trelliskv/types.h"

#include <string>

namespace trelliskv {

struct NodeInfo {
    NodeId node_id;
    std::string address;

    NodeInfo() = default;

    NodeInfo(const NodeId &id, const std::string &addr)
        : node_id(id), address(addr) {}

    std::pair<std::string, uint16_t> parse_address() const {
        size_t colon_pos = address.find(':');
        if (colon_pos == std::string::npos) {
            return {"", 0};
        }
        std::string host = address.substr(0, colon_pos);
        uint16_t port = static_cast<uint16_t>(std::stoi(address.substr(colon_pos + 1)));
        return {host, port};
    }
};

} // namespace trelliskv
