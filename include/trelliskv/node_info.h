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

using NodeAddressHash = std::hash<trelliskv::NodeAddress>;

struct NodeInfo {
    NodeId id;
    NodeAddress address;

    NodeInfo() = default;

    NodeInfo(const NodeId& id, const NodeAddress& addr)
        : id(id), address(addr) {}
};

}  // namespace trelliskv

namespace std {
template <>
struct hash<trelliskv::NodeAddress> {
    size_t operator()(const trelliskv::NodeAddress& addr) const {
        return hash<string>()(addr.hostname) ^
               (hash<uint16_t>()(addr.port) << 1);
    }
};
}  // namespace std
