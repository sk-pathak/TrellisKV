#pragma once

#include <chrono>
#include <string>

#include "types.h"

namespace trelliskv {

struct ReplicationRequest {
    std::string key;
    std::string value;
    TimestampVersion version;
    NodeId original_writer;
    Timestamp timestamp;

    ReplicationRequest() = default;
    ReplicationRequest(const std::string& k, const std::string& v,
                       const TimestampVersion& ver, const NodeId& writer)
        : key(k),
          value(v),
          version(ver),
          original_writer(writer),
          timestamp(std::chrono::system_clock::now()) {}
};

}  // namespace trelliskv