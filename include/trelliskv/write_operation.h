#pragma once

#include <chrono>
#include <string>

#include "types.h"

namespace trelliskv {

struct WriteOperation {
    std::string key;
    std::string value;
    TimestampVersion version;
    NodeId original_writer;
    Timestamp timestamp;
    ConsistencyLevel consistency;

    WriteOperation() = default;
    WriteOperation(const std::string& k, const std::string& v,
                   const TimestampVersion& ver, const NodeId& writer,
                   ConsistencyLevel c = ConsistencyLevel::EVENTUAL)
        : key(k),
          value(v),
          version(ver),
          original_writer(writer),
          timestamp(std::chrono::system_clock::now()),
          consistency(c) {}
};

}  // namespace trelliskv