#pragma once

#include <chrono>
#include <string>

namespace trelliskv {

using NodeId = std::string;
using Timestamp = std::chrono::system_clock::time_point;

enum class NodeState { ACTIVE, FAILED };

enum class ResponseStatus { OK, NOT_FOUND, ERROR, CONFLICT, TIMEOUT };

struct TimestampVersion {
    uint64_t timestamp;
    NodeId last_writer;

    TimestampVersion() : timestamp(0), last_writer("") {}

    TimestampVersion(uint64_t ts, const NodeId& writer)
        : timestamp(ts), last_writer(writer) {}

    static TimestampVersion now(const NodeId& writer) {
        auto now = std::chrono::system_clock::now();
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            now.time_since_epoch());
        return TimestampVersion(ms.count(), writer);
    }

    bool is_newer_than(const TimestampVersion& other) const {
        if (timestamp != other.timestamp) {
            return timestamp > other.timestamp;
        }
        return last_writer > other.last_writer;
    }

    bool is_older_than(const TimestampVersion& other) const {
        return other.is_newer_than(*this);
    }

    bool equals(const TimestampVersion& other) const {
        return timestamp == other.timestamp && last_writer == other.last_writer;
    }
};

struct VersionedValue;
struct NodeInfo;
struct Request;
struct Response;

}  // namespace trelliskv
