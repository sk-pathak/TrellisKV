#pragma once

#include <optional>
#include <string>
#include <vector>

#include "types.h"
#include "versioned_value.h"

namespace trelliskv {

struct ReadResult {
    ResponseStatus status;
    std::optional<VersionedValue> value;
    std::vector<VersionedValue> conflicting_values;
    std::string error_message;

    ReadResult() : status(ResponseStatus::ERROR) {}
    ReadResult(ResponseStatus s) : status(s) {}
    ReadResult(const VersionedValue& val)
        : status(ResponseStatus::OK), value(val) {}

    bool is_success() const { return status == ResponseStatus::OK; }
    bool is_not_found() const { return status == ResponseStatus::NOT_FOUND; }
    bool is_conflict() const { return status == ResponseStatus::CONFLICT; }
    bool is_error() const { return status == ResponseStatus::ERROR; }

    static ReadResult success(const VersionedValue& val) {
        return ReadResult(val);
    }

    static ReadResult not_found() {
        return ReadResult(ResponseStatus::NOT_FOUND);
    }

    static ReadResult conflict(const std::vector<VersionedValue>& values) {
        ReadResult result(ResponseStatus::CONFLICT);
        result.conflicting_values = values;
        result.error_message = "Multiple conflicting values found";
        return result;
    }

    static ReadResult error(const std::string& message) {
        ReadResult result(ResponseStatus::ERROR);
        result.error_message = message;
        return result;
    }
};

}  // namespace trelliskv