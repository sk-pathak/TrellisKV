#pragma once

#include <string>

#include "messages.h"
#include "nlohmann/json.hpp"
#include "result.h"

namespace trelliskv {

class JsonSerializer {
   public:
    // Request
    static Result<std::string> serialize_request(const Request& request);
    static Result<std::unique_ptr<Request>> deserialize_request(
        const std::string& json_str);

    // Response
    static Result<std::string> serialize_response(const Response& response);
    static Result<std::unique_ptr<Response>> deserialize_response(
        const std::string& json_str);

    // Helper methods for common types
    static nlohmann::json serialize_timestamp_version(
        const TimestampVersion& version);
    static Result<TimestampVersion> deserialize_timestamp_version(
        const nlohmann::json& json);

    static nlohmann::json serialize_timestamp(const Timestamp& timestamp);
    static Result<Timestamp> deserialize_timestamp(const nlohmann::json& json);

    // Enum
    static std::string response_status_to_string(ResponseStatus status);
    static Result<ResponseStatus> string_to_response_status(
        const std::string& str);
};

}  // namespace trelliskv
