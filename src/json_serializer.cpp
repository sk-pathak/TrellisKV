#include "trelliskv/json_serializer.h"

#include "trelliskv/messages.h"

namespace trelliskv {

// Request serialization
Result<std::string> JsonSerializer::serialize_request(const Request& request) {
    try {
        nlohmann::json json;
        request.to_json(json);
        return Result<std::string>::success(json.dump());
    } catch (const std::exception& e) {
        return Result<std::string>::error("Serialization error: " +
                                          std::string(e.what()));
    }
}

Result<std::unique_ptr<Request>> JsonSerializer::deserialize_request(
    const std::string& json_str) {
    try {
        auto json = nlohmann::json::parse(json_str);

        if (!json.contains("type")) {
            return Result<std::unique_ptr<Request>>::error(
                "Missing type field");
        }

        std::string message_type = json["type"];
        std::unique_ptr<Request> request;

        // Create the appropriate request object based on type
        if (message_type == "GET") {
            request = std::make_unique<GetRequest>();
        } else if (message_type == "PUT") {
            request = std::make_unique<PutRequest>();
        } else if (message_type == "DELETE") {
            request = std::make_unique<DeleteRequest>();
        } else {
            return Result<std::unique_ptr<Request>>::error(
                "Unknown message type: " + message_type);
        }

        request->from_json(json);
        return Result<std::unique_ptr<Request>>::success(std::move(request));

    } catch (const std::exception& e) {
        return Result<std::unique_ptr<Request>>::error(
            "Deserialization error: " + std::string(e.what()));
    }
}

// Response serialization
Result<std::string> JsonSerializer::serialize_response(
    const Response& response) {
    try {
        nlohmann::json json;
        response.to_json(json);
        return Result<std::string>::success(json.dump());
    } catch (const std::exception& e) {
        return Result<std::string>::error("Serialization error: " +
                                          std::string(e.what()));
    }
}

Result<std::unique_ptr<Response>> JsonSerializer::deserialize_response(
    const std::string& json_str) {
    try {
        auto json = nlohmann::json::parse(json_str);

        std::string response_type = "RESPONSE";
        if (json.contains("type")) {
            response_type = json["type"];
        }

        std::unique_ptr<Response> response = std::make_unique<Response>();

        response->from_json(json);

        return Result<std::unique_ptr<Response>>::success(std::move(response));

    } catch (const std::exception& e) {
        return Result<std::unique_ptr<Response>>::error(
            "Deserialization error: " + std::string(e.what()));
    }
}

nlohmann::json JsonSerializer::serialize_timestamp_version(
    const TimestampVersion& version) {
    return nlohmann::json{{"timestamp", version.timestamp},
                          {"last_writer", version.last_writer}};
}

Result<TimestampVersion> JsonSerializer::deserialize_timestamp_version(
    const nlohmann::json& json) {
    if (!json.is_object()) {
        return Result<TimestampVersion>::error("Version must be an object");
    }

    TimestampVersion version;

    if (json.contains("timestamp")) {
        if (json["timestamp"].is_number()) {
            version.timestamp = json["timestamp"].get<uint64_t>();
        }
    }

    if (json.contains("last_writer")) {
        version.last_writer = json["last_writer"];
    }

    return Result<TimestampVersion>::success(version);
}

nlohmann::json JsonSerializer::serialize_timestamp(const Timestamp& timestamp) {
    auto duration = timestamp.time_since_epoch();
    auto millis =
        std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    return millis;
}

Result<Timestamp> JsonSerializer::deserialize_timestamp(
    const nlohmann::json& json) {
    if (!json.is_number()) {
        return Result<Timestamp>::error("Timestamp must be a number");
    }

    auto millis = json.get<int64_t>();
    auto duration = std::chrono::milliseconds(millis);
    auto timestamp = Timestamp(duration);

    return Result<Timestamp>::success(timestamp);
}

std::string JsonSerializer::response_status_to_string(ResponseStatus status) {
    switch (status) {
        case ResponseStatus::OK:
            return "ok";
        case ResponseStatus::NOT_FOUND:
            return "not_found";
        case ResponseStatus::ERROR:
            return "error";
        case ResponseStatus::CONFLICT:
            return "conflict";
        case ResponseStatus::TIMEOUT:
            return "timeout";
        default:
            return "error";
    }
}

Result<ResponseStatus> JsonSerializer::string_to_response_status(
    const std::string& str) {
    if (str == "ok") return Result<ResponseStatus>::success(ResponseStatus::OK);
    if (str == "not_found")
        return Result<ResponseStatus>::success(ResponseStatus::NOT_FOUND);
    if (str == "error")
        return Result<ResponseStatus>::success(ResponseStatus::ERROR);
    if (str == "conflict")
        return Result<ResponseStatus>::success(ResponseStatus::CONFLICT);
    if (str == "timeout")
        return Result<ResponseStatus>::success(ResponseStatus::TIMEOUT);
    return Result<ResponseStatus>::error("Unknown response status: " + str);
}

}  // namespace trelliskv