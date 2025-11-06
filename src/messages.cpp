#include "trelliskv/messages.h"

#include <nlohmann/json.hpp>

#include "trelliskv/json_serializer.h"

namespace trelliskv {

void Request::to_json(nlohmann::json& j) const {
    j["request_id"] = request_id;
    j["sender_id"] = sender_id;
}

void Request::from_json(const nlohmann::json& j) {
    if (j.contains("request_id")) {
        request_id = j["request_id"];
    }
    if (j.contains("sender_id")) {
        sender_id = j["sender_id"];
    }
}

void GetRequest::to_json(nlohmann::json& json) const {
    Request::to_json(json);
    json["type"] = "GET";
    json["key"] = key;
    if (client_version.has_value()) {
        json["client_version"] =
            JsonSerializer::serialize_timestamp_version(client_version.value());
    }
}

void GetRequest::from_json(const nlohmann::json& json) {
    Request::from_json(json);
    if (json.contains("key")) {
        key = json["key"];
    }
    if (json.contains("client_version")) {
        auto result = JsonSerializer::deserialize_timestamp_version(
            json["client_version"]);
        if (result) {
            client_version = result.value();
        }
    }
}

void PutRequest::to_json(nlohmann::json& json) const {
    Request::to_json(json);
    json["type"] = "PUT";
    json["key"] = key;
    json["value"] = value;
    json["is_replication"] = is_replication;
    if (expected_version.has_value()) {
        json["expected_version"] = JsonSerializer::serialize_timestamp_version(
            expected_version.value());
    }
}

void PutRequest::from_json(const nlohmann::json& json) {
    Request::from_json(json);
    if (json.contains("key")) {
        key = json["key"];
    }
    if (json.contains("value")) {
        value = json["value"];
    }
    if (json.contains("is_replication")) {
        is_replication = json["is_replication"];
    }
    if (json.contains("expected_version")) {
        auto result = JsonSerializer::deserialize_timestamp_version(
            json["expected_version"]);
        if (result) {
            expected_version = result.value();
        }
    }
}

void DeleteRequest::to_json(nlohmann::json& json) const {
    Request::to_json(json);
    json["type"] = "DELETE";
    json["key"] = key;
    if (expected_version.has_value()) {
        json["expected_version"] = JsonSerializer::serialize_timestamp_version(
            expected_version.value());
    }
}

void DeleteRequest::from_json(const nlohmann::json& json) {
    Request::from_json(json);
    if (json.contains("key")) {
        key = json["key"];
    }
    if (json.contains("expected_version")) {
        auto result = JsonSerializer::deserialize_timestamp_version(
            json["expected_version"]);
        if (result) {
            expected_version = result.value();
        }
    }
}

void Response::to_json(nlohmann::json& json) const {
    json["request_id"] = request_id;
    json["status"] = JsonSerializer::response_status_to_string(status);
    json["responder_id"] = responder_id;

    if (value.has_value()) {
        json["value"] = value.value();
    }
    if (version.has_value()) {
        json["version"] =
            JsonSerializer::serialize_timestamp_version(version.value());
    }
    if (!error_message.empty()) {
        json["error_message"] = error_message;
    }
}

void Response::from_json(const nlohmann::json& json) {
    if (json.contains("request_id")) {
        request_id = json["request_id"];
    }
    if (json.contains("status")) {
        auto result = JsonSerializer::string_to_response_status(json["status"]);
        if (result) {
            status = result.value();
        }
    }
    if (json.contains("responder_id")) {
        responder_id = json["responder_id"];
    }
    if (json.contains("value")) {
        value = json["value"].get<std::string>();
    }
    if (json.contains("version")) {
        auto result =
            JsonSerializer::deserialize_timestamp_version(json["version"]);
        if (result) {
            version = result.value();
        }
    }
    if (json.contains("error_message")) {
        error_message = json["error_message"];
    }
}

}  // namespace trelliskv
