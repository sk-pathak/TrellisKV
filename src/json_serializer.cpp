#include "trelliskv/json_serializer.h"
#include <stdexcept>

using json = nlohmann::json;

namespace trelliskv {

std::string JsonSerializer::serialize(const Request& request) {
    json j;
    j["type"] = request_type_to_string(request.type);
    j["key"] = request.key;
    
    if (request.type == RequestType::PUT) {
        j["value"] = request.value;
    }
    
    return j.dump();
}

std::string JsonSerializer::serialize(const Response& response) {
    json j;
    j["success"] = response.success;
    
    if (!response.value.empty()) {
        j["value"] = response.value;
    }
    
    if (!response.error.empty()) {
        j["error"] = response.error;
    }
    
    return j.dump();
}

Request JsonSerializer::deserialize_request(const std::string& json_str) {
    json j = json::parse(json_str);
    
    Request req;
    req.type = string_to_request_type(j["type"]);
    req.key = j["key"];
    
    if (j.contains("value")) {
        req.value = j["value"];
    }
    
    return req;
}

Response JsonSerializer::deserialize_response(const std::string& json_str) {
    json j = json::parse(json_str);
    
    Response resp;
    resp.success = j["success"];
    
    if (j.contains("value")) {
        resp.value = j["value"];
    }
    
    if (j.contains("error")) {
        resp.error = j["error"];
    }
    
    return resp;
}

std::string JsonSerializer::request_type_to_string(RequestType type) {
    switch (type) {
        case RequestType::GET:
            return "GET";
        case RequestType::PUT:
            return "PUT";
        case RequestType::DELETE_KEY:
            return "DELETE";
        default:
            return "UNKNOWN";
    }
}

RequestType JsonSerializer::string_to_request_type(const std::string& type_str) {
    if (type_str == "GET") {
        return RequestType::GET;
    } else if (type_str == "PUT") {
        return RequestType::PUT;
    } else if (type_str == "DELETE") {
        return RequestType::DELETE_KEY;
    } else {
        throw std::runtime_error("Unknown request type: " + type_str);
    }
}

} // namespace trelliskv
