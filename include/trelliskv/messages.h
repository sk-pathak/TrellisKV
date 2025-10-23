#pragma once

#include <string>

namespace trelliskv {

enum class RequestType {
    GET,
    PUT,
    DELETE_KEY
};

struct Request {
    RequestType type;
    std::string key;
    std::string value;
};

struct Response {
    bool success;
    std::string value;
    std::string error;
};

struct GetRequest {
    std::string key;
};

struct PutRequest {
    std::string key;
    std::string value;
};

struct DeleteRequest {
    std::string key;
};

struct GetResponse {
    bool success;
    std::string value;
    std::string error;
};

struct PutResponse {
    bool success;
    std::string error;
};

struct DeleteResponse {
    bool success;
    std::string error;
};

} // namespace trelliskv
