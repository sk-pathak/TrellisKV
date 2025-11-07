#pragma once

#include <optional>
#include <string>

#include "nlohmann/json.hpp"
#include "types.h"

namespace trelliskv {

struct Request {
    std::string request_id;
    NodeId sender_id;

    Request() = default;
    Request(const std::string& id, const NodeId& sender)
        : request_id(id), sender_id(sender) {}
    virtual ~Request() = default;

    virtual void to_json(nlohmann::json& json) const;
    virtual void from_json(const nlohmann::json& json);
};

struct GetRequest : public Request {
    std::string key;
    std::optional<TimestampVersion> client_version;

    GetRequest() = default;
    explicit GetRequest(const std::string& k) : key(k) {}

    void to_json(nlohmann::json& json) const override;
    void from_json(const nlohmann::json& json) override;
};

struct PutRequest : public Request {
    std::string key;
    std::string value;
    std::optional<TimestampVersion> expected_version;
    bool is_replication = false;

    PutRequest() = default;
    PutRequest(const std::string& k, const std::string& v) : key(k), value(v) {}

    void to_json(nlohmann::json& json) const override;
    void from_json(const nlohmann::json& json) override;
};

struct DeleteRequest : public Request {
    std::string key;
    std::optional<TimestampVersion> expected_version;

    DeleteRequest() = default;
    explicit DeleteRequest(const std::string& k) : key(k) {}

    void to_json(nlohmann::json& json) const override;
    void from_json(const nlohmann::json& json) override;
};

struct Response {
    std::string request_id;
    ResponseStatus status;
    std::optional<std::string> value;
    std::optional<TimestampVersion> version;
    std::string error_message;
    NodeId responder_id;

    Response() = default;
    Response(ResponseStatus s) : status(s) {}
    Response(ResponseStatus s, const std::string& val,
             const TimestampVersion& ver)
        : status(s), value(val), version(ver) {}
    virtual ~Response() = default;

    bool is_success() const { return status == ResponseStatus::OK; }
    bool is_not_found() const { return status == ResponseStatus::NOT_FOUND; }
    bool is_error() const { return status == ResponseStatus::ERROR; }
    bool is_conflict() const { return status == ResponseStatus::CONFLICT; }
    bool is_timeout() const { return status == ResponseStatus::TIMEOUT; }

    static Response success(const std::string& val = "",
                            const TimestampVersion& ver = TimestampVersion{}) {
        return Response(ResponseStatus::OK, val, ver);
    }

    static Response not_found() { return Response(ResponseStatus::NOT_FOUND); }

    static Response error(const std::string& message) {
        Response resp(ResponseStatus::ERROR);
        resp.error_message = message;
        return resp;
    }

    static Response conflict() {
        Response resp(ResponseStatus::CONFLICT);
        resp.error_message = "Multiple conflicting values found";
        return resp;
    }

    static Response timeout() {
        Response resp(ResponseStatus::TIMEOUT);
        resp.error_message = "Request timed out";
        return resp;
    }

    virtual void to_json(nlohmann::json& json) const;
    virtual void from_json(const nlohmann::json& json);
};
}  // namespace trelliskv