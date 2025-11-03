#pragma once

#include "messages.h"
#include <string>

namespace trelliskv {

// Simple JSON serializer/deserializer for Request and Response messages.
class JsonSerializer {
  public:
    static std::string serialize(const Request& request);
    static std::string serialize(const Response& response);
    
    static Request deserialize_request(const std::string& json_str);    
    static Response deserialize_response(const std::string& json_str);
    
  private:
    static std::string request_type_to_string(RequestType type);    
    static RequestType string_to_request_type(const std::string& type_str);
};

} // namespace trelliskv
