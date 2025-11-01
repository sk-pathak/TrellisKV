#pragma once

#include <string>
#include <unordered_map>
#include <mutex>

namespace trelliskv {

class StorageEngine {
  private:
    std::unordered_map<std::string, std::string> data_;
    mutable std::mutex mutex_;

  public:
    StorageEngine() = default;
    ~StorageEngine() = default;

    void put(const std::string &key, const std::string &value);
    std::string get(const std::string &key) const;
    void remove(const std::string &key);
    bool contains(const std::string &key) const;
};

} // namespace trelliskv
