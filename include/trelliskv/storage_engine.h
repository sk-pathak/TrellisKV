#pragma once

#include <mutex>
#include <string>
#include <unordered_map>

#include "result.h"
#include "versioned_value.h"

namespace trelliskv {

class StorageEngine {
   private:
    mutable std::unordered_map<std::string, VersionedValue> data_;
    mutable std::mutex mutex_;

   public:
    StorageEngine();
    ~StorageEngine();

    Result<void> put(const std::string& key, const VersionedValue& value);
    Result<VersionedValue> get(const std::string& key) const;
    Result<bool> remove(const std::string& key);
    Result<bool> contains(const std::string& key) const;
    size_t size() const;
    bool empty() const;
    void clear();
    std::vector<std::string> get_all_keys() const;
};

}  // namespace trelliskv
