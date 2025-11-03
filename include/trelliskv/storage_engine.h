#pragma once

#include <mutex>
#include <string>
#include <unordered_map>

#include "versioned_value.h"

namespace trelliskv {

class StorageEngine {
   private:
    mutable std::unordered_map<std::string, VersionedValue> data_;
    mutable std::mutex mutex_;

   public:
    StorageEngine();
    ~StorageEngine();

    void put(const std::string& key, const VersionedValue& value);
    VersionedValue get(const std::string& key) const;
    bool remove(const std::string& key);
    bool contains(const std::string& key) const;
};

}  // namespace trelliskv
