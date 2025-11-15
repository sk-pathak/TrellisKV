#pragma once

#include <atomic>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "result.h"
#include "versioned_value.h"

namespace trelliskv {

class StorageEngine {
   private:
    mutable std::unordered_map<std::string, VersionedValue> data_;
    mutable std::shared_mutex data_mutex_;
    std::atomic<size_t> current_memory_bytes_{0};

   public:
    StorageEngine();
    ~StorageEngine();

    StorageEngine(const StorageEngine&) = delete;
    StorageEngine& operator=(const StorageEngine&) = delete;
    StorageEngine(StorageEngine&&) = delete;
    StorageEngine& operator=(StorageEngine&&) = delete;

    Result<void> put(const std::string& key, const VersionedValue& value);
    Result<VersionedValue> get(const std::string& key) const;
    Result<bool> remove(const std::string& key);
    Result<bool> contains(const std::string& key) const;
    size_t size() const;
    bool empty() const;
    void clear();

    std::vector<std::string> get_all_keys() const;
    std::pair<size_t, size_t> get_stats() const;
};

}  // namespace trelliskv
