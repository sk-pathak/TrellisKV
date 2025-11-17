#include "trelliskv/storage_engine.h"

#include <mutex>

#include "trelliskv/result.h"

namespace trelliskv {

StorageEngine::StorageEngine() {}

StorageEngine::~StorageEngine() {}

Result<void> StorageEngine::put(const std::string& key,
                                const VersionedValue& value) {
    try {
        std::unique_lock<std::shared_mutex> lock(data_mutex_);
        data_[key] = value;
        return Result<void>::success();
    } catch (const std::exception& e) {
        return Result<void>::error("Failed to store key '" + key +
                                   "': " + e.what());
    }
}

Result<VersionedValue> StorageEngine::get(const std::string& key) const {
    try {
        std::shared_lock<std::shared_mutex> lock(data_mutex_);
        auto it = data_.find(key);
        if (it != data_.end()) {
            return Result<VersionedValue>::success(it->second);
        } else {
            return Result<VersionedValue>::error("Key '" + key + "' not found");
        }
    } catch (const std::exception& e) {
        return Result<VersionedValue>::error("Failed to retrieve key '" + key +
                                             "': " + e.what());
    }
}

Result<bool> StorageEngine::remove(const std::string& key) {
    try {
        std::unique_lock<std::shared_mutex> lock(data_mutex_);
        auto it = data_.find(key);
        if (it != data_.end()) {
            data_.erase(it);
            return Result<bool>::success(true);
        }
        return Result<bool>::success(false);
    } catch (const std::exception& e) {
        return Result<bool>::error("Failed to remove key '" + key +
                                   "': " + e.what());
    }
}

Result<bool> StorageEngine::contains(const std::string& key) const {
    try {
        std::shared_lock<std::shared_mutex> lock(data_mutex_);
        auto it = data_.find(key);
        if (it != data_.end()) return Result<bool>::success(true);
        return Result<bool>::success(false);
    } catch (const std::exception& e) {
        return Result<bool>::error("Failed to look for key '" + key +
                                   "': " + e.what());
    }
}

size_t StorageEngine::size() const {
    std::shared_lock<std::shared_mutex> lock(data_mutex_);
    return data_.size();
}

bool StorageEngine::empty() const {
    std::shared_lock<std::shared_mutex> lock(data_mutex_);
    return data_.empty();
}

void StorageEngine::clear() {
    std::unique_lock<std::shared_mutex> lock(data_mutex_);
    data_.clear();
}

std::vector<std::string> StorageEngine::get_all_keys() const {
    std::shared_lock<std::shared_mutex> lock(data_mutex_);
    std::vector<std::string> keys;
    keys.reserve(data_.size());
    for (const auto& pair : data_) {
        keys.push_back(pair.first);
    }
    return keys;
}

std::pair<size_t, size_t> StorageEngine::get_stats() const {
    std::shared_lock<std::shared_mutex> lock(data_mutex_);
    size_t total_keys = data_.size();
    size_t total_memory = current_memory_bytes_.load();
    return std::make_pair(total_keys, total_memory);
}

}  // namespace trelliskv
