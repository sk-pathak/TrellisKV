#include "trelliskv/storage_engine.h"

#include "trelliskv/result.h"

namespace trelliskv {

StorageEngine::StorageEngine() {}

StorageEngine::~StorageEngine() {}

Result<void> StorageEngine::put(const std::string& key,
                                const VersionedValue& value) {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        data_[key] = value;
        return Result<void>::success();
    } catch (const std::exception& e) {
        return Result<void>::error("Failed to store key '" + key +
                                   "': " + e.what());
    }
}

Result<VersionedValue> StorageEngine::get(const std::string& key) const {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
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
        std::lock_guard<std::mutex> lock(mutex_);
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
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = data_.find(key);
        if (it != data_.end()) return Result<bool>::success(true);
        return Result<bool>::success(false);
    } catch (const std::exception& e) {
        return Result<bool>::error("Failed to look for key '" + key +
                                   "': " + e.what());
    }
}

size_t StorageEngine::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return data_.size();
}

bool StorageEngine::empty() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return data_.empty();
}

void StorageEngine::clear() {
    std::unique_lock<std::mutex> lock(mutex_);
    data_.clear();
}

std::vector<std::string> StorageEngine::get_all_keys() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> keys;
    keys.reserve(data_.size());
    for (const auto& pair : data_) {
        keys.push_back(pair.first);
    }
    return keys;
}

}  // namespace trelliskv
