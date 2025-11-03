#include "trelliskv/storage_engine.h"

#include "trelliskv/logger.h"

namespace trelliskv {

StorageEngine::StorageEngine() {}

StorageEngine::~StorageEngine() {}

void StorageEngine::put(const std::string& key, const VersionedValue& value) {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        data_[key] = value;
    } catch (const std::exception& e) {
        LOG_ERROR("Failed to store key '" + key + "' " + e.what());
    }
}

VersionedValue StorageEngine::get(const std::string& key) const {
    try {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = data_.find(key);
        if (it != data_.end()) {
            return it->second;
        }
        LOG_ERROR("Key '" + key + "' not found");
    } catch (const std::exception& e) {
        LOG_ERROR("Failed to retrieve key '" + key + "' " + e.what());
    }
    return VersionedValue();
}

bool StorageEngine::remove(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (contains(key)) {
        data_.erase(key);
        return true;
    }
    return false;
}

bool StorageEngine::contains(const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return data_.find(key) != data_.end();
}

}  // namespace trelliskv
