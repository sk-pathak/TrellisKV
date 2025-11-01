#include "trelliskv/storage_engine.h"

namespace trelliskv {

void StorageEngine::put(const std::string &key, const std::string &value) {
    std::lock_guard<std::mutex> lock(mutex_);
    data_[key] = value;
}

std::string StorageEngine::get(const std::string &key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = data_.find(key);
    if (it != data_.end()) {
        return it->second;
    }
    return "";
}

void StorageEngine::remove(const std::string &key) {
    std::lock_guard<std::mutex> lock(mutex_);
    data_.erase(key);
}

bool StorageEngine::contains(const std::string &key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return data_.find(key) != data_.end();
}

} // namespace trelliskv
