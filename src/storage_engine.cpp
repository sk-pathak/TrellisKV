#include "trelliskv/storage_engine.h"

namespace trelliskv {

void StorageEngine::put(const std::string &key, const std::string &value) {
    data_[key] = value;
}

std::string StorageEngine::get(const std::string &key) const {
    auto it = data_.find(key);
    if (it != data_.end()) {
        return it->second;
    }
    return "";
}

void StorageEngine::remove(const std::string &key) {
    data_.erase(key);
}

bool StorageEngine::contains(const std::string &key) const {
    return data_.find(key) != data_.end();
}

} // namespace trelliskv
