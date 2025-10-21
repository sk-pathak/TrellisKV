#include "trelliskv/logger.h"
#include "trelliskv/storage_engine.h"
#include <iostream>

int main() {
    std::cout << "TrellisKV - Testing Storage Engine" << std::endl;

    auto &logger = trelliskv::Logger::instance();
    logger.info("Starting storage engine test");

    trelliskv::StorageEngine storage;

    // Test put
    logger.info("Testing PUT operation");
    storage.put("key1", "value1");
    storage.put("key2", "value2");
    storage.put("name", "TrellisKV");

    // Test get
    logger.info("Testing GET operation");
    std::string val1 = storage.get("key1");
    std::string val2 = storage.get("key2");
    std::string name = storage.get("name");

    std::cout << "key1 = " << val1 << std::endl;
    std::cout << "key2 = " << val2 << std::endl;
    std::cout << "name = " << name << std::endl;

    // Test contains
    logger.info("Testing CONTAINS operation");
    std::cout << "Contains key1: " << (storage.contains("key1") ? "yes" : "no") << std::endl;
    std::cout << "Contains key3: " << (storage.contains("key3") ? "yes" : "no") << std::endl;

    // Test remove
    logger.info("Testing REMOVE operation");
    storage.remove("key1");
    std::cout << "After removing key1, contains: " << (storage.contains("key1") ? "yes" : "no") << std::endl;

    logger.info("Storage engine test completed");

    return 0;
}
