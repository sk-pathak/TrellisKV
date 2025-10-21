#include "trelliskv/logger.h"
#include <iostream>

namespace trelliskv {

Logger &Logger::instance() {
    static Logger instance;
    return instance;
}

void Logger::info(const std::string &message) {
    log("INFO", message);
}

void Logger::error(const std::string &message) {
    log("ERROR", message);
}

void Logger::log(const std::string &level, const std::string &message) {
    if (level == "ERROR") {
        std::cerr << "[" << level << "] " << message << std::endl;
    } else {
        std::cout << "[" << level << "] " << message << std::endl;
    }
}

} // namespace trelliskv
