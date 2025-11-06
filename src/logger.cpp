#include "trelliskv/logger.h"

#include <chrono>
#include <iomanip>
#include <iostream>

namespace trelliskv {

Logger& Logger::instance() {
    static Logger instance;
    return instance;
}

void Logger::error(const std::string& message) {
    log(LogLevel::ERROR, message);
}

void Logger::debug(const std::string& message) {
    log(LogLevel::DEBUG, message);
}

void Logger::info(const std::string& message) { log(LogLevel::INFO, message); }

void Logger::warn(const std::string& message) { log(LogLevel::WARN, message); }

void Logger::set_level(LogLevel level) { current_level_ = level; }

std::string Logger::level_to_string(LogLevel level) {
    switch (level) {
        case LogLevel::INFO:
            return "INFO";
        case LogLevel::WARN:
            return "WARN";
        case LogLevel::ERROR:
            return "ERROR";
        case LogLevel::DEBUG:
            return "DEBUG";
        default:
            return "UNKNOWN";
    }
}

void Logger::log(LogLevel level, const std::string& message) {
    if (level < current_level_) {
        return;
    }

    try {
        using namespace std::chrono;
        auto now = system_clock::now();
        auto time = system_clock::to_time_t(now);
        auto ms = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;

        std::ostringstream oss;
        oss << std::put_time(std::localtime(&time), "%Y-%m-%d %H:%M:%S") << '.'
            << std::setw(3) << std::setfill('0') << ms.count();

        auto formatted =
            oss.str() + " [" + level_to_string(level) + "] " + message;

        (level == LogLevel::ERROR ? std::cerr : std::cout) << formatted << '\n';
    } catch (const std::exception& e) {
        std::cerr << "[ERROR] Logger exception: " << e.what()
                  << " - Original message: " << message << '\n';
    }
}

}  // namespace trelliskv
