#pragma once

#include <string>

namespace trelliskv {

enum class LogLevel { DEBUG = 0, INFO = 1, WARN = 2, ERROR = 3 };

class Logger {
   public:
    // Singleton access
    static Logger& instance();

    void info(const std::string& message);
    void warn(const std::string& message);
    void error(const std::string& message);
    void debug(const std::string& message);

    void set_level(LogLevel level);
    static std::string level_to_string(LogLevel level);

   private:
    Logger() = default;
    ~Logger() = default;

    LogLevel current_level_{LogLevel::WARN};
    void log(LogLevel level, const std::string& message);
};

#define LOG_DEBUG(msg) trelliskv::Logger::instance().debug(msg)

#define LOG_INFO(msg) trelliskv::Logger::instance().info(msg)

#define LOG_WARN(msg) trelliskv::Logger::instance().warn(msg)

#define LOG_ERROR(msg) trelliskv::Logger::instance().error(msg)

}  // namespace trelliskv
