#pragma once

#include <string>

namespace trelliskv {

class Logger {
  public:
    // Singleton access
    static Logger &instance();

    void info(const std::string &message);
    void warn(const std::string &message);
    void error(const std::string &message);

  private:
    Logger() = default;
    ~Logger() = default;

    void log(const std::string &level, const std::string &message);
};

} // namespace trelliskv
