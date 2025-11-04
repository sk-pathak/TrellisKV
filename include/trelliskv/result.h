#pragma once

#include <functional>
#include <optional>
#include <stdexcept>
#include <string>

namespace trelliskv {

template <typename T>
class Result {
   private:
    std::optional<T> value_;
    std::string error_message_;
    bool is_success_;

    struct ErrorTag {};
    struct SuccessTag {};

    Result(ErrorTag, const std::string& error)
        : error_message_(error), is_success_(false) {}
    Result(SuccessTag, T&& value)
        : value_(std::move(value)), is_success_(true) {}
    Result(SuccessTag, const T& value) : value_(value), is_success_(true) {}

   public:
    // Static factory methods
    static Result<T> success(T&& value) {
        return Result<T>(SuccessTag{}, std::move(value));
    }

    static Result<T> success(const T& value) {
        return Result<T>(SuccessTag{}, value);
    }

    static Result<T> error(const std::string& message) {
        return Result<T>(ErrorTag{}, message);
    }

    bool is_success() const { return is_success_; }
    bool is_error() const { return !is_success_; }

    const T& value() const {
        if (!is_success_) {
            throw std::runtime_error(
                "Attempted to access value of failed Result: " +
                error_message_);
        }
        return *value_;
    }

    T& value() {
        if (!is_success_) {
            throw std::runtime_error(
                "Attempted to access value of failed Result: " +
                error_message_);
        }
        return *value_;
    }

    const std::string& error() const { return error_message_; }

    const T* operator->() const { return is_success_ ? &(*value_) : nullptr; }

    T* operator->() { return is_success_ ? &(*value_) : nullptr; }

    const T& operator*() const { return value(); }

    T& operator*() { return value(); }

    explicit operator bool() const { return is_success_; }

    T value_or(const T& default_value) const {
        return is_success_ ? *value_ : default_value;
    }

    template <typename U>
    Result<U> map(std::function<U(const T&)> func) const {
        if (is_success_) {
            try {
                return Result<U>::success(func(*value_));
            } catch (const std::exception& e) {
                return Result<U>::error(e.what());
            }
        }
        return Result<U>::error(error_message_);
    }

    template <typename U>
    Result<U> flat_map(std::function<Result<U>(const T&)> func) const {
        if (is_success_) {
            return func(*value_);
        }
        return Result<U>::error(error_message_);
    }
};

template <>
class Result<void> {
   private:
    std::string error_message_;
    bool is_success_;

   public:
    Result() : is_success_(true) {}

    Result(const std::string& error)
        : error_message_(error), is_success_(false) {}

    static Result<void> success() { return Result<void>(); }

    static Result<void> error(const std::string& message) {
        return Result<void>(message);
    }

    bool is_success() const { return is_success_; }
    bool is_error() const { return !is_success_; }

    const std::string& error() const { return error_message_; }

    explicit operator bool() const { return is_success_; }
};

}  // namespace trelliskv