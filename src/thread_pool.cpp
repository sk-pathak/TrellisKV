#include "trelliskv/thread_pool.h"

#include "trelliskv/logger.h"

namespace trelliskv {

ThreadPool::ThreadPool(size_t num_threads) {
    LOG_INFO("Creating thread pool with " + std::to_string(num_threads) +
             " threads");

    for (size_t i = 0; i < num_threads; ++i) {
        workers_.emplace_back([this]() { worker_loop(); });
    }
}

ThreadPool::~ThreadPool() {
    stop_ = true;
    cv_.notify_all();

    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    LOG_INFO("Thread pool stopped");
}

void ThreadPool::submit(std::function<void()> task) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        tasks_.push(std::move(task));
    }
    cv_.notify_one();
}

void ThreadPool::worker_loop() {
    while (!stop_) {
        std::function<void()> task;

        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this]() { return stop_ || !tasks_.empty(); });

            if (stop_ && tasks_.empty()) {
                return;
            }

            if (!tasks_.empty()) {
                task = std::move(tasks_.front());
                tasks_.pop();
            }
        }

        if (task) {
            try {
                task();
            } catch (const std::exception& e) {
                LOG_ERROR("Exception in thread pool task: " +
                          std::string(e.what()));
            }
        }
    }
}

}  // namespace trelliskv
