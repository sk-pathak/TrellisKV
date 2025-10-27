#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace trelliskv {

class ThreadPool {
  public:
    explicit ThreadPool(size_t num_threads);
    
    ~ThreadPool();

    void submit(std::function<void()> task);

    size_t get_thread_count() const { return workers_.size(); }

  private:
    void worker_loop();

    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> tasks_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<bool> stop_{false};
};

} // namespace trelliskv
