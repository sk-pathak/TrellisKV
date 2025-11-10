#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace trelliskv {

class ThreadPool {
   public:
    explicit ThreadPool(
        size_t num_threads = std::thread::hardware_concurrency());
    ~ThreadPool();

    template <typename F>
    auto enqueue(F&& f) -> std::future<typename std::invoke_result<F>::type>;

    void shutdown();
    size_t get_queue_size() const;

   private:
    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> tasks_;

    mutable std::mutex queue_mutex_;
    std::condition_variable condition_;
    std::atomic<bool> stop_;
};

template <typename F>
auto ThreadPool::enqueue(F&& f)
    -> std::future<typename std::invoke_result<F>::type> {
    using return_type = typename std::invoke_result<F>::type;

    auto task =
        std::make_shared<std::packaged_task<return_type()>>(std::forward<F>(f));

    std::future<return_type> result = task->get_future();

    {
        std::unique_lock<std::mutex> lock(queue_mutex_);

        if (stop_) {
            throw std::runtime_error("ThreadPool is stopped");
        }

        tasks_.emplace([task]() { (*task)(); });
    }

    condition_.notify_one();
    return result;
}

}  // namespace trelliskv