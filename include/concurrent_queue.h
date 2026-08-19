#pragma once
#include <queue>
#include <mutex>
#include <condition_variable>
#include <optional>

using namespace std;

template <typename T>
class ConcurrentQueue {
private:
    queue<T> queue_;
    mutex mutex_;
    condition_variable cond_var_;
    bool done_ = false;

public:
    void push(T item) {
        lock_guard<mutex> lock(mutex_);
        if (done_) {
            return;
        }
        queue_.push(move(item));
        cond_var_.notify_one();
    }

    void finish() {
        lock_guard<mutex> lock(mutex_);
        done_ = true;
        cond_var_.notify_all();
    }

    optional<T> pop() {
        unique_lock<mutex> lock(mutex_);
        cond_var_.wait(lock, [this] { return !queue_.empty() || done_; });

        if (queue_.empty() && done_) {
            return nullopt;
        }

        T item = move(queue_.front());
        queue_.pop();
        return item;
    }
};
