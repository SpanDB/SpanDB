/*
* Hao Chen
*/
#pragma once

#include "functional"
#include "atomic"
#include "deque"
#include "mutex"
#include "condition_variable"

namespace rocksdb {

namespace spandb{
  
  class TaskQueue{
    private:
      struct BGTaskItem {
        void* tag = nullptr;
        std::function<void()> function;
        std::function<void()> unschedFunction;
      };
      std::deque<BGTaskItem> queue_;
      std::mutex mutex_;
      std::condition_variable  cv_;
      std::atomic_uint queue_len_;
      std::atomic_uint call_num_;
      std::string name_;
      const int max_size_;

    public:
      TaskQueue(std::string name, int max_size = 1024):
        queue_len_(0),
        call_num_(0),
        name_(name),
        max_size_(max_size){
      }
      ~TaskQueue(){
        printf("%s call: %d\n", name_.c_str(), call_num_.load());
      }

      unsigned int GetQueueLen() const {
        return queue_len_.load(std::memory_order_relaxed);
      }

      int Submit(std::function<void()>&& schedule,
                 std::function<void()>&& unschedule, void* tag){
        std::lock_guard<std::mutex> lock(mutex_);
        if((int)queue_.size() > max_size_ - 1)
          return 0;
        queue_.push_back(BGTaskItem());
        auto& item = queue_.back();
        item.tag = tag;
        item.function = std::move(schedule);
        item.unschedFunction = std::move(unschedule);
        queue_len_.store(static_cast<unsigned int>(queue_.size()), std::memory_order_relaxed);
        cv_.notify_one();
        return 1;
      }

      int UnSchedule(void* arg) {
        int count = 0;
        std::vector<std::function<void()>> candidates;
        {
          std::lock_guard<std::mutex> lock(mutex_);
          // Remove from priority queue
          std::deque<BGTaskItem>::iterator it = queue_.begin();
          while (it != queue_.end()) {
            if (arg == (*it).tag) {
              if (it->unschedFunction) {
                candidates.push_back(std::move(it->unschedFunction));
              }
              it = queue_.erase(it);
              count++;
            } else {
              ++it;
            }
          }
          queue_len_.store(static_cast<unsigned int>(queue_.size()), std::memory_order_relaxed);
        }
        
        // Run unschedule functions outside the mutex
        for (auto& f : candidates) {
          f();
        }
        return count;
      }

      void RunTask(){
        while(true){
          std::unique_lock<std::mutex> lock(mutex_);
          if(queue_.empty()){
              return;
          }
          auto func = std::move(queue_.front().function);
          queue_.pop_front();
          queue_len_.store(static_cast<unsigned int>(queue_.size()),std::memory_order_relaxed);
          lock.unlock();
          call_num_.fetch_add(1);
          func();
        }
      }

      void RunOneTask(){
        std::unique_lock<std::mutex> lock(mutex_);
        if(queue_.empty()){
            return;
        }
        auto func = std::move(queue_.front().function);
        queue_.pop_front();
        queue_len_.store(static_cast<unsigned int>(queue_.size()),std::memory_order_relaxed);
        lock.unlock();
        call_num_.fetch_add(1);
        func();
      }
  };

  class SubCompactionQueue{
    private:
      struct TaskItem {
        std::function<void()> function;
        std::atomic<int> *num;
      };
      std::deque<TaskItem> queue_;
      std::mutex mutex_;
      std::atomic_uint queue_len_;
      std::atomic_uint call_num_;
      std::string name_;

    public:
      SubCompactionQueue(std::string name):
        queue_len_(0),
        call_num_(0),
        name_(name){
      }
      ~SubCompactionQueue(){
        printf("%s call: %d\n", name_.c_str(), call_num_.load());
      }

      unsigned int GetQueueLen() const {
        return queue_len_.load(std::memory_order_relaxed);
      }

      int Schedule(void (*function)(void* arg1), void* arg, std::atomic<int> *num){
        return Submit(std::bind(function, arg), num);
      }

      int Submit(std::function<void()> schedule, std::atomic<int> *num){
        std::lock_guard<std::mutex> lock(mutex_);
        queue_.push_back(TaskItem());
        auto& item = queue_.back();
        item.function = std::move(schedule);
        item.num = num;
        queue_len_.store(static_cast<unsigned int>(queue_.size()), std::memory_order_relaxed);
        return 1;
      }
      void RunTask(){
        while(true){
          if(queue_len_.load() == 0)
            return;
          std::unique_lock<std::mutex> lock(mutex_);
          if(queue_.empty()){
              return;
          }
          auto func = std::move(queue_.front().function);
          std::atomic<int> *num = queue_.front().num;
          queue_.pop_front();
          queue_len_.store(static_cast<unsigned int>(queue_.size()),std::memory_order_relaxed);
          lock.unlock();
          call_num_.fetch_add(1);
          func();
          if(num != nullptr)
            num->fetch_sub(1);
        }
      }
      void RunOneTask(){
        if(queue_len_.load() == 0)
            return;
        std::unique_lock<std::mutex> lock(mutex_);
        if(queue_.empty()){
          return;
        }
        auto func = std::move(queue_.front().function);
        std::atomic<int> *num = queue_.front().num;
        queue_.pop_front();
        queue_len_.store(static_cast<unsigned int>(queue_.size()),std::memory_order_relaxed);
        lock.unlock();
        call_num_.fetch_add(1);
        func();
        if(num != nullptr)
          num->fetch_sub(1);
      }
  };
}

}