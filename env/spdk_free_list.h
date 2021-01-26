#pragma once
#include "vector"
#include "unistd.h"
#include "mutex"
#include "spandb_options.h"
#include "atomic"

namespace rocksdb{
  struct FreeListEntry{
    private:
      uint64_t start_;
      uint64_t end_;
      uint64_t size_;
    public:
      FreeListEntry(uint64_t start, uint64_t end):
        start_(start),
        end_(end),
        size_(end - start + 1){
          
      }
      uint64_t Size(){
        return size_;
      };
      void SetStart(uint64_t start){
        start_ = start;
        size_ = end_ - start_ + 1;
        assert(start_ <= end_);
      }
      void SetEnd(uint64_t end){
        end_ = end;
        size_ = end_ - start_ + 1;
        assert(start_ <= end_);
      }
      uint64_t GetStart(){
        return start_;
      }
      uint64_t GetEnd(){
        return end_;
      }
  };
  class SPDKFreeList{
    private:
      std::mutex mutex_;
      std::vector<FreeListEntry> free_list_;
      std::atomic<uint64_t> size_;
    public:
      SPDKFreeList(): size_(0){

      }
      ~SPDKFreeList(){

      }

      void PrintStatus(){
        uint64_t space_size = 0;
        uint64_t size = 0;
        {
          std::unique_lock<std::mutex> lk(mutex_);
          for(auto it = free_list_.begin(); it != free_list_.end(); it++){
            space_size += it->Size();
          }
          size = free_list_.size();
        }
        printf("free list entry num: %ld, total size: %.3lf GB\n", size, (space_size * 4096 * 1.0)/(1ull<<30));
        printf("spandb_controller ld size: %.2lf\n", spandb_controller_.GetLDLeftSpace());
      }

      uint64_t Size(){return size_.load();}

      void Put(uint64_t start, uint64_t end){
        spandb_controller_.IncreaseLDLeftSpace(end - start + 1);
        size_.fetch_add(end - start + 1);
        std::unique_lock<std::mutex> lk(mutex_);
        //merge
        for(auto it = free_list_.begin(); it != free_list_.end(); it++){
          if(it->GetEnd() + 1 == start){
            it->SetEnd(end);
            return;
          }else if(end + 1 == it->GetStart()){
            it->SetStart(start);
            return;
          }
        }
        //insert
        free_list_.push_back(FreeListEntry(start, end));
      }

      void Remove(uint64_t start, uint64_t end){
        assert(end >= start);
        size_.fetch_sub(end - start + 1);
        spandb_controller_.DecreaseLDLeftSpace(end - start + 1);
        std::unique_lock<std::mutex> lk(mutex_);
        for(auto it = free_list_.begin(); it != free_list_.end(); it++){
          if(start >= it->GetStart() && end <= it->GetEnd()){
            if(start == it->GetStart()){
              if(end == it->GetEnd()){
                free_list_.erase(it);
                return;
              }
              it->SetStart(end + 1);
            }else if(end == it->GetEnd()){
              if(start == it->GetStart()){
                free_list_.erase(it);
                return;
              }
              it->SetEnd(start - 1);
            }else{
              uint64_t new_end = it->GetEnd();
              it->SetEnd(start - 1);
              free_list_.push_back(FreeListEntry(end + 1, new_end));
            }
            return;
          }
        }
        printf("------------\n");
        for(auto it = free_list_.begin(); it != free_list_.end(); it++){
          printf("%ld %ld\n", it->GetStart(), it->GetEnd());
        }
        printf("------------\n");
        printf("remove %ld %ld\n", start, end);
        fflush(stdout);
        assert(0);
      }

      uint64_t Get(uint64_t size){
        size_.fetch_sub(size);
        spandb_controller_.DecreaseLDLeftSpace(size);
        {
          std::unique_lock<std::mutex> lk(mutex_);
          //find one entry whose exactly equals to "size"
          for(auto it = free_list_.begin(); it != free_list_.end(); it++){
            if(it->Size() == size){
              uint64_t start = it->GetStart();
              free_list_.erase(it);
              return start;
            }
          }
          //split big entry
          for(auto it = free_list_.begin(); it != free_list_.end(); it++){
            if(it->Size() > size * 2){
              uint64_t start = it->GetStart();
              it->SetStart(start + size);
              return start;
            }
          }
          //split small entry
          for(auto it = free_list_.begin(); it != free_list_.end(); it++){
            if(it->Size() > size){
              uint64_t start = it->GetStart();
              it->SetStart(start + size);
              return start;
            }
          }
        }
        PrintStatus();
        printf("Can not get a free page\n");
        return 0;
      }
  };
}