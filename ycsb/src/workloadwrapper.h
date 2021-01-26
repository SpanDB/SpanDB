#pragma once

#include "core/workload_proxy.h"
#include "core/core_workload.h"
#include "chrono"

namespace ycsbc{

class WorkloadWrapper{
  public:
  	struct Request{
  	 private:
  		ycsbc::Operation opt_;
  		const std::string key_;
  		const int len_; //value lenghth or scan lenghth
  	 public:
  		Request(ycsbc::Operation opt, std::string key, int len):
  			opt_(opt),
  			key_(key), 
  			len_(len){}

  		ycsbc::Operation Type(){ return opt_;}

  		std::string Key(){return key_;}

  		int Length(){return len_; }

      void SetType(ycsbc::Operation opt){opt_ = opt;}
  	};

    void produce_request(const uint64_t num, bool is_load, bool master){
      double f = 0.1;
      ycsbc::Operation opt = NONE;
      for(uint64_t i=0; i<num; i++){
        uint64_t index = __sync_fetch_and_add(&load_index_, 1);
        assert(index < num_);
        if(master && index >= num_ * f){
          printf("%.0f%%...", f * 100);
          f += 0.1;
          fflush(stdout);
        }
        std::string table;
        std::string key;
        std::vector<std::string> fields;
        std::vector<ycsbc::CoreWorkload::KVPair> values;
        int len;
        if(is_load){
          workload_proxy_->LoadInsertArgs(table, key, values);
          assert(values.size() == 1);
          requests_[index] = new Request(INSERT, key, values[0].second.length());
          continue;
        }
        opt = workload_proxy_->GetNextOperation();
        if(opt == READ){
          workload_proxy_->GetReadArgs(table, key, fields);
          requests_[index] = new Request(opt, key, 0);
        }else if(opt == UPDATE){
          workload_proxy_->GetUpdateArgs(table, key, values);
          assert(values.size() == 1);
          requests_[index] = new Request(opt, key, values[0].second.length());
        }else if(opt == INSERT){
          workload_proxy_->GetInsertArgs(table, key, values);
          assert(values.size() == 1);
          requests_[index] = new Request(opt, key, values[0].second.length());
        }else if(opt == READMODIFYWRITE){
          workload_proxy_->GetReadModifyWriteArgs(table, key, fields, values);
          assert(values.size() == 1);
          requests_[index] = new Request(opt, key, values[0].second.length());
        }else if(opt == SCAN){
          workload_proxy_->GetScanArgs(table, key, len, fields);
          requests_[index] = new Request(opt, key, len);
        }else{
          throw utils::Exception("Operation request is not recognized!");
        }
      }
    }

  	WorkloadWrapper(WorkloadProxy *workload_proxy, const uint64_t num, bool is_load):
  	   workload_proxy_(workload_proxy),
  	   num_(num),
  	   cursor_(0),
       load_index_(0){
      assert(workload_proxy_ != nullptr);
      int threads_num = sysconf(_SC_NPROCESSORS_ONLN);
  		requests_ = new Request*[num_];
  		printf("loading workload (%ld)...\n", num_);
      auto start = std::chrono::high_resolution_clock::now();
      std::vector<std::thread> threads;
      auto fn = std::bind(&WorkloadWrapper::produce_request, this, 
                      std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
      uint64_t n = num_ / threads_num;
      for(int i=0; i<threads_num; i++){
        if(i == threads_num - 1)
          n += num_ % threads_num;
        threads.emplace_back(fn, n, is_load, i == 0);
      }
      for(auto &t : threads)
        t.join();
      printf("\n");
      assert(load_index_ == num_);
      auto end = std::chrono::high_resolution_clock::now();
      double time = std::chrono::duration<double>(end - start).count();
    	printf("loading workload finished in %.3lf s\n", time);
  	}
  	~WorkloadWrapper(){
  		for(uint64_t i=0; i<num_; i++){
  			if(requests_[i] != nullptr){
  				delete requests_[i];
  			} 
  		}
  		if(requests_ != nullptr){
  			delete requests_;
  		}
  	}

  	Request* GetNextRequest(){
  		uint64_t index = __sync_fetch_and_add(&cursor_, 1);
  		assert(index < num_);
  		return requests_[index];
  	}

  private:
  	WorkloadProxy *workload_proxy_ = nullptr;
  	const uint64_t num_;
  	Request **requests_;
  	uint64_t cursor_;
    uint64_t load_index_;
};

}