/*
* Hao Chen
* 2020-03-19
*/
#pragma once

#include "spandb_task_que.h"

namespace rocksdb {

class DBImpl;

class RequestScheduler{
  public:
    enum ReqType {NoType, Read, Write, Scan, IteratorReq/*txn*/, MultiGet, Merge};
    struct AsyncRequest{
      const std::string key_;
      std::atomic<Status *> &status_;
      ColumnFamilyHandle* column_family_;
      ReqType type_;
      AsyncRequest* prev = nullptr;
      Status *tmp_status_ = nullptr;
      const uint64_t start_time;

      AsyncRequest(ColumnFamilyHandle* column_family,
                   const std::string key, std::atomic<Status *> &status,
                   ReqType type):
          key_(key),
          status_(status),
          column_family_(column_family),
          type_(type),
          prev(nullptr),
          start_time(SPDK_TIME){ tmp_status_ = new Status();}
      AsyncRequest(std::atomic<Status *> &status, ReqType type):
          status_(status),
          type_(type),
          start_time(SPDK_TIME){ }
      virtual ~AsyncRequest(){}
      ReqType Type(){return type_;}
    };
    struct AsyncReadRequest : AsyncRequest{
      public:
        std::string *value_;
        const ReadOptions& read_options_;
        AsyncReadRequest(const ReadOptions& options,
                        ColumnFamilyHandle* column_family,
                        const std::string key, std::string* value, 
                        std::atomic<Status *> &status):
            AsyncRequest(column_family, key, status, ReqType::Read),
            value_(value),
            read_options_(options){
          }
    };
    struct AsyncScanRequest : AsyncRequest{
      public:
        std::string *value_;
        const ReadOptions& read_options_;
        const int length_;
        AsyncScanRequest(const ReadOptions& options,
                        ColumnFamilyHandle* column_family,
                        const std::string key, std::string* value, 
                        const int length, std::atomic<Status *> &status):
            AsyncRequest(column_family, key, status, ReqType::Scan),
            value_(value),
            read_options_(options),
            length_(length){
          }
    };

    struct AsyncWriteRequest : AsyncRequest{
      public:
        const std::string value_;
        const WriteOptions& write_options_;
        WriteBatch* my_batch_;
        uint64_t *seq_used_;
        uint64_t *log_used_;
        const Slice* ts;
        uint64_t finish_logging;
        AsyncWriteRequest* leader;
        AsyncWriteRequest* next_batch_req;
        int batch_size = 0;

        AsyncWriteRequest(const WriteOptions& options,
                         ColumnFamilyHandle* column_family,
                         const std::string key, const std::string value, 
                         std::atomic<Status *> &status):
            AsyncRequest(column_family, key, status, ReqType::Write),
            value_(value),
            write_options_(options),
            ts(options.timestamp),
            leader(nullptr),
            next_batch_req(nullptr){
                seq_used_ = (uint64_t *)malloc(sizeof(uint64_t));
                log_used_ = (uint64_t *)malloc(sizeof(uint64_t));
                my_batch_ = nullptr;
                finish_logging = 0;
          }
          ~AsyncWriteRequest(){
            if(my_batch_ != nullptr){
              delete my_batch_;
            }
            if(seq_used_ != nullptr){
              free(seq_used_);
            }
            if(log_used_ != nullptr){
              free(log_used_);
            }
          }
    };

    struct ThreadNum{
      std::atomic<int>* num_;
      int total_;
      int cur_;
      ThreadNum(std::atomic<int> *num, int total):
          num_(num),
          total_(total){
            cur_ = num_->fetch_add(1) + 1;
      }
      bool Check(){
        return  cur_ >= total_;
      }
      ~ThreadNum(){
        num_->fetch_sub(1);
      }
    };

  private:
    DBImpl *db_impl_ = nullptr;
    ssdlogging::SPDKLoggingServer *logging_server_;
    DisruptorQueue<AsyncWriteRequest *> *write_queue = nullptr;
    DisruptorQueue<AsyncRequest *> *read_queue = nullptr;
    std::atomic<struct AsyncRequest *> batch_write_queue;
    enum ThreadRole {WORKER, COMPACTOR, FLUSHER, DEDICATED_COMPACTOR, DEDICATED_FLUSHER, LOGGER};
    std::atomic<ThreadRole> *thread_roles_;
    std::atomic<uint64_t> last_batch_size_;
    bool dynamic_moving = false;

    struct WriteQueue{
      private:
        const int64_t size_ = 2048;
        const int64_t batch_size = 128;
        std::atomic<int64_t> last_read_;
        std::atomic<int64_t> last_wrote_;
        std::atomic<int64_t> next_write_;
        std::atomic<int64_t> next_read_;
        port::Mutex mutex_;
        AsyncRequest **buffer_;
      public:
        WriteQueue():
          last_read_(-1),
          last_wrote_(-1),
          next_write_(0),
          next_read_(0){
            buffer_ = new AsyncRequest*[size_];
            for(int64_t i=0; i<size_; i++)
              buffer_[i] = nullptr;
        }
        ~WriteQueue(){
          delete buffer_;
        }
        void Put(AsyncRequest* value){
          const int64_t seq = next_write_.fetch_add(1);
          while (seq - last_read_.load() > size_){

          }
          buffer_[seq % size_] = value;
          while (seq - 1 != last_wrote_.load()){

          }
          last_wrote_.store(seq);
        }
        AsyncRequest* Get(){
          int64_t from = -1;
          int64_t end = -1;
          {
            MutexLock lock(&mutex_);
            from = next_read_;
            if(from > last_wrote_.load()){
              return nullptr;
            }
            end = from + batch_size;
            if(end > last_wrote_.load()){
              end = last_wrote_.load();
            }
            next_read_ = end + 1;
          }
          AsyncRequest *res =  buffer_[from % size_];
          AsyncRequest *head = res;
          for(int64_t seq = from+1; seq<=end; seq++){
            head->prev = buffer_[seq % size_];
            head = buffer_[seq % size_];
          }
          head->prev = nullptr;
          while (from - 1 != last_read_.load()){

          }
          last_read_.store(end);
          return res;
        }
        int64_t Size(){
          int64_t size = last_wrote_.load(std::memory_order_relaxed) - last_read_.load(std::memory_order_relaxed);
          return (size == 0) ? 0 : (size/batch_size + 1);
        }
    };

    struct RequestSample{
      private:
        std::atomic<uint64_t> request_speed;
        std::atomic<uint64_t> last_time;
        std::atomic<uint64_t> num;
        const uint64_t interval = 0.5 * 1000000;//1s = 1000000us
        const uint64_t tsc_rate;
      public:
        RequestSample():
          request_speed(0),
          last_time(SPDK_TIME),
          num(0),
          tsc_rate(spdk_get_ticks_hz()){

          }
        void Inc(){
          auto duration = SPDK_TIME_DURATION(last_time, SPDK_TIME, tsc_rate);
          if(duration > interval){
            last_time.store(SPDK_TIME);
            uint64_t n = num.load();
            num.store(0);
            request_speed.store(n/(duration/1000000));
          }else{
            num.fetch_add(1);
          }
        }
        uint64_t GetNumber(){
          auto duration = SPDK_TIME_DURATION(last_time, SPDK_TIME, tsc_rate);
          if(duration > interval){
            last_time.store(SPDK_TIME);
            uint64_t n = num.load();
            num.store(0);
            if(duration > 2*interval){
              request_speed.store(0);
            }else{
              request_speed.store(n/(duration/1000000));
            }
          }
          return request_speed.load();
        }
    };

    RequestSample request_sample_;

    spandb::TaskQueue *compaction_queue = nullptr;
    spandb::TaskQueue *flush_queue = nullptr;
    spandb::SubCompactionQueue *subcompaction_queue = nullptr;

    void ScheduleFlushCompaction();
    int max_flush = 1;
    int max_compaction = 1;
    int max_prolog = 1;
    std::atomic<int> delayed_num;
    int worker_num = 0;

    void ProcessRead(AsyncReadRequest *req);
    void ProcessScan(AsyncScanRequest *req);
    void BatchProcessProLog(AsyncWriteRequest *leader);
    void BatchProcessEPiLog(AsyncWriteRequest *leader);
    void WorkerThread(int worker_id, int core_id, bool is_master);
    void DynamicLevelMoving(uint64_t &start_time);
    void Init();
    void Stop();
    uint64_t spdk_tsc_rate_;

    #ifdef SPANDB_STAT
      ssdlogging::statistics::AvgStat prolog_time_;
      ssdlogging::statistics::AvgStat epilog_time_;
      ssdlogging::statistics::AvgStat write_to_wal_time_;
      ssdlogging::statistics::AvgStat prolog_write_wait_;
      ssdlogging::statistics::AvgStat epilog_wait_;
      ssdlogging::statistics::AvgStat request_time_;
      ssdlogging::statistics::AvgStat request_time_1;
      ssdlogging::statistics::AvgStat request_time_2;
      ssdlogging::statistics::AvgStat request_time_3;
      ssdlogging::statistics::AvgStat request_time_4;
      ssdlogging::statistics::AvgStat enqueue_time_;
      ssdlogging::statistics::AvgStat dequeue_time_;
      ssdlogging::statistics::AvgStat batch_size;
      ssdlogging::statistics::MySet read_latency_;
      ssdlogging::statistics::MySet read_wait_;
    #endif

    uint64_t write_num_;
    uint64_t read_num_;

    std::atomic<int> proLog_thread_num_;
    int max_read_queue = 1;

    std::atomic<double> last_proLog_time_;
    std::atomic<double> last_epiLog_time_;
    std::atomic<double> last_read_time_;
    std::atomic<double> last_proLog_wait_;
    std::atomic<double> last_epiLog_wait_;
    std::atomic<double> last_read_wait_;
    std::atomic<bool> stop_;

    std::vector<std::thread> worker_threads;
    ImmutableDBOptions db_options_;

  public:
    RequestScheduler(DBImpl *db_impl, ssdlogging::SPDKLoggingServer *logging_server):
        db_impl_(db_impl),
        logging_server_(logging_server){
          batch_write_queue.store(nullptr);
          proLog_thread_num_.store(0);
          Init();
    }
    ~RequestScheduler(){
        Stop();
        printf("Delete request scheduler server...\n");
    }

    int Schedule(void (*function)(void* arg), void* arg,
                 Env::Priority pri = Env::Priority::LOW, void* tag = nullptr,
                 void (*unschedFunction)(void* arg) = nullptr);
    int ScheduleSubcompaction(std::function<void()> schedule, std::atomic<int> *num);                    
    int UnSchedule(void* arg, Env::Priority pri);


    Status EnqueueWrite(const WriteOptions& options,
                  ColumnFamilyHandle* column_family, const std::string key,
                  const std::string value, std::atomic<Status *> &status, bool is_merge);
    Status EnqueueRead(const ReadOptions& options,
                     ColumnFamilyHandle* column_family, const std::string key,
                     std::string* value, std::atomic<Status *> &status);
    Status EnqueueScan(const ReadOptions& options,
                       ColumnFamilyHandle* column_family, const std::string key,
                       std::string* value, const int length, std::atomic<Status *> &status);

    void ResetStats();
};

extern RequestScheduler *request_scheduler_;

}