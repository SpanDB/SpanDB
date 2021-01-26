#pragma once

#include "atomic"
#include "deque"
#include "mutex"
#include "spdk_device.h"
#include "util.h"
#include "utilities/lock_free_queue/disruptor_queue.h"

// 8GB = (2<<20) * 4KB
#define MAX_LPN_FOR_LOGGING (2ul << 20)
#define LOGGING_START_LPN 1

namespace ssdlogging {

class SPDKLoggingServer {
 public:
  struct Log {
    Log(uint32_t _seq, uint64_t _start_lpn) {
      seq = _seq;
      start_lpn = _start_lpn;
      current_lpn = _start_lpn;
      end_lpn = 0;
      size_.store(0);
    }
    Log(uint32_t _seq, uint64_t _start_lpn, uint64_t _end_lpn) {
      seq = _seq;
      start_lpn = _start_lpn;
      end_lpn = _end_lpn;
      size_.store(0);
    }
    Log() {}
    void Add(uint64_t size) { size_.fetch_add(size); }
    uint64_t Size() { return size_.load(); }
    uint32_t seq;
    uint64_t start_lpn;
    uint64_t end_lpn;
    uint64_t current_lpn;
    std::atomic<uint64_t> size_;
  };

  struct LoggingRequest {
    explicit LoggingRequest(const char *data, uint32_t size, void *ptr)
        : data_(data),
          size_(size),
          ptr_(ptr),
          prev(nullptr),
          start_time(SPDK_TIME) {}
    const char *data_;
    uint32_t size_;
    void *ptr_;  // pointer of the request in rocskdb
    std::atomic<LoggingRequest *> prev;
    const uint64_t start_time;
  };

 private:
  std::deque<Log> alive_log_files_;
  std::deque<Log> logs_;

  struct SPDKInfo *spdk = nullptr;
  uint64_t current_offset;
  int logs_num_;
  char **buffer = nullptr;
  std::string device_;
  rocksdb::DisruptorQueue<LoggingRequest *> *completion_queue;
  std::atomic<LoggingRequest *> submission_queue;

#ifdef PRINT_BANDWIDTH
  std::atomic<uint64_t> last_print_time_;
#endif

  std::atomic<bool> stop_;
  std::vector<std::thread> servers_;
  uint32_t logfile_seq_;
  uint64_t log_size_;
  uint32_t base_log_tag_seq_ = 110000;
  char *metadata;

#ifdef SPANDB_STAT
  statistics::MyStat spdklogging_time;
  statistics::MyStat sync_time;
  statistics::MyStat wait_time;
  statistics::MyStat temp_time;
  statistics::AvgStat request_size;
  statistics::AvgStat batch_size;
  statistics::MySet sync_time_dis;
  statistics::MySet batch_size_dis;
  statistics::MySet sync_time_per_request;
  uint64_t req_sizes[10];
  std::atomic<uint64_t> server_start_time_;
  std::atomic<uint64_t> server_stop_time_;
  statistics::MyMap spdk_queue_length_;
  statistics::MyMap spdk_logging_time_dis_;
  std::atomic<uint64_t> queue_length_;
#endif

  uint64_t spdk_tsc_rate_;
  inline uint64_t MemCopy(LoggingRequest *leader, int queue_id);
  inline void SPDKWrite(struct NSEntry *ns_entry, uint64_t size, int queue_id,
                        bool *flag);
  void SetLastLogfileSeq();
  void PrintStatistics();
  void PrintMetadata();
  void WriteMetadata();

  const uint64_t BUFFER_SIZE = 4096 * 1024;
  const uint32_t PAGESIZE = 4096;

  uint64_t total_log_written = 0;
  uint64_t total_page_written = 0;

  uint64_t last_pickup_time = SPDK_TIME;

 public:
  void Server(int coreid, int serverid, int pipeline_num);
  void *GetCompletedRequest();
  void *AsyncGetCompletedRequest(int64_t &seq);
  void *GetCompletedRequestWithLock();
  int GetCompleteQueueLen();
  bool AsyncFinishReading(int64_t seq);
  void AddLog(const char *data, int size, void *ptr);
  void Stop();
  SPDKInfo *GetSPDK() { return spdk; };
  void ReadVeriy();
  uint32_t GetCurrentLogSeq();
  uint64_t GetLogSize();
  void DeleteLog(uint32_t min_log_number, uint32_t size);
  void CreateWAL(uint32_t log_file_num);
  void AddSize(size_t size);
  void ResetStats();

  SPDKLoggingServer(std::string device, int log_num, int server_num)
      : current_offset(LOGGING_START_LPN),  // start from Page 1
        logs_num_(log_num),
        device_(device),
        completion_queue(nullptr),
        stop_(false),
        logfile_seq_(0),
        log_size_(0) {
    // 1. initialize SPDK
    spdk = ssdlogging::InitSPDK(device_, logs_num_);
    assert(spdk != nullptr);
    spdk_tsc_rate_ = spdk_get_ticks_hz();
    submission_queue.store(nullptr);

#ifdef PRINT_BANDWIDTH
    last_print_time_.store(0);
#endif

    // 2. initialize writer server
    completion_queue = new rocksdb::DisruptorQueue<LoggingRequest *>();
    buffer = new char *[logs_num_];
    for (int i = 0; i < logs_num_; i++) {
      buffer[i] = (char *)spdk_zmalloc(BUFFER_SIZE, PAGESIZE, NULL,
                                       SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    }
    assert(logs_num_ % server_num == 0);

    // auto fn = std::bind(&SPDKLoggingServer::Server, this,
    // std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
    // int corebase = rocksdb::spandb_controller_.GetFlushCores() +
    //                rocksdb::spandb_controller_.GetCompactionCores() + 1;
    // for(int i=0; i<server_num; i++){ 
    //   servers_.emplace_back(fn, corebase + i, i, logs_num_/server_num);
    // }

    // 3. others
    metadata = (char *)spdk_zmalloc(PAGESIZE, PAGESIZE, NULL,
                                    SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    //SetLastLogfileSeq();

#ifdef SPANDB_STAT
    queue_length_.store(0);
    server_start_time_.store(0);
    for (int i = 0; i < 10; i++) req_sizes[i] = 0;
#endif
  }

  ~SPDKLoggingServer() {
    printf("Delete SPDK logging server...\n");
    PrintMetadata();
    PrintStatistics();
    for (auto &t : servers_) 
      t.join();
    for (int i = 0; i < logs_num_; i++) {
      spdk_free(buffer[i]);
    }
    spdk_free(metadata);
    free(buffer);
    free(completion_queue);
    // uint64_t lsn = __sync_fetch_and_add(&current_offset, 8);
  }
};  // class SPDKLoggingServer
}  // namespace ssdlogging