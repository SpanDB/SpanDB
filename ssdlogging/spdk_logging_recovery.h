#pragma once

#include "array"
#include "atomic"
#include "spdk_device.h"
#include "spdk_logging_server.h"
#include "util.h"
#include "utilities/lock_free_queue/disruptor_queue.h"

namespace ssdlogging {

class SSDLoggingRecovery {
 private:
  struct LogPage {
    char *data;
    uint64_t lpn;
  };
  struct LogPageBuffer {
    LogPageBuffer()
        : lastRead_(-1),
          lastWrote_(-1),
          lastDispatch_(-1),
          writableSeq_(0),
          stop_(false) {}
    ~LogPageBuffer() { assert(stop_.load()); }
    static const int32_t size_ = (1 << 10);
    std::atomic<int64_t> lastRead_;
    std::atomic<int64_t> lastWrote_;
    std::atomic<int64_t> lastDispatch_;
    std::atomic<int64_t> writableSeq_;
    std::atomic<bool> stop_;
    std::array<char *, size_> buffer_;

    int64_t GetWriteSeq() {
      int64_t writableSeq = writableSeq_.fetch_add(1);
      while (writableSeq - lastRead_.load() > size_) {
        if (stop_.load()) return -1;
      }
      return writableSeq;
    }
    int64_t GetCurrentWriteSeq() { return writableSeq_; }
    void Write(int64_t writableSeq, char *data) {
      buffer_[writableSeq & (size_ - 1)] = data;
    }
    bool FinishWrite(int64_t writableSeq) {
      if (writableSeq - 1 != lastWrote_.load()) return false;
      lastWrote_.store(writableSeq);
      return true;
    }
    char *Read() {
      const int64_t readableSeq = lastDispatch_.fetch_add(1) + 1;
      while (readableSeq > lastWrote_.load()) {
        if (stop_.load() && Empty()) {
          return nullptr;
        }
      }
      char *tmp = buffer_[readableSeq & (size_ - 1)];
      while (readableSeq - 1 != lastRead_.load()) {
        //
      }
      lastRead_.store(readableSeq);
      return tmp;
    }
    bool Empty() { return lastWrote_.load() == lastRead_.load(); }
    void Stop() { stop_.store(true); }
  };

  LogPageBuffer *page_buffer_;

  struct LogEntry {
    char *data_;
    uint32_t size_;
    uint32_t curr_size;
    uint32_t log_tag_seq;
    LogEntry(uint32_t size, uint32_t seq) {
      data_ = (char *)malloc(sizeof(char) * size);
      size_ = size;
      curr_size = 0;
      log_tag_seq = seq;
    }
    void Append(char *data, uint32_t size) {
      if (curr_size + size > size_) {
        printf("curr_size: %d, size: %d, size_: %d", curr_size, size, size_);
        assert(curr_size + size <= size_);
      }
      memcpy(data_ + curr_size, data, size);
      curr_size += size;
    }
    bool Check() {
      if (curr_size != size_) return false;
      return true;
    }
  };

  rocksdb::DisruptorQueue<LogEntry *> *entry_buffer_;

  std::deque<SPDKLoggingServer::Log> logs_;
  std::deque<SPDKLoggingServer::Log> recovered_logs_;
  int reader_num_;
  std::thread recovery_server_;

  uint32_t base_log_tag_seq_;

  struct SPDKInfo *spdk;
  uint32_t PAGESIZE = 4096;

  void ReadFromSPDK(int coreid, int id, int reader_num, uint64_t start_lpn,
                    uint64_t end_lpn, uint32_t logfile_seq, int64_t first_seq);
  void ProcessData(int coreid);
  void RecoveryServer(int coreid);
  void ReadMetadata();

 public:
  char *GetLogEntry(uint32_t &size, uint32_t &log_tag_seq);
  std::vector<uint64_t> GetLogs();

  SSDLoggingRecovery(std::string addr, int reader_num)
      : reader_num_(reader_num) {
    if(spdk == nullptr)
      spdk = ssdlogging::InitSPDK(addr, 6);
    ReadMetadata();
    page_buffer_ = new LogPageBuffer();
    entry_buffer_ = new rocksdb::DisruptorQueue<LogEntry *>();
    auto fn = std::bind(&SSDLoggingRecovery::RecoveryServer, this,
                        std::placeholders::_1);
    recovery_server_ = std::thread(fn, 38);
  }
  ~SSDLoggingRecovery() { recovery_server_.join(); }
};

}  // namespace ssdlogging