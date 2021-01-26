
#pragma once
#include "../db/db_impl/spandb_task_que.h"
#include "atomic"
#include "iostat.h"
#include "rocksdb/env.h"

namespace rocksdb {
class LDBandwidth {
 private:
  std::atomic<uint64_t> written;
  std::atomic<uint64_t> read;
  std::atomic<uint64_t> ld_bg_io;
  std::atomic<double> read_bandwidth;
  std::atomic<double> write_bandwidth;
  std::atomic<double> ld_bg_bandwidth;
  uint64_t last_written;
  uint64_t last_read;
  uint64_t last_ld_bg_io;
  double last_check_time;

 public:
  LDBandwidth()
      : written(0),
        read(0),
        ld_bg_io(0),
        read_bandwidth(0),
        write_bandwidth(0),
        ld_bg_bandwidth(0),
        last_written(0),
        last_read(0),
        last_ld_bg_io(0),
        last_check_time(0) {}
  void IncreaseLDWritten(uint64_t value) { written.fetch_add(value); }
  void IncreaseLDRead(uint64_t value) { read.fetch_add(value); }
  void IncreaseLDBgIO(uint64_t value) { ld_bg_io.fetch_add(value); }
  void Start(double time) {
    last_check_time = time;
    last_written = written.load();
    last_read = read.load();
    last_ld_bg_io = ld_bg_io.load();
  }
  void ComputeBandwidth(double time_now) {
    uint64_t w = written.load();
    uint64_t r = read.load();
    uint64_t bg = ld_bg_io.load();
    double time = time_now - last_check_time;
    double w_bandwidth = (w - last_written) / time / (1024 * 1024);  // MB/s
    double r_bandwidth = (r - last_read) / time / (1024 * 1024);     // MB/s
    double bg_bandwidth = (bg - last_ld_bg_io) / time / (1024 * 1024);
    write_bandwidth.store(w_bandwidth);
    read_bandwidth.store(r_bandwidth);
    ld_bg_bandwidth.store(bg_bandwidth);
    last_written = w;
    last_read = r;
    last_ld_bg_io = bg;
    last_check_time = time_now;
  }
  double GetWriteBandwidth() { return write_bandwidth.load(); }
  double GetReadBandwidth() { return read_bandwidth.load(); }
  double GetBandwidth() {
    return write_bandwidth.load() + read_bandwidth.load();
  }
  double GetLDBgBandwidth() { return ld_bg_bandwidth.load(); }
};

class DDBandwidth {
 private:
  std::atomic<double> read_bandwidth;
  std::atomic<double> write_bandwidth;
  unsigned long long last_written;
  unsigned long long last_read;
  double last_check_time;
  ssdlogging::StatIO* stat_io = nullptr;

 public:
  DDBandwidth()
      : read_bandwidth(0),
        write_bandwidth(0),
        last_written(0),
        last_read(0),
        last_check_time(0) {}
  ~DDBandwidth() {
    if (stat_io != nullptr) delete stat_io;
  }
  void Start(double time, std::string disk_name) {
    stat_io = new ssdlogging::StatIO(disk_name);
    last_check_time = time;
    stat_io->GetTotalMBytes(last_read, last_written);
  }
  void ComputeBandwidth(double time_now) {
    assert(stat_io != nullptr);
    unsigned long long r, w;
    stat_io->GetTotalMBytes(r, w);
    double time = time_now - last_check_time;
    double r_bandwidth = (r - last_read) / time;
    double w_bandwidth = (w - last_written) / time;
    write_bandwidth.store(w_bandwidth);
    read_bandwidth.store(r_bandwidth);
    last_read = r;
    last_written = w;
    last_check_time = time_now;
  }
  double GetWriteBandwidth() { return write_bandwidth.load(); }
  double GetReadBandwidth() { return read_bandwidth.load(); }
  double GetBandwidth() {
    return write_bandwidth.load() + read_bandwidth.load();
  }
};

class SpanDBController {
 private:
  LDBandwidth ld_bandwidth;
  DDBandwidth dd_bandwidth;
  std::atomic<int> max_level;
  int rocksdb_compaction_cores = 0;
  int rocksdb_flush_cores = 0;
  std::atomic<uint64_t> ld_left_space;  // num of pages
  std::atomic<int> num_of_imm_;
  bool auto_config_ = false;
  std::atomic<int> max_compaction_jobs;
  std::atomic<int> max_flush_jobs;
  std::atomic<bool> speedup_compaction;

  spandb::TaskQueue* compaction_queue = nullptr;
  spandb::TaskQueue* flush_queue = nullptr;

 public:
  SpanDBController()
      : max_level(0),
        ld_left_space(0),
        num_of_imm_(0),
        max_compaction_jobs(0),
        max_flush_jobs(0),
        speedup_compaction(false) {
    compaction_queue = new spandb::TaskQueue("compaction");
    flush_queue = new spandb::TaskQueue("flush");
  }
  ~SpanDBController() {
    if (compaction_queue != nullptr) delete compaction_queue;
    if (flush_queue != nullptr) delete flush_queue;
  }

  void SetMaxLevel(int level) { max_level.store(level); }
  int GetMaxLevel() { return max_level.load(); }
  void IncreaseMaxLevel() {
    int level = max_level.load() + 1;
    if (level <= 4) max_level.store(level);
  }
  void DecreaseMaxLevel() {
    int level = max_level.load() - 1;
    if (level >= -1) max_level.store(level);
  }
  void SetCompactionCores(int num) { rocksdb_compaction_cores = num; }
  int GetCompactionCores() { return rocksdb_compaction_cores; }
  void SetFlushCores(int num) { rocksdb_flush_cores = num; }
  int GetFlushCores() { return rocksdb_flush_cores; }

  void IncreaseLDLeftSpace(uint64_t value) { ld_left_space.fetch_add(value); }
  void DecreaseLDLeftSpace(uint64_t value) {
    assert(ld_left_space.load() >= value);
    ld_left_space.fetch_sub(value);
  }
  double GetLDLeftSpace() {
    return ld_left_space.load() * 4 * 1.0 / (1024 * 1024);  // GB size
  }

  void StartBandwidthStat(double time, std::string dd_name) {
    ld_bandwidth.Start(time);
    dd_bandwidth.Start(time, dd_name);
  }
  void ComputeLDBandwidth(double time_now) {
    ld_bandwidth.ComputeBandwidth(time_now);
    dd_bandwidth.ComputeBandwidth(time_now);
  }

  double GetDDReadBandwidth() { return dd_bandwidth.GetReadBandwidth(); }
  double GetDDWriteBandwidth() { return dd_bandwidth.GetWriteBandwidth(); }
  double GetDDBandwidth() { return dd_bandwidth.GetBandwidth(); }

  void IncreaseLDWritten(uint64_t value) {
    ld_bandwidth.IncreaseLDWritten(value);
  }
  void IncreaseLDRead(uint64_t value) { ld_bandwidth.IncreaseLDRead(value); }
  void IncreaseLDBgIO(uint64_t value) { ld_bandwidth.IncreaseLDBgIO(value); }
  double GetLDReadBandwidth() { return ld_bandwidth.GetReadBandwidth(); }
  double GetLDWriteBandwidth() { return ld_bandwidth.GetWriteBandwidth(); }
  double GetLDBandwidth() { return ld_bandwidth.GetBandwidth(); }
  double GetLDBgBandwidth() { return ld_bandwidth.GetLDBgBandwidth(); }

  void IncreaseImm() { num_of_imm_.fetch_add(1); }
  void DecreaseImm() { num_of_imm_.fetch_sub(1); }
  int GetImmNum() {
    int num = num_of_imm_.load();
    assert(num >= 0);
    return num;
  }

  void SetAutoConfig(bool auto_config) { auto_config_ = auto_config; }
  bool GetAutoConfig() { return auto_config_; }

  void SetMaxCompactionJobs(int num) {
    if (num <= 0) num = 1;
    max_compaction_jobs.store(num);
  }
  void SetMaxFlushJobs(int num) {
    if (num <= 0) num = 1;
    max_flush_jobs.store(num);
  }
  int GetMaxCompactionJobs() { return max_compaction_jobs.load(); }
  int GetMaxFlushJobs() { return max_flush_jobs.load(); }

  void SpeedupCompaction(bool speedup) { speedup_compaction.store(speedup); }
  bool NeddSpeedupCompaction() { return speedup_compaction.load(); }

  int Schedule(void (*function)(void* arg1), void* arg, Env::Priority pri,
               void* tag = nullptr,
               void (*unschedFunction)(void* arg) = nullptr) {
    assert(pri >= Env::Priority::BOTTOM && pri <= Env::Priority::HIGH);
    if (pri == Env::Priority::HIGH) {
      if (unschedFunction == nullptr) {
        return flush_queue->Submit(std::bind(function, arg),
                                   std::function<void()>(), tag);
      } else {
        return flush_queue->Submit(std::bind(function, arg),
                                   std::bind(unschedFunction, arg), tag);
      }
    } else if (pri == Env::Priority::LOW) {
      if (unschedFunction == nullptr) {
        return compaction_queue->Submit(std::bind(function, arg),
                                        std::function<void()>(), tag);
      } else {
        return compaction_queue->Submit(std::bind(function, arg),
                                        std::bind(unschedFunction, arg), tag);
      }
    } else {
      assert(0);
    }
    return 0;
  }

  int UnSchedule(void* arg, Env::Priority pri) {
    assert(pri >= Env::Priority::BOTTOM && pri <= Env::Priority::HIGH);
    if (pri == Env::Priority::HIGH) {
      return flush_queue->UnSchedule(arg);
    } else if (pri == Env::Priority::LOW) {
      return compaction_queue->UnSchedule(arg);
    } else {
      assert(0);
    }
    return 0;
  }

  void RunTask(Env::Priority pri) {
    assert(pri >= Env::Priority::BOTTOM && pri <= Env::Priority::HIGH);
    if (pri == Env::Priority::HIGH) {
      flush_queue->RunTask();
    } else if (pri == Env::Priority::LOW) {
      compaction_queue->RunTask();
    } else {
      assert(0);
    }
  }

  void RunOneTask(Env::Priority pri) {
    assert(pri >= Env::Priority::BOTTOM && pri <= Env::Priority::HIGH);
    if (pri == Env::Priority::HIGH) {
      flush_queue->RunOneTask();
    } else if (pri == Env::Priority::LOW) {
      compaction_queue->RunOneTask();
    } else {
      assert(0);
    }
  }
};
extern SpanDBController spandb_controller_;
};  // namespace rocksdb
