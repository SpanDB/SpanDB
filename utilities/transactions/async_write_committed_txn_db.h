#pragma once

#ifndef ROCKSDB_LITE

#include <algorithm>
#include <atomic>
#include <mutex>
#include <stack>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/write_callback.h"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/snapshot.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "util/autovector.h"
#include "utilities/lock_free_queue/disruptor_queue.h"
#include "utilities/transactions/async_write_committed_txn.h"
#include "utilities/transactions/transaction_base.h"
#include "utilities/transactions/transaction_util.h"

#include "../../db/db_impl/spandb_task_que.h"

namespace rocksdb {

class AsyncWriteCommittedTxnDB : public PessimisticTransactionDB {
 public:
  explicit AsyncWriteCommittedTxnDB(DB* db,
                                    const TransactionDBOptions& txn_db_options)
      : PessimisticTransactionDB(db, txn_db_options) {
    
    spdk_tsc_rate = spdk_get_ticks_hz();
    request_queue_ =  new DisruptorQueue<AsyncWriteCommittedTxn::AsyncTxnRequest*>();
    commit_queue_ =  new DisruptorQueue<AsyncWriteCommittedTxn*>();
    compaction_queue = new spandb::TaskQueue("compaction");
    flush_queue = new spandb::TaskQueue("flush");
    stop_.store(false);

    last_proLog_time_.store(1);
    last_epiLog_time_.store(1);
    last_read_time_.store(1);
    last_proLog_wait_.store(1);
    last_epiLog_wait_.store(1);
    last_read_wait_.store(1);

    proLog_thread_num_.store(0);

    ImmutableDBOptions options = db_impl_->immutable_db_options();
    /*max_read_queue = options.max_read_que_length;
    int logs = options.logging_server_num;
    int n = options.before_server_num;
    int m = options.after_server_num;
    total_thread_ = n + m;
    auto fn = std::bind(&AsyncWriteCommittedTxnDB::Worker,
                        this, std::placeholders::_1);
    for(int i=0; i<n+m; i++){
      worker_threads_.emplace_back(fn, logs + 1 + i);
    }*/

    max_read_queue = options.max_read_que_length;
    worker_num = options.spandb_worker_num;
    dynamic_moving = options.dynamic_moving;

    max_compaction = 1;
    max_flush = 1;
    spandb_controller_.SetAutoConfig(options.auto_config);
    spandb_controller_.SetMaxCompactionJobs(max_compaction);
    spandb_controller_.SetMaxFlushJobs(max_flush);
    if(spandb_controller_.GetAutoConfig()){
      max_prolog = worker_num - max_flush - max_compaction;
    }else{
      max_prolog = worker_num;
    }

    thread_roles_ = new std::atomic<ThreadRole>[worker_num];
    auto fn = std::bind(&AsyncWriteCommittedTxnDB::Worker, this, std::placeholders::_1, 
                         std::placeholders::_2, std::placeholders::_3);
    int coresbase = options.logging_server_num + spandb_controller_.GetFlushCores() 
                    + spandb_controller_.GetCompactionCores() + 1;
    for(int i=0; i<worker_num; i++){
      if(i >= 1 && i <= max_flush){
        thread_roles_[i].store(ThreadRole::DEDICATED_FLUSHER);
      }else if(i >= max_flush + 1 && i<= max_flush + max_compaction){
        thread_roles_[i].store(ThreadRole::DEDICATED_COMPACTOR);
      }else{
        thread_roles_[i].store(ThreadRole::WORKER);
      }
      worker_threads_.emplace_back(fn, i, coresbase + i, i == 0);
    }
  }

  explicit AsyncWriteCommittedTxnDB(StackableDB* db,
                                    const TransactionDBOptions& txn_db_options)
      : PessimisticTransactionDB(db, txn_db_options) {}

  virtual ~AsyncWriteCommittedTxnDB() { Stop(); }

  Transaction* BeginTransaction(const WriteOptions& write_options,
                                const TransactionOptions& txn_options,
                                Transaction* old_txn) override;
  using TransactionDB::Write;
  virtual Status Write(const WriteOptions& opts,
                       const TransactionDBWriteOptimizations& optimizations,
                       WriteBatch* updates) override;
  virtual Status Write(const WriteOptions& opts, WriteBatch* updates) override;

  Status EnqueueRequest(AsyncWriteCommittedTxn::AsyncTxnRequest *req);

  Status EnqueueCommit(AsyncWriteCommittedTxn *txn);

 private:
  friend class AsyncWriteCommittedTxn;

  void Stop();
  void Worker(int worker_id, int core_id, bool is_master);

  uint64_t spdk_tsc_rate;
  std::atomic<bool> stop_;
  DisruptorQueue<AsyncWriteCommittedTxn::AsyncTxnRequest*>* request_queue_;
  DisruptorQueue<AsyncWriteCommittedTxn*>* commit_queue_;
  std::vector<std::thread> worker_threads_;
  std::atomic<double> last_proLog_time_;
  std::atomic<double> last_epiLog_time_;
  std::atomic<double> last_read_time_;
  std::atomic<double> last_proLog_wait_;
  std::atomic<double> last_epiLog_wait_;
  std::atomic<double> last_read_wait_;
  std::atomic<int> proLog_thread_num_;
  std::atomic<int> epiLog_thread_num_;
  int total_thread_ = 0;

  uint64_t total_request_  = 0;
  int max_read_queue = 1;

  spandb::TaskQueue *compaction_queue = nullptr;
  spandb::TaskQueue *flush_queue = nullptr;
  void ScheduleFlushCompaction();
  void DynamicLevelMoving(uint64_t &start_time);
  enum ThreadRole {WORKER, COMPACTOR, FLUSHER, DEDICATED_COMPACTOR, DEDICATED_FLUSHER};
  int max_flush = 1;
  int max_compaction = 1;
  int max_prolog = 1;
  int worker_num = 0;
  bool dynamic_moving = false;
  std::atomic<ThreadRole> *thread_roles_ = nullptr;

  #ifdef SPANDB_STAT
    ssdlogging::statistics::MySet prolog_wait;
    ssdlogging::statistics::AvgStat epilog_wait;
    ssdlogging::statistics::AvgStat prolog_time;
    ssdlogging::statistics::AvgStat epilog_time;
    ssdlogging::statistics::AvgStat read_time;
    ssdlogging::statistics::AvgStat iterate_time;
    ssdlogging::statistics::AvgStat multi_read_time;
    ssdlogging::statistics::MySet read_wait_time;
  #endif

};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE