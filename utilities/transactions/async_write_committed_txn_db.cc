
#ifndef ROCKSDB_LITE

#include "utilities/transactions/async_write_committed_txn.h"
#include "utilities/transactions/async_write_committed_txn_db.h"
#include "util/random.h"

namespace rocksdb {

Transaction* AsyncWriteCommittedTxnDB::BeginTransaction(
    const WriteOptions& write_options, const TransactionOptions& txn_options,
        Transaction* old_txn) {
    if (old_txn != nullptr) {
        ReinitializeTransaction(old_txn, write_options, txn_options);
        return old_txn;
    } else {
        return new AsyncWriteCommittedTxn(this, write_options, txn_options);
    }
}

Status AsyncWriteCommittedTxnDB::Write(const WriteOptions& opts, WriteBatch* updates) {
    if (txn_db_options_.skip_concurrency_control) {
        return db_impl_->Write(opts, updates);
    } else {
        return WriteWithConcurrencyControl(opts, updates);
  	}
}

Status AsyncWriteCommittedTxnDB::Write(const WriteOptions& opts,
				const TransactionDBWriteOptimizations& optimizations, WriteBatch* updates) {
  if (optimizations.skip_concurrency_control) {
		return db_impl_->Write(opts, updates);
  } else {
    return WriteWithConcurrencyControl(opts, updates);
	}
}

void AsyncWriteCommittedTxnDB::ScheduleFlushCompaction(){

}

void AsyncWriteCommittedTxnDB::DynamicLevelMoving(uint64_t &start_time){
  auto time_now = SPDK_TIME;
  if(SPDK_TIME_DURATION(start_time, time_now, spdk_tsc_rate) > 1000000){
    spandb_controller_.ComputeLDBandwidth(time_now * 1.0 / spdk_tsc_rate);
    //printf("dd read: %.2lf, dd write: %.2lf\n", spandb_controller_.GetDDWriteBandwidth(), spandb_controller_.GetDDReadBandwidth());
    //printf("ld write: %.2lf, read: %.2lf\n", spandb_controller_.GetLDWriteBandwidth(), spandb_controller_.GetLDReadBandwidth());
    start_time = time_now;
    if(spandb_controller_.GetLDBandwidth() > 1300){
      spandb_controller_.DecreaseMaxLevel();
    }else{
      spandb_controller_.IncreaseMaxLevel();
    }
    int level = spandb_controller_.GetMaxLevel();
    double left_space = spandb_controller_.GetLDLeftSpace();
    if(left_space < 50.0 && level > 1){
      spandb_controller_.SetMaxLevel(1);
    }else if(left_space < 30 ){
      spandb_controller_.SetMaxLevel(-2);
    }
    // printf("%d %.2lf %.2lf\n", spandb_controller_.GetMaxLevel(), spandb_controller_.GetLDBandwidth(), spandb_controller_.GetDDBandwidth());
  }
}

void AsyncWriteCommittedTxnDB::Worker(int worker_id, int core_id, bool is_master){
  ssdlogging::SetAffinity("SpanDB worker", core_id);
  int64_t epilog_seq = -1;
  int64_t read_seq = -1;
  double w1, w2, w3, p1,p2;
  bool skip_to_epilog = false;

  uint64_t start_time = SPDK_TIME;
  if(is_master){
    spandb_controller_.StartBandwidthStat(SPDK_TIME * 1.0 / spdk_tsc_rate, "sdb");
  }
  while(true){
    if(UNLIKELY(stop_.load()))
      break;

    if(is_master){
      // printf("imm num: %d\n", spandb_controller_.GetImmNum());
      if(dynamic_moving){
        DynamicLevelMoving(start_time);
      }
      // SwitchStatus(start_time);
    }
    if(spandb_controller_.GetAutoConfig()){
      if(is_master){
        // ScheduleFlushCompaction();
      }else{
        if(thread_roles_[worker_id].load() == ThreadRole::DEDICATED_FLUSHER){
          spandb_controller_.RunTask(Env::Priority::HIGH);
        }else if(thread_roles_[worker_id].load() == ThreadRole::DEDICATED_COMPACTOR){
          spandb_controller_.RunTask(Env::Priority::LOW);
        }else if(thread_roles_[worker_id].load() == ThreadRole::FLUSHER){
          spandb_controller_.RunOneTask(Env::Priority::HIGH);
        }else if(thread_roles_[worker_id].load() == ThreadRole::COMPACTOR){
          spandb_controller_.RunOneTask(Env::Priority::LOW);
        }
      }
    }
  
    w1 = last_proLog_wait_.load() / last_proLog_time_.load() * commit_queue_->Length();
    w2 = last_epiLog_wait_.load() / last_epiLog_time_.load() * db_impl_->LoggingQueueLength();
    w2 = last_proLog_wait_.load() / last_proLog_time_.load() * db_impl_->LoggingQueueLength();
    w3 = last_read_wait_.load() / last_read_time_.load() * request_queue_->Length();

    // w1 = commit_queue_->Length() * last_proLog_time_.load();
    // w2 = db_impl_->LoggingQueueLength() * last_epiLog_time_.load();
    // w3 = request_queue_->Length() * last_read_time_.load();


    if(w1 + w2 + w3  == 0){
      p1 = 0.33;
      p2 = 0.66;
    }else{
      p1 = w1/(w1+w2+w3);
      p2 = (w1+w2)/(w1+w2+w3);
    }
    double p = (Random::GetTLSInstance()->Next() & ((1<<20)-1))*1.0/(1<<20);
    if(p <= p1 && !skip_to_epilog){ // ProLog
      RequestScheduler::ThreadNum prolog_thrd_num(&proLog_thread_num_, max_prolog);
      if(prolog_thrd_num.Check()){
        skip_to_epilog = true;
        continue;
      }
      AsyncWriteCommittedTxn *txn = commit_queue_->ReadWithLock();
      if(txn == nullptr){
        continue;
      }
      
      last_proLog_wait_.store(SPDK_TIME_DURATION(txn->commit_time, SPDK_TIME, spdk_tsc_rate));
      #ifdef SPANDB_STAT
        prolog_wait.insert((SPDK_TIME_DURATION(txn->commit_time, SPDK_TIME, spdk_tsc_rate)));
      #endif

      auto start = SPDK_TIME;
      txn->ProcessProLog();
      last_proLog_time_.store(SPDK_TIME_DURATION(start, SPDK_TIME, spdk_tsc_rate));

      #ifdef SPANDB_STAT
        prolog_time.add((SPDK_TIME_DURATION(start, SPDK_TIME, spdk_tsc_rate)));
      #endif
  
      continue;
    }
    if((p > p1 && p <= p2) || (skip_to_epilog)){ // EPiLog
      skip_to_epilog = false;
      void *ptr = db_impl_->AsyncGetFromLoggingWithLock();
      if(ptr == nullptr)
        continue;

      AsyncWriteCommittedTxn *txn = static_cast<AsyncWriteCommittedTxn *>(ptr);
      auto start = SPDK_TIME;
      txn->ProcessEpiLog();
      last_epiLog_time_.store(SPDK_TIME_DURATION(start, SPDK_TIME, spdk_tsc_rate));
      #ifdef SPANDB_STAT
        epilog_time.add((SPDK_TIME_DURATION(start, SPDK_TIME, spdk_tsc_rate)));
      #endif
    }else{
      AsyncWriteCommittedTxn::AsyncTxnRequest *req = request_queue_->ReadWithLock();
      if(req == nullptr)
        continue;

      #ifdef SPANDB_STAT
        read_wait_time.insert(SPDK_TIME_DURATION(req->start_time, SPDK_TIME, spdk_tsc_rate));
      #endif

      last_read_wait_.store(SPDK_TIME_DURATION(req->start_time, SPDK_TIME, spdk_tsc_rate));
      auto start = SPDK_TIME;
      static_cast<AsyncWriteCommittedTxn *>(req->txn_)->ProcessRequest(req);
      last_read_time_.store(SPDK_TIME_DURATION(start, SPDK_TIME, spdk_tsc_rate));

      #ifdef SPANDB_STAT
          if(req->Type() == RequestScheduler::Read){
            read_time.add(SPDK_TIME_DURATION(start, SPDK_TIME, spdk_tsc_rate));
          }else if(req->Type() == RequestScheduler::Scan){
            // TODO
          }else if(req->Type() == RequestScheduler::IteratorReq){
            iterate_time.add(SPDK_TIME_DURATION(start, SPDK_TIME, spdk_tsc_rate));
          }else if(req->Type() == RequestScheduler::MultiGet){
            multi_read_time.add(SPDK_TIME_DURATION(start, SPDK_TIME, spdk_tsc_rate));
          }else{
            assert(0);
          }
      #endif

      delete req;
    }
  }
}

Status AsyncWriteCommittedTxnDB::EnqueueRequest(AsyncWriteCommittedTxn::AsyncTxnRequest *req){
  if(UNLIKELY(db_impl_->IsWriteDelay())){
    delete req;
    return Status::Busy("write stall");
  }
  if(req->CheckQueue()){
    if(request_queue_->Length() >= max_read_queue){
      delete req;
      return Status::Busy("too many request");
    }
  }
  request_queue_->WriteInBuf(req);
  // printf("enqueue...\n");
  return Status::OK();
}

Status AsyncWriteCommittedTxnDB::EnqueueCommit(AsyncWriteCommittedTxn *txn){
  commit_queue_->WriteInBuf(txn);
  return Status::OK();
}

void AsyncWriteCommittedTxnDB::Stop(){
  stop_.store(true);
  commit_queue_->stop();
  request_queue_->stop();
  for(auto &t : worker_threads_)
    t.join();
  if(compaction_queue != nullptr)
    delete compaction_queue;
  if(flush_queue != nullptr)
    delete flush_queue;
  printf("max_flush: %d\n", max_flush);
  printf("max_compaction: %d\n", max_compaction);
  printf("max_prolog: %d\n", max_prolog);
  #ifdef SPANDB_STAT
    if(prolog_wait.size() > 0){
      printf("Prolog wait (%ld) avg latency: %.3lf, median: %.3lf, P999: %.3lf, P99: %.3lf, P90: %lf\n", 
              prolog_wait.size(), prolog_wait.sum()/prolog_wait.size(), prolog_wait.get_tail(0.5),
              prolog_wait.get_tail(0.999),prolog_wait.get_tail(0.99),prolog_wait.get_tail(0.90));
    }
    //printf("prolog_wait: %ld %.2lf\n", prolog_wait.size(), prolog_wait.avg());
    printf("epilog_wait: %ld %.2lf\n", epilog_wait.size(), epilog_wait.avg());
    printf("prolog_time: %ld %.2lf\n", prolog_time.size(), prolog_time.avg());
    printf("epilog_time: %ld %.2lf\n", epilog_time.size(), epilog_time.avg());
    printf("read_time: %ld %.2lf\n", read_time.size(), read_time.avg());
    printf("iterate_time: %ld %.2lf\n", iterate_time.size(), iterate_time.avg());
    printf("multi_read_time: %ld %.2lf\n", multi_read_time.size(), multi_read_time.avg());
    printf("total worker time: %.2lf s\n", (prolog_time.sum() + epilog_time.sum() + 
                                            read_time.sum() + iterate_time.sum() + multi_read_time.sum())/1000.0/1000.0);
    if(read_wait_time.size() > 0){
      printf("Read wait (%ld) avg latency: %.3lf, median: %.3lf, P999: %.3lf, P99: %.3lf, P90: %lf\n", 
              read_wait_time.size(), read_wait_time.sum()/read_wait_time.size(), read_wait_time.get_tail(0.5),
              read_wait_time.get_tail(0.999),read_wait_time.get_tail(0.99),read_wait_time.get_tail(0.90));
    }
  #endif
}

}    // namespace rocksdb
#endif    // ROCKSDB_LITE
