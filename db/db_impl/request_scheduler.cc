/*
* Hao Chen
* 2020-03-19
*/

#include "db_impl.h"

namespace rocksdb {

thread_local ThreadLocalInfo thread_local_info_;
SpanDBController spandb_controller_;

void RequestScheduler::Init(){
    stop_.store(false);
    spdk_tsc_rate_ = spdk_get_ticks_hz();
    write_num_ = 0;
    read_num_ = 0;
    last_proLog_time_.store(1);
    last_epiLog_time_.store(1);
    last_read_time_.store(1);
    last_proLog_wait_.store(1);
    last_epiLog_wait_.store(1);
    last_read_wait_.store(1);
    last_batch_size_.store(0);
    delayed_num.store(0);

    read_queue = new DisruptorQueue<AsyncRequest *>();
    compaction_queue = new spandb::TaskQueue("compaction");
    flush_queue = new spandb::TaskQueue("flush");
    subcompaction_queue = new spandb::SubCompactionQueue("subcompaction");

    db_options_ = db_impl_->immutable_db_options();
    max_read_queue = db_options_.max_read_que_length;
    worker_num = db_options_.spandb_worker_num;
    dynamic_moving = db_options_.dynamic_moving;

    max_flush = 3;
    max_compaction = 6;
    if(max_flush <= 0)
      max_flush = 1;
    if(max_compaction <= 0)
      max_compaction = 1;

    spandb_controller_.SetAutoConfig(db_options_.auto_config);
    spandb_controller_.SetMaxCompactionJobs(max_compaction);
    spandb_controller_.SetMaxFlushJobs(max_flush);
    if(spandb_controller_.GetAutoConfig()){
      max_prolog = worker_num - max_flush - max_compaction;
    }else{
      max_prolog = worker_num;
    }

    thread_roles_ = new std::atomic<ThreadRole>[worker_num];
    auto fn = std::bind(&RequestScheduler::WorkerThread, this, std::placeholders::_1, 
                         std::placeholders::_2, std::placeholders::_3);
    int coresbase = spandb_controller_.GetFlushCores() + spandb_controller_.GetCompactionCores();
    for(int i=0; i<worker_num; i++){
      if(i >= 1 && i <= max_flush){
        thread_roles_[i].store(ThreadRole::DEDICATED_FLUSHER);
      }else if(i >= max_flush + 1 && i<= max_flush + max_compaction){
        thread_roles_[i].store(ThreadRole::DEDICATED_COMPACTOR);
      }else{
        thread_roles_[i].store(ThreadRole::WORKER);
      }
      worker_threads.emplace_back(fn, i, coresbase + i, i == 0);
    }
    for(int i=1; i<=db_options_.logging_server_num; i++){
      thread_roles_[worker_num - i].store(ThreadRole::LOGGER);
    }
}

Status RequestScheduler::EnqueueWrite(const WriteOptions& options,
                  ColumnFamilyHandle* column_family, const std::string key,
                  const std::string value, std::atomic<Status *> &status, bool is_merge){
  #ifdef SPANDB_STAT
    auto start = SPDK_TIME;
  #endif

  assert(status == nullptr);
  if(UNLIKELY(db_impl_->IsWriteDelay())){
    return Status::Busy("write stall");
  }

  AsyncRequest *req = new AsyncWriteRequest(options, column_family,
                                            key, value, status);
  if(is_merge){
    req->type_ = ReqType::Merge;
  }

  AsyncRequest* newest_req = batch_write_queue.load(std::memory_order_relaxed);
  while(true) {
		req->prev = newest_req;
    if (batch_write_queue.compare_exchange_weak(newest_req, req)){
			break ;
		}
	}

  request_sample_.Inc();
  #ifdef SPANDB_STAT
    enqueue_time_.add(SPDK_TIME_DURATION(start, SPDK_TIME, spdk_tsc_rate_));
  #endif
  return Status::OK();
}

Status RequestScheduler::EnqueueRead(const ReadOptions& options,
                       ColumnFamilyHandle* column_family, const std::string key,
                       std::string* value, std::atomic<Status *> &status){
  #ifdef SPANDB_STAT
    auto start = SPDK_TIME;
  #endif

  if(read_queue->Length() >= max_read_queue){
    return Status::Busy("Too many read requests\n");
  }
  assert(status == nullptr);
  assert(value != nullptr);
  __sync_fetch_and_add(&read_num_, 1);
  AsyncReadRequest *req = new AsyncReadRequest(options, column_family, key, value, status);                             
  read_queue->WriteInBuf(req);

  request_sample_.Inc();
  #ifdef SPANDB_STAT
    enqueue_time_.add(SPDK_TIME_DURATION(start, SPDK_TIME, spdk_tsc_rate_));
  #endif
  return Status::OK();
}


Status RequestScheduler::EnqueueScan(const ReadOptions& options,
                       ColumnFamilyHandle* column_family, const std::string key,
                       std::string* value, const int length, std::atomic<Status *> &status){
  #ifdef SPANDB_STAT
    auto start = SPDK_TIME;
  #endif

  if(read_queue->Length() >= max_read_queue){
      return Status::Busy("Too many read requests\n");
  }
  assert(status == nullptr);
  assert(value != nullptr);
  __sync_fetch_and_add(&read_num_, 1);
  AsyncScanRequest *req = new AsyncScanRequest(options, column_family, key, value, length, status);                             
  read_queue->WriteInBuf(req);

  request_sample_.Inc();
  #ifdef SPANDB_STAT
    enqueue_time_.add(SPDK_TIME_DURATION(start, SPDK_TIME, spdk_tsc_rate_));
  #endif
  return Status::OK();
}

void RequestScheduler::ProcessRead(AsyncReadRequest *req){
  auto start = SPDK_TIME;
  #ifdef SPANDB_STAT
    read_wait_.insert(SPDK_TIME_DURATION(req->start_time, SPDK_TIME, spdk_tsc_rate_));
  #endif
    last_read_wait_.store(SPDK_TIME_DURATION(req->start_time, SPDK_TIME, spdk_tsc_rate_));

    assert(req->value_ != nullptr);
    PinnableSlice pinnable_val(req->value_);
    assert(!pinnable_val.IsPinned());
    auto s = db_impl_->Get(req->read_options_, req->column_family_,
                           req->key_, &pinnable_val);
    if (s.ok() && pinnable_val.IsPinned()) {
      req->value_->assign(pinnable_val.data(), pinnable_val.size());
    } // else value is already assigned
    assert(req->status_.load() == nullptr);
    req->status_.store(new Status(s));
    delete req;
  double time = SPDK_TIME_DURATION(start, SPDK_TIME, spdk_tsc_rate_);
  last_read_time_.store(time);
  #ifdef SPANDB_STAT
    read_latency_.insert(time);
  #endif
}

void RequestScheduler::ProcessScan(AsyncScanRequest *req){
  auto start = SPDK_TIME;
  #ifdef SPANDB_STAT
    read_wait_.insert(SPDK_TIME_DURATION(req->start_time, SPDK_TIME, spdk_tsc_rate_));
  #endif
  last_read_wait_.store(SPDK_TIME_DURATION(req->start_time, SPDK_TIME, spdk_tsc_rate_));

  Iterator* iter = db_impl_->NewIterator(req->read_options_, req->column_family_);
	iter->Seek(req->key_);
	for (int i = 0; i < req->length_ && iter->Valid(); i++) {
    iter->Next();
  }
  req->status_.store(new Status(iter->status()));
  delete iter;
  delete req;

  double time = SPDK_TIME_DURATION(start, SPDK_TIME, spdk_tsc_rate_);
  last_read_time_.store(time);
  #ifdef SPANDB_STAT
    read_latency_.insert(time);
  #endif
}

void RequestScheduler::BatchProcessProLog(AsyncWriteRequest *leader){
    // last_proLog_wait_.store(SPDK_TIME_DURATION(leader->start_time, SPDK_TIME, spdk_tsc_rate_));
    double wait_time = 0;

    auto start = SPDK_TIME;
    size_t size = 24; //batch size
    int batchsize = 0;
    AsyncWriteRequest *head = leader;
    while(head != nullptr){
      size = size + head->key_.size() + head->value_.size();
      double wait = SPDK_TIME_DURATION(head->start_time, SPDK_TIME, spdk_tsc_rate_);
      wait_time += wait;
      #ifdef SPANDB_STAT
        prolog_write_wait_.add(wait);
      #endif
      head = dynamic_cast<AsyncWriteRequest *>(head->prev);
      batchsize++;
    }
    last_proLog_wait_.store(wait_time / batchsize);
    #ifdef SPANDB_STAT
      batch_size.add(batchsize);
    #endif
    leader->batch_size = batchsize;

    WriteBatch *batch = new WriteBatch(size*1.2);
    assert(batch != nullptr);
    Status s;
    head = leader;
    while(head != nullptr){
        if(head->Type() == ReqType::Merge){
          s = batch->Merge(head->column_family_, head->key_, head->value_);
        }else{
          assert((head->Type() == ReqType::Write));
          s = batch->Put(head->column_family_, head->key_, head->value_);
        }
        assert(s.ok());
        head->leader = leader;
        head = dynamic_cast<AsyncWriteRequest *>(head->prev);
    }
    leader->my_batch_ = batch;
    last_batch_size_.store(WriteBatchInternal::ByteSize(leader->my_batch_));

    // s = db_impl_->BatchProLog(leader->write_options_, leader->my_batch_, nullptr, 
                            //  leader->log_used_, 0, false, leader->seq_used_, 0, nullptr);
    s = db_impl_->ProLog(leader->write_options_, leader->my_batch_, nullptr, 
                                leader->log_used_, 0, false, leader->seq_used_, 0, nullptr);
    db_impl_->SPDKWriteWAL(leader->my_batch_, leader);

    double time = SPDK_TIME_DURATION(start, SPDK_TIME, spdk_tsc_rate_);
    last_proLog_time_.store(time);
    #ifdef SPANDB_STAT
      prolog_time_.add(time);
    #endif
}

void RequestScheduler::BatchProcessEPiLog(AsyncWriteRequest *leader){
  auto start = SPDK_TIME;
  #ifdef SPANDB_STAT
    epilog_wait_.add(SPDK_TIME_DURATION(leader->finish_logging, SPDK_TIME, spdk_tsc_rate_));
  #endif
  last_epiLog_wait_.store(SPDK_TIME_DURATION(leader->finish_logging, SPDK_TIME, spdk_tsc_rate_));

  Status s = db_impl_->BatchEPiLog(leader->write_options_, leader->my_batch_,
                                     leader->seq_used_, leader->log_used_);
  assert(leader->leader == leader);
  assert(leader->my_batch_ != nullptr);

  AsyncWriteRequest *head = leader;
  auto ss = SPDK_TIME;
  while(head != nullptr){
    assert(head->status_.load() == nullptr);
    *(head->tmp_status_) = s;
    head->status_.store(head->tmp_status_);
    // head->status_ = new Status(s);
    head = dynamic_cast<AsyncWriteRequest *>(head->prev);
  }

  //delete 
  head = leader;
  while(head != nullptr){
    AsyncWriteRequest *req = head;
    head = dynamic_cast<AsyncWriteRequest *>(head->prev);
    delete req;
  }

  double time = SPDK_TIME_DURATION(start, SPDK_TIME, spdk_tsc_rate_);
  last_epiLog_time_.store(time);
  #ifdef SPANDB_STAT
    epilog_time_.add(SPDK_TIME_DURATION(start, SPDK_TIME, spdk_tsc_rate_));
  #endif
}

int RequestScheduler::UnSchedule(void* arg, Env::Priority pri){
  assert(pri >= Env::Priority::BOTTOM && pri <= Env::Priority::HIGH);
  if(pri == Env::Priority::HIGH){
    return flush_queue->UnSchedule(arg);
  }else if(pri == Env::Priority::LOW){
    return compaction_queue->UnSchedule(arg);
  }else{
    assert(0);
  }
  return 0;
}

int RequestScheduler::Schedule(void (*function)(void* arg1), void* arg, Env::Priority pri,
                               void* tag, void (*unschedFunction)(void* arg)){
  assert(pri >= Env::Priority::BOTTOM && pri <= Env::Priority::HIGH);
  if(pri == Env::Priority::HIGH){
    if (unschedFunction == nullptr) {
      return flush_queue->Submit(std::bind(function, arg), std::function<void()>(), tag);
    } else {
      return flush_queue->Submit(std::bind(function, arg), std::bind(unschedFunction, arg), tag);
    }
  }else if(pri == Env::Priority::LOW){
    if (unschedFunction == nullptr) {
      return compaction_queue->Submit(std::bind(function, arg), std::function<void()>(), tag);
    } else {
      return compaction_queue->Submit(std::bind(function, arg), std::bind(unschedFunction, arg), tag);
    }
  }else{
    assert(0);
  }
  return 0;
}

int RequestScheduler::ScheduleSubcompaction(std::function<void()> schedule, std::atomic<int> *num){
  return subcompaction_queue->Submit(schedule, num);
}

void RequestScheduler::DynamicLevelMoving(uint64_t &start_time){
  auto time_now = SPDK_TIME;
  if(SPDK_TIME_DURATION(start_time, time_now, spdk_tsc_rate_) > 1000000){
    spandb_controller_.ComputeLDBandwidth(time_now * 1.0 / spdk_tsc_rate_);
    start_time = time_now;
    if(spandb_controller_.GetLDBandwidth() > 1300){
      spandb_controller_.DecreaseMaxLevel();
    }
    if(spandb_controller_.GetLDBandwidth() < 800){
      spandb_controller_.IncreaseMaxLevel();
    }
    int level = spandb_controller_.GetMaxLevel();
    double left_space = spandb_controller_.GetLDLeftSpace();
    if(left_space < 50.0 && level > 1){
      spandb_controller_.SetMaxLevel(1);
    }else if(left_space < 30 ){
      spandb_controller_.SetMaxLevel(-2);
    }
  }
}

void RequestScheduler::ScheduleFlushCompaction(){
  bool delayed = true;
  int num = 0;
  /*
  Status s = db_impl_->WillDelay(last_batch_size_.load(), delayed);
  if(delayed){
    int n = spandb_controller_.GetMaxCompactionJobs();
    if(n + 4 < 20 && n + 4 < worker_num - 2){
      num = n + 4;
    }
  }else if(request_sample_.GetNumber() >= 10000){
    num = 1;
  }
  if(request_sample_.GetNumber() < 10000){
    num = worker_num - max_flush - 4;
  }*/
  if(request_sample_.GetNumber() < 1000){
    num = worker_num - max_flush - 5;
    spandb_controller_.SpeedupCompaction(true);
  }else{
    num = max_compaction;
    spandb_controller_.SpeedupCompaction(false);
  }
  if(num == spandb_controller_.GetMaxCompactionJobs())
    return;
  spandb_controller_.SetMaxCompactionJobs(num);
  int inc = num - max_compaction;
  for(int i = max_flush + max_compaction + 1; i<worker_num; i++){
    if(inc > 0){
      thread_roles_[i].store(ThreadRole::COMPACTOR);
      inc--;
    }else{
      thread_roles_[i].store(ThreadRole::WORKER);
    }
  }
}

void RequestScheduler::WorkerThread(int worker_id, int core_id, bool is_master){
  ssdlogging::SetAffinity("SpanDB worker", core_id);
  int64_t epilog_seq = -1;
  int64_t read_seq = -1;
  double w1, w2, w3, p1,p2;
  bool skip_to_epilog = false;
  uint64_t start_time = SPDK_TIME;
  uint64_t num = 0;

  if(is_master){
    spandb_controller_.StartBandwidthStat(SPDK_TIME * 1.0 / spdk_tsc_rate_, "sdb");
  }

  while(true){
    auto start = SPDK_TIME;

    if(UNLIKELY(stop_.load()))
      break;

    if(thread_roles_[worker_id].load() == ThreadRole::LOGGER){
      logging_server_->Server(0,0,6);
    }

    if(is_master){
      if(dynamic_moving){
        DynamicLevelMoving(start_time);
      }
    }
    thread_local_info_.SetName("None");
    if(spandb_controller_.GetAutoConfig()){
      if(is_master){
        // ScheduleFlushCompaction();
      }else{
        if(thread_roles_[worker_id].load() == ThreadRole::DEDICATED_FLUSHER){
          flush_queue->RunTask();
        }else if(thread_roles_[worker_id].load() == ThreadRole::DEDICATED_COMPACTOR){
          // subcompaction_queue->RunOneTask();
          compaction_queue->RunTask();
        }else if(thread_roles_[worker_id].load() == ThreadRole::FLUSHER){
          flush_queue->RunOneTask();
        }else if(thread_roles_[worker_id].load() == ThreadRole::COMPACTOR){
          // subcompaction_queue->RunOneTask();
          compaction_queue->RunOneTask();
        }
      }
    }
    subcompaction_queue->RunOneTask();
    thread_local_info_.SetName("Worker");

    bool delayed = false;
    //Status s = db_impl_->WillDelay(last_batch_size_.load(), delayed);
    if(delayed){
      delayed_num.fetch_add(1);
      w1 = w2 = 0;
      w3 = last_read_wait_.load() / last_read_time_.load() * read_queue->Length();
      if(w3 == 0)
        continue;
    }else{
      if(skip_to_epilog || is_master){
        w1 = 0;
      }else{
        if(UNLIKELY(db_impl_->IsWriteDelay()) || batch_write_queue.load() == nullptr){
          w1 = 0;
        }else{
          w1 = last_proLog_wait_.load() / last_proLog_time_.load();
        }
      }
      w2 = last_epiLog_wait_.load() / last_epiLog_time_.load() * db_impl_->LoggingQueueLength();
      w3 = last_read_wait_.load() / last_read_time_.load() * read_queue->Length();
      // if(UNLIKELY(db_impl_->IsWriteDelay()) || batch_write_queue.load() == nullptr){
      //   w1 = 0;
      // }else{
      //   w1 = last_proLog_time_.load();
      // }
      // w2 = db_impl_->LoggingQueueLength() * last_epiLog_time_.load();
      // w3 = read_queue->Length() * last_read_time_.load();
    }
    if(w1+w2+w3 == 0)
      continue;
    p1 = w1/(w1+w2+w3);
    p2 = (w1+w2)/(w1+w2+w3);
    double p = (Random::GetTLSInstance()->Next() & ((1<<20)-1))*1.0/(1<<20);
    if(p < p1){ // ProLog
      assert(!skip_to_epilog);
      ThreadNum prolog_thrd_num(&proLog_thread_num_, max_prolog);
      if(prolog_thrd_num.Check()){
        skip_to_epilog = true;
        continue;
      }
      #ifdef SPANDB_STAT
        auto s = SPDK_TIME;
      #endif
      AsyncRequest* leader = batch_write_queue.exchange(nullptr, std::memory_order_relaxed);
      if(leader == nullptr){
        continue;
      }
      #ifdef SPANDB_STAT
        dequeue_time_.add(SPDK_TIME_DURATION(s, SPDK_TIME, spdk_tsc_rate_));
      #endif
      assert(leader->Type() == ReqType::Write || leader->Type() == ReqType::Merge);
      BatchProcessProLog(static_cast<AsyncWriteRequest *>(leader));
      continue;
    }
    skip_to_epilog = false;
    if(p >= p1 && p <= p2){ // EPiLog
      #ifdef SPANDB_STAT
        auto s = SPDK_TIME;
      #endif
      void *ptr = logging_server_->GetCompletedRequestWithLock();
      if(ptr == nullptr)
        continue;
      #ifdef SPANDB_STAT
        dequeue_time_.add(SPDK_TIME_DURATION(s, SPDK_TIME, spdk_tsc_rate_));
      #endif
      AsyncWriteRequest *req = static_cast<AsyncWriteRequest *>(ptr);
      BatchProcessEPiLog(req);
    }else{//Read
      #ifdef SPANDB_STAT
        auto s = SPDK_TIME;
      #endif
      AsyncRequest *req = read_queue->ReadWithLock();
      if(req == nullptr)
        continue;
      #ifdef SPANDB_STAT
        dequeue_time_.add(SPDK_TIME_DURATION(s, SPDK_TIME, spdk_tsc_rate_));
      #endif
      if(req->Type() == ReqType::Read){
        ProcessRead(static_cast<AsyncReadRequest *>(req));
      }else if(req->Type() == ReqType::Scan){
        ProcessScan(static_cast<AsyncScanRequest *>(req));
      }else{
        assert(false);
      }
    }
  }
}

void RequestScheduler::ResetStats(){
  #ifdef SPANDB_STAT
  prolog_time_.reset();
  epilog_time_.reset();
  write_to_wal_time_.reset();
  prolog_write_wait_.reset();
  epilog_wait_.reset();
  request_time_.reset();
  request_time_1.reset();
  request_time_2.reset();
  request_time_3.reset();
  request_time_4.reset();
  // read_latency_.reset();
  read_wait_.reset();
  enqueue_time_.reset();
  dequeue_time_.reset();
  batch_size.reset();
  read_latency_.reset();
  #endif
}

void RequestScheduler::Stop(){
  if(logging_server_ != nullptr){
    logging_server_->Stop();
  }
  stop_.store(true);
  if(read_queue != nullptr)
    read_queue->stop();
  for(auto &t : worker_threads)
    t.join();
  delete logging_server_;
  printf("----------SpanDB-Performance--------------\n");
  if(compaction_queue != nullptr)
    delete compaction_queue;
  if(flush_queue != nullptr)
    delete flush_queue;
  if(subcompaction_queue != nullptr){
    delete subcompaction_queue;
  }
  printf("max_flush: %d\n", max_flush);
  printf("max_compaction: %d\n", max_compaction);
  printf("max_prolog: %d\n", max_prolog);
  printf("delayed_num: %d\n", delayed_num.load());
  #ifdef SPANDB_STAT
    enqueue_time_.print("enqueue time");
    dequeue_time_.print("dequeue time");
    prolog_time_.print("prolog_time_");
    epilog_time_.print("epilog_time_");
    // write_to_wal_time_.print("write_to_wal_time_");
    prolog_write_wait_.print("prolog_write_wait_");
    epilog_wait_.print("epilog_wait_");
    // request_time_1.print("request_latency1");
    // request_time_2.print("request_latency2");
    // request_time_3.print("request_latency3");
    // request_time_4.print("request_latency4");
    // request_time_.print("request_latency");
    // read_latency_.print("read_latency");
    if(read_latency_.size() > 0){
      printf("worker read avg latency: %.3lf, median: %.3lf, P999: %.3lf, P99: %.3lf, P90: %lf\n", 
              read_latency_.sum()/read_latency_.size(), read_latency_.get_tail(0.5),
              read_latency_.get_tail(0.999),read_latency_.get_tail(0.99),read_latency_.get_tail(0.90));
      printf("read requests num: %ld\n", read_latency_.size());
    }
    if(read_wait_.size() > 0){
      printf("Read wait avg latency: %.3lf, median: %.3lf, P999: %.3lf, P99: %.3lf, P90: %lf\n", 
              read_wait_.sum()/read_wait_.size(), read_wait_.get_tail(0.5),
              read_wait_.get_tail(0.999),read_wait_.get_tail(0.99),read_wait_.get_tail(0.90));
    }
    // read_wait_.Print("read_wait");
    printf("prolog batch size: %.2lf\n", batch_size.avg());
  #endif
  printf("---------------------------------------\n");
}

}