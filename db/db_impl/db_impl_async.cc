
#include "db_impl.h"

namespace rocksdb{

void DBImpl::CreateRequestSchduler(){
    assert(spdk_logging_server_ != nullptr);
    request_scheduler_ = new RequestScheduler(this, spdk_logging_server_);
}

Status DBImpl::AsyncPut(const WriteOptions& options,
                     ColumnFamilyHandle* column_family, const std::string key,
                     const std::string value, std::atomic<Status *> &status){
    return request_scheduler_->EnqueueWrite(options, column_family, key, value, status, false);
}

Status DBImpl::AsyncMerge(const WriteOptions& options,
                     ColumnFamilyHandle* column_family, const std::string key,
                     const std::string value, std::atomic<Status *> &status){
    return request_scheduler_->EnqueueWrite(options, column_family, key, value, status, true);
}

Status DBImpl::AsyncGet(const ReadOptions& options,
                     ColumnFamilyHandle* column_family, const std::string key,
                     std::string* value, std::atomic<Status *> &status){
    assert(value != nullptr);
    assert(status.load() == nullptr);
    PinnableSlice pinnable_val(value);
    assert(!pinnable_val.IsPinned());
    if(GetFromMemtable(options, column_family, key, &pinnable_val, status) || status.load() != nullptr){
        if (status.load()->ok() && pinnable_val.IsPinned()) {
            value->assign(pinnable_val.data(), pinnable_val.size());
        }
        return Status::OK();
    }
    return request_scheduler_->EnqueueRead(options, column_family, key, value, status);
}

Status DBImpl::AsyncScan(const ReadOptions& options,
                     ColumnFamilyHandle* column_family, const std::string key,
                     std::string* value, const int length, std::atomic<Status *> &status){
    
    return request_scheduler_->EnqueueScan(options, column_family, key, value, length, status);
}

bool DBImpl::GetFromMemtable(const ReadOptions& read_options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     PinnableSlice* value, std::atomic<Status *> &status){
    GetImplOptions get_impl_options;
    get_impl_options.column_family = column_family;
    get_impl_options.value = value;

    assert(get_impl_options.value != nullptr || get_impl_options.merge_operands != nullptr);

    PERF_CPU_TIMER_GUARD(get_cpu_nanos, env_);
    StopWatch sw(env_, stats_, DB_GET);
    PERF_TIMER_GUARD(get_snapshot_time);

    auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(get_impl_options.column_family);
    auto cfd = cfh->cfd();

    if (tracer_) {
        // TODO: This mutex should be removed later, to improve performance when
        // tracing is enabled.
        InstrumentedMutexLock lock(&trace_mutex_);
        if (tracer_) {
            tracer_->Get(get_impl_options.column_family, key);
        }
    }

    // Acquire SuperVersion
    SuperVersion* sv = GetAndRefSuperVersion(cfd);

    TEST_SYNC_POINT("DBImpl::GetImpl:1");
    TEST_SYNC_POINT("DBImpl::GetImpl:2");

    SequenceNumber snapshot;
    if (read_options.snapshot != nullptr) {
        if (get_impl_options.callback) {
            // Already calculated based on read_options.snapshot
            snapshot = get_impl_options.callback->max_visible_seq();
        } else {
            snapshot = reinterpret_cast<const SnapshotImpl*>(read_options.snapshot)->number_;
        }
    } else {
        // Note that the snapshot is assigned AFTER referencing the super
        // version because otherwise a flush happening in between may compact away
        // data for the snapshot, so the reader would see neither data that was be
        // visible to the snapshot before compaction nor the newer data inserted
        // afterwards.
        snapshot = last_seq_same_as_publish_seq_
                    ? versions_->LastSequence()
                    : versions_->LastPublishedSequence();
        if (get_impl_options.callback) {
            // The unprep_seqs are not published for write unprepared, so it could be
            // that max_visible_seq is larger. Seek to the std::max of the two.
            // However, we still want our callback to contain the actual snapshot so
            // that it can do the correct visibility filtering.
            get_impl_options.callback->Refresh(snapshot);
            // Internally, WriteUnpreparedTxnReadCallback::Refresh would set
            // max_visible_seq = max(max_visible_seq, snapshot)
            //
            // Currently, the commented out assert is broken by
            // InvalidSnapshotReadCallback, but if write unprepared recovery followed
            // the regular transaction flow, then this special read callback would not
            // be needed.
            //
            // assert(callback->max_visible_seq() >= snapshot);
            snapshot = get_impl_options.callback->max_visible_seq();
        }
    }
    TEST_SYNC_POINT("DBImpl::GetImpl:3");
    TEST_SYNC_POINT("DBImpl::GetImpl:4");

    // Prepare to store a list of merge operations if merge occurs.
    MergeContext merge_context;
    SequenceNumber max_covering_tombstone_seq = 0;

    Status s;
    // First look in the memtable, then in the immutable memtable (if any).
    // s is both in/out. When in, s could either be OK or MergeInProgress.
    // merge_operands will contain the sequence of merges in the latter case.
    LookupKey lkey(key, snapshot, read_options.timestamp);
    PERF_TIMER_STOP(get_snapshot_time);

    bool skip_memtable = (read_options.read_tier == kPersistedTier &&
                            has_unpersisted_data_.load(std::memory_order_relaxed));
    bool done = false;
    if (!skip_memtable) {
        // Get value associated with key
        if (get_impl_options.get_value) {
            if (sv->mem->Get(lkey, get_impl_options.value->GetSelf(), &s,
                            &merge_context, &max_covering_tombstone_seq,
                            read_options, get_impl_options.callback,
                            get_impl_options.is_blob_index)) {
                done = true;
                get_impl_options.value->PinSelf();
                RecordTick(stats_, MEMTABLE_HIT);
            } else if ((s.ok() || s.IsMergeInProgress()) &&
                sv->imm->Get(lkey, get_impl_options.value->GetSelf(), &s,
                                    &merge_context, &max_covering_tombstone_seq,
                                    read_options, get_impl_options.callback,
                                    get_impl_options.is_blob_index)) {
                done = true;
                get_impl_options.value->PinSelf();
                RecordTick(stats_, MEMTABLE_HIT);
            }
        } else {
            // Get Merge Operands associated with key, Merge Operands should not be
            // merged and raw values should be returned to the user.
            if (sv->mem->Get(lkey, nullptr, &s, &merge_context,
                            &max_covering_tombstone_seq, read_options, nullptr,
                            nullptr, false)) {
                done = true;
                RecordTick(stats_, MEMTABLE_HIT);
            } else if ((s.ok() || s.IsMergeInProgress()) &&
                        sv->imm->GetMergeOperands(lkey, &s, &merge_context,
                                                &max_covering_tombstone_seq,
                                                read_options)) {
                done = true;
                RecordTick(stats_, MEMTABLE_HIT);
            }
        }
        if (!done && !s.ok() && !s.IsMergeInProgress()) {
            ReturnAndCleanupSuperVersion(cfd, sv);
            status.store(new Status(s));
            return done;
        }
    }
    ReturnAndCleanupSuperVersion(cfd, sv);
    if(done){
        PERF_TIMER_GUARD(get_post_process_time);
        RecordTick(stats_, NUMBER_KEYS_READ);
        size_t size = 0;
        if (s.ok()) {
        if (get_impl_options.get_value) {
            size = get_impl_options.value->size();
        } else {
            // Return all merge operands for get_impl_options.key
            *get_impl_options.number_of_operands =
                static_cast<int>(merge_context.GetNumOperands());
            if (*get_impl_options.number_of_operands >
                get_impl_options.get_merge_operands_options->expected_max_number_of_operands) {
                s = Status::Incomplete(Status::SubCode::KMergeOperandsInsufficientCapacity);
            } else {
                for (const Slice& sl : merge_context.GetOperands()) {
                    size += sl.size();
                    get_impl_options.merge_operands->PinSelf(sl);
                    get_impl_options.merge_operands++;
                }
            }
        }
        RecordTick(stats_, BYTES_READ, size);
        PERF_COUNTER_ADD(get_read_bytes, size);
        }
        RecordInHistogram(stats_, BYTES_PER_READ, size);
        status.store(new Status(s));
    }
    return done;
}

Status DBImpl::AsyncDelete(const WriteOptions& options,
                        ColumnFamilyHandle* column_family,
                        const std::string key, std::atomic<Status*> &status){
    return Status::OK();
}


Status DBImpl::ProLog(const WriteOptions& write_options,
                         WriteBatch* my_batch,
                         WriteCallback* callback,
                         uint64_t* log_used, uint64_t log_ref,
                         bool disable_memtable, uint64_t* seq_used,
                         size_t batch_cnt,
                         PreReleaseCallback* pre_release_callback){
    assert(!seq_per_batch_ || batch_cnt != 0);
    if (my_batch == nullptr) {
        return Status::Corruption("Batch is nullptr!");
    }
    if (tracer_) {
        InstrumentedMutexLock lock(&trace_mutex_);
        if (tracer_) {
            tracer_->Write(my_batch);
        }
    }
    if (write_options.sync && write_options.disableWAL) {
        return Status::InvalidArgument("Sync writes has to enable WAL.");
    }
    if (two_write_queues_ && immutable_db_options_.enable_pipelined_write) {
        return Status::NotSupported(
                "pipelined_writes is not compatible with concurrent prepares");
    }
    if (seq_per_batch_ && immutable_db_options_.enable_pipelined_write) {
        return Status::NotSupported(
                "pipelined_writes is not compatible with seq_per_batch");
    }
    if (immutable_db_options_.unordered_write &&
            immutable_db_options_.enable_pipelined_write) {
        return Status::NotSupported(
                "pipelined_writes is not compatible with unordered_write");
    }
    // Otherwise IsLatestPersistentState optimization does not make sense
    assert(!WriteBatchInternal::IsLatestPersistentState(my_batch) ||
                 disable_memtable);

    Status status;
    if (write_options.low_pri) {
        status = ThrottleLowPriWritesIfNeeded(write_options, my_batch);
        if (!status.ok()) {
            return status;
        }
    }

    if (two_write_queues_ && disable_memtable) {
        return Status::NotSupported(
                "two_write_queues_ and disable_memtable not support in this mode");
    }

    if (immutable_db_options_.unordered_write) {
        return Status::NotSupported(
                "unordered_write not support in this mode");
    }

    if (immutable_db_options_.enable_pipelined_write) {
        return Status::NotSupported("pipelined_write not support in this mode");
    }

    PERF_TIMER_GUARD(write_pre_and_post_process_time);
    WriteThread::Writer w(write_options, my_batch, callback, log_ref,
                            disable_memtable, batch_cnt, pre_release_callback);

    if (!write_options.disableWAL) {
        RecordTick(stats_, WRITE_WITH_WAL);
    }

    StopWatch write_sw(env_, immutable_db_options_.statistics.get(), DB_WRITE);

    bool need_log_sync = write_options.sync;
    WriteContext write_context;

    assert(!seq_per_batch_);
    
    assert(w.ShouldWriteToMemtable());
    size_t total_count = WriteBatchInternal::Count(w.batch);

    mutex_.Lock();
    if (!two_write_queues_ || !disable_memtable) {
        PERF_TIMER_STOP(write_pre_and_post_process_time);
        //  last_batch_group_size_ = WriteBatchInternal::ByteSize(my_batch);
        status = PreprocessWrite(write_options, &need_log_sync, &write_context);
        assert(!two_write_queues_);
        PERF_TIMER_START(write_pre_and_post_process_time);
        uint64_t last_sequence = versions_->LastSequence();
        w.sequence = last_sequence + 1;
        versions_->SetLastSequence(last_sequence+total_count);
    }
    pending_memtable_writes_++;
    mutex_.Unlock();
    // uint64_t last_sequence = 
    //         versions_->FetchAddLastAllocatedSequence(total_count);
    w.log_used = spdk_logging_server_->GetCurrentLogSeq();
    WriteBatchInternal::SetSequence(my_batch, w.sequence);
    if(seq_used != nullptr)
      *seq_used = w.sequence;
    if(log_used != nullptr)
      *log_used = w.log_used;
    return status;
}


Status DBImpl::BatchProLog(const WriteOptions& write_options, WriteBatch* my_batch,
                   WriteCallback* callback,
                   uint64_t* log_used, uint64_t log_ref,
                   bool disable_memtable, uint64_t* seq_used,
                   size_t batch_cnt,
                   PreReleaseCallback* pre_release_callback){
    assert(!seq_per_batch_ || batch_cnt != 0);
    if (my_batch == nullptr) {
        return Status::Corruption("Batch is nullptr!");
    }
    if (tracer_) {
        InstrumentedMutexLock lock(&trace_mutex_);
        if (tracer_) {
            tracer_->Write(my_batch);
        }
    }
    if (write_options.sync && write_options.disableWAL) {
        return Status::InvalidArgument("Sync writes has to enable WAL.");
    }
    if (two_write_queues_ && immutable_db_options_.enable_pipelined_write) {
        return Status::NotSupported(
                "pipelined_writes is not compatible with concurrent prepares");
    }
    if (seq_per_batch_ && immutable_db_options_.enable_pipelined_write) {
        return Status::NotSupported(
                "pipelined_writes is not compatible with seq_per_batch");
    }
    if (immutable_db_options_.unordered_write &&
            immutable_db_options_.enable_pipelined_write) {
        return Status::NotSupported(
                "pipelined_writes is not compatible with unordered_write");
    }
    // Otherwise IsLatestPersistentState optimization does not make sense
    assert(!WriteBatchInternal::IsLatestPersistentState(my_batch) ||
                 disable_memtable);

    Status status;
    if (write_options.low_pri) {
        status = ThrottleLowPriWritesIfNeeded(write_options, my_batch);
        if (!status.ok()) {
            return status;
        }
    }

    if (two_write_queues_ && disable_memtable) {
        return Status::NotSupported(
                "two_write_queues_ and disable_memtable not support in this mode");
    }

    if (immutable_db_options_.unordered_write) {
        return Status::NotSupported(
                "unordered_write not support in this mode");
    }

    if (immutable_db_options_.enable_pipelined_write) {
        return Status::NotSupported("pipelined_write not support in this mode");
    }


    PERF_TIMER_GUARD(write_pre_and_post_process_time);
    WriteThread::Writer w(write_options, my_batch, callback, log_ref,
                        disable_memtable, batch_cnt, pre_release_callback);


    if (!write_options.disableWAL) {
        RecordTick(stats_, WRITE_WITH_WAL);
    }

    StopWatch write_sw(env_, immutable_db_options_.statistics.get(), DB_WRITE);

    write_thread_.JoinBatchGroup(&w);
    if (w.state == WriteThread::STATE_PARALLEL_MEMTABLE_WRITER) {
        assert(false);
    }
    if (w.state == WriteThread::STATE_COMPLETED) {
        if (log_used != nullptr) {
            *log_used = w.log_used;
        }
        if (seq_used != nullptr) {
            *seq_used = w.sequence;
        }
        // write is complete and leader has updated sequence
        return w.FinalStatus();
    }
    // else we are the leader of the write batch group
    assert(w.state == WriteThread::STATE_GROUP_LEADER);

    WriteContext write_context;
    WriteThread::WriteGroup write_group;
    bool in_parallel_group = false;
    uint64_t last_sequence = kMaxSequenceNumber;

    mutex_.Lock();
    bool need_log_sync = write_options.sync;
    bool need_log_dir_sync = need_log_sync && !log_dir_synced_;
    if (!two_write_queues_ || !disable_memtable) {
        PERF_TIMER_STOP(write_pre_and_post_process_time);
        status = PreprocessWrite(write_options, &need_log_sync, &write_context);
        PERF_TIMER_START(write_pre_and_post_process_time);
    }
    // log::Writer* log_writer = logs_.back().writer;
    mutex_.Unlock();

    size_t seq_inc = 0;

    TEST_SYNC_POINT("DBImpl::WriteImpl:BeforeLeaderEnters");
    last_batch_group_size_ =
        write_thread_.EnterAsBatchGroupLeader(&w, &write_group);

    if (status.ok()) {
        bool parallel = immutable_db_options_.allow_concurrent_memtable_write &&
                        write_group.size > 1;
        size_t total_count = 0;
        size_t valid_batches = 0;
        size_t total_byte_size = 0;
        size_t pre_release_callback_cnt = 0;
        for (auto* writer : write_group) {
            writer->log_used = spdk_logging_server_->GetCurrentLogSeq();
            if (writer->CheckCallback(this)) {
                valid_batches += writer->batch_cnt;
                if (writer->ShouldWriteToMemtable()) {
                    total_count += WriteBatchInternal::Count(writer->batch);
                    parallel = parallel && !writer->batch->HasMerge();
                }
                total_byte_size = WriteBatchInternal::AppendedByteSize(
                    total_byte_size, WriteBatchInternal::ByteSize(writer->batch));
                if (writer->pre_release_callback) {
                    pre_release_callback_cnt++;
                }
            }
        }

        seq_inc = seq_per_batch_ ? valid_batches : total_count;

        const bool concurrent_update = two_write_queues_;

        auto stats = default_cf_internal_stats_;
        stats->AddDBStats(InternalStats::kIntStatsNumKeysWritten, total_count,
                        concurrent_update);
        RecordTick(stats_, NUMBER_KEYS_WRITTEN, total_count);
        stats->AddDBStats(InternalStats::kIntStatsBytesWritten, total_byte_size,
                        concurrent_update);
        RecordTick(stats_, BYTES_WRITTEN, total_byte_size);
        stats->AddDBStats(InternalStats::kIntStatsWriteDoneBySelf, 1,
                        concurrent_update);
        RecordTick(stats_, WRITE_DONE_BY_SELF);
        auto write_done_by_other = write_group.size - 1;
        if (write_done_by_other > 0) {
            stats->AddDBStats(InternalStats::kIntStatsWriteDoneByOther,
                            write_done_by_other, concurrent_update);
            RecordTick(stats_, WRITE_DONE_BY_OTHER, write_done_by_other);
        }
        RecordInHistogram(stats_, BYTES_PER_WRITE, total_byte_size);

        if (write_options.disableWAL) {
            has_unpersisted_data_.store(true, std::memory_order_relaxed);
        }

        assert(!two_write_queues_);
    
        last_sequence = versions_->FetchAddLastAllocatedSequence(seq_inc);
        uint64_t current_sequence = last_sequence + 1;
        if (status.ok()) {
            SequenceNumber next_sequence = current_sequence;
            for (auto* writer : write_group) {
                assert(!writer->CallbackFailed());
                assert(!writer->pre_release_callback);
                assert(!seq_per_batch_);    
                writer->sequence = next_sequence;
                next_sequence += WriteBatchInternal::Count(writer->batch);
            }
        }
        pending_memtable_writes_ += write_group.size;
        if (status.ok() && seq_used != nullptr) {
            *seq_used = w.sequence;
        }
    }

    if (!w.CallbackFailed()) {
        WriteStatusCheck(status);
    }

    if (status.ok()) {
        versions_->SetLastSequence(last_sequence + seq_inc);
    }
    write_thread_.ExitAsBatchGroupLeader(write_group, status);
    
    if (status.ok()) {
        status = w.FinalStatus();
    }
    return status;
}

Status DBImpl::BatchEPiLog(const WriteOptions& write_options, WriteBatch* my_batch, 
                      uint64_t *seq_used, uint64_t *log_used){
    WriteThread::Writer w(write_options, my_batch, nullptr, 0,
                            0, 0, nullptr);
    w.sequence = *seq_used;
    w.log_used = *log_used;
    WriteBatchInternal::SetSequence(my_batch, w.sequence);
    ColumnFamilyMemTablesImpl column_family_memtables(versions_->GetColumnFamilySet());
    {
        PERF_TIMER_GUARD(write_memtable_time);
        w.status = WriteBatchInternal::InsertInto(
                &w, w.sequence, &column_family_memtables, &flush_scheduler_,
                &trim_history_scheduler_,
                write_options.ignore_missing_column_families, 0 /*log_number*/,
                this, true /*concurrent_memtable_writes*/, seq_per_batch_,
                w.batch_cnt, batch_per_txn_,
                write_options.memtable_insert_hint_per_batch);
    }
    size_t pending_cnt = pending_memtable_writes_.fetch_sub(1) - 1;
    if (pending_cnt == 0) {
        std::lock_guard<std::mutex> lck(switch_mutex_);
        switch_cv_.notify_all();
    }
    return w.status;
}

void DBImpl::SPDKWriteWAL(WriteBatch* my_batch, void *ptr){
  assert(spdk_logging_server_ != nullptr);
  Slice log_entry = WriteBatchInternal::Contents(my_batch);
  spdk_logging_server_->AddLog(log_entry.data(), log_entry.size(), ptr);
  log_empty_ = false;
}

void DBImpl::StopLogging(){
  if(request_scheduler_ != nullptr){
    delete request_scheduler_;
  }
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::WillDelay(uint64_t num_bytes, bool &delayed){
  uint64_t time_delayed = 0;
  delayed = false;
  {
    StopWatch sw(env_, stats_, WRITE_STALL, &time_delayed);
    uint64_t delay = write_controller_.GetDelay(env_, num_bytes);
    if (delay > 0) {
      begin_write_stall_.store(true);
      const uint64_t kDelayInterval = 1000;
      uint64_t stall_end = sw.start_time() + delay;
      if(write_controller_.NeedsDelay()) {
        delayed = true;
      }
    }else{
      begin_write_stall_.store(false);
    }

    if(error_handler_.GetBGError().ok() && write_controller_.IsStopped()) {
      delayed = true;
      begin_write_stall_.store(true);
    }else{
      begin_write_stall_.store(false);
    }
  }

  // If DB is not in read-only mode and write_controller is not stopping
  // writes, we can ignore any background errors and allow the write to
  // proceed
  Status s;
  if (write_controller_.IsStopped()) {
    // If writes are still stopped, it means we bailed due to a background
    // error
    s = Status::Incomplete(error_handler_.GetBGError().ToString());
  }
  if (error_handler_.IsDBStopped()) {
    s = error_handler_.GetBGError();
  }
  return s;
}

}