#ifndef ROCKSDB_LITE

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/transaction_db.h"
#include "util/cast_util.h"
#include "utilities/transactions/pessimistic_transaction.h"
#include "utilities/transactions/write_prepared_txn_db.h"

#include "utilities/transactions/async_write_committed_txn.h"
#include "utilities/transactions/async_write_committed_txn_db.h"

namespace rocksdb {

AsyncWriteCommittedTxn::AsyncWriteCommittedTxn(TransactionDB* txn_db,
						const WriteOptions& write_options,
						const TransactionOptions& txn_options)
		: PessimisticTransaction(txn_db, write_options, txn_options){
			txn_db_ = txn_db;
			is_finished.store(false);
      start_time = SPDK_TIME;
	};

void AsyncWriteCommittedTxn::Initialize(const TransactionOptions& txn_options) {
	PessimisticTransaction::Initialize(txn_options);
	is_finished.store(false);
  start_time = SPDK_TIME;
}

Status AsyncWriteCommittedTxn::AsyncGet(const ReadOptions& options,
                     ColumnFamilyHandle* column_family, const std::string key,
                     std::string* value, std::atomic<Status *> &status){
  AsyncTxnReadRequest *req = new AsyncTxnReadRequest(options, column_family, key, value, status, this);
  return static_cast<AsyncWriteCommittedTxnDB *>(txn_db_)->EnqueueRequest(req);
}

Status AsyncWriteCommittedTxn::AsyncGet(const ReadOptions& options, const std::string key,
                     std::string* value, std::atomic<Status *> &status){
  return AsyncGet(options, db_->DefaultColumnFamily(), key, value, status);
}

Status AsyncWriteCommittedTxn::AsyncMultiGet(const ReadOptions& options,
      ColumnFamilyHandle* column_family,
      const std::vector<std::string> keys,
      std::vector<std::string>* values,
      std::vector<Status>* statuses, std::atomic<Status *> &status , bool check_queue){
  AsyncTxnMultiGetRequest *req = new AsyncTxnMultiGetRequest(options, column_family, keys, values, statuses, status, this);
  req->SetCheck(check_queue);
  return static_cast<AsyncWriteCommittedTxnDB *>(txn_db_)->EnqueueRequest(req);
};
  
Status AsyncWriteCommittedTxn::AsyncScan(const ReadOptions& options, const std::string key,
                     std::string* value, const int length, std::atomic<Status *> &status){
  return AsyncScan(options, db_->DefaultColumnFamily(), key, value, length, status);                      
}

Status AsyncWriteCommittedTxn::AsyncScan(const ReadOptions& options,
                     ColumnFamilyHandle* column_family, const std::string key,
                     std::string* value, const int length, std::atomic<Status *> &status){
  AsyncTxnScanRequest *req = new AsyncTxnScanRequest(options, column_family, key, value, length, status, this);
  return static_cast<AsyncWriteCommittedTxnDB *>(txn_db_)->EnqueueRequest(req);
}

Status AsyncWriteCommittedTxn::AsyncGetIterator(const ReadOptions& read_options, Iterator *&iterator, std::atomic<Status *>&status){
  return AsyncGetIterator(read_options, db_->DefaultColumnFamily(),iterator, status);
}

Status AsyncWriteCommittedTxn::AsyncGetIterator(const ReadOptions& read_options, ColumnFamilyHandle* column_family,
                                Iterator *&iterator, std::atomic<Status *> &status){
  AsyncTxnIteratorRequest *req = new AsyncTxnIteratorRequest(read_options, column_family, iterator, status, this);
  return static_cast<AsyncWriteCommittedTxnDB *>(txn_db_)->EnqueueRequest(req);                              
}

void AsyncWriteCommittedTxn::ProcessProLog(){
	bool commit_without_prepare = false;
	bool commit_prepared = false;

	if (IsExpired()) {
		final_status_ = Status::Expired();
		is_finished.store(true);
		return;
	}

	if (expiration_time_ > 0) {
		TransactionState expected = STARTED;
		commit_without_prepare = std::atomic_compare_exchange_strong(
				&txn_state_, &expected, AWAITING_COMMIT);
		TEST_SYNC_POINT("TransactionTest::ExpirableTransactionDataRace:1");
	} else if (txn_state_ == PREPARED) {
		commit_prepared = true;
	} else if (txn_state_ == STARTED) {
		commit_without_prepare = true;
	}

	if (commit_without_prepare) {
		if (WriteBatchInternal::Count(GetCommitTimeWriteBatch()) > 0) {
			final_status_ = Status::InvalidArgument(
				"Commit-time batch contains values that will not be committed.");
		}else{
			txn_state_.store(AWAITING_COMMIT);
			uint64_t seq_used = kMaxSequenceNumber;
			final_status_ = db_impl_->WriteImplBeforeLogging(write_options_, GetWriteBatch()->GetWriteBatch(),
			 						/*callback*/ nullptr, /*log_used*/ nullptr,
			 						/*log_ref*/ 0, /*disable_memtable*/ false, &seq_used);
			assert(!final_status_.ok() || seq_used != kMaxSequenceNumber);
			seq_used_ = seq_used;
			db_impl_->SPDKWriteWAL( GetWriteBatch()->GetWriteBatch(), this);
			return;
		}
	} else if (commit_prepared) {
		final_status_ = Status::NotSupported("commit prepared not support in async.");
	} else if (txn_state_ == LOCKS_STOLEN) {
		final_status_ = Status::Expired();
	} else if (txn_state_ == COMMITED) {
		final_status_ = Status::InvalidArgument("Transaction has already been committed.");
	} else if (txn_state_ == ROLLEDBACK) {
		final_status_ = Status::InvalidArgument("Transaction has already been rolledback.");
	} else {
		final_status_ = Status::InvalidArgument("Transaction is not in state for commit.");
	}
	is_finished.store(true);
}

void AsyncWriteCommittedTxn::ProcessEpiLog(){
	uint64_t seq_used  = seq_used_;
  uint64_t log_used = log_used_;
	//auto start = SPDK_TIME;
	final_status_ = db_impl_->WriteImplAfterLogging(write_options_, GetWriteBatch()->GetWriteBatch(),
		 						/*callback*/ nullptr, /*log_used*/ nullptr,
		 						/*log_ref*/ 0, /*disable_memtable*/ false, &seq_used);
  // final_status_ = db_impl_->BatchEPiLog(write_options_, GetWriteBatch()->GetWriteBatch(),
  //                                       &seq_used, &log_used);
	//db_impl_->TimeStat(SPDK_TIME_DURATION(start, SPDK_TIME, spdk_get_ticks_hz()));
	if (final_status_.ok()) {
		SetId(seq_used);
	}else{
		is_finished.store(true);
		return ;
	}
	if (!name_.empty()) {
		txn_db_impl_->UnregisterTransaction(this);
	}
	Clear();
	if (final_status_.ok()) {
		txn_state_.store(COMMITED);
	}
  finish_time = SPDK_TIME;
	is_finished.store(true);
}

void AsyncWriteCommittedTxn::ProcessRequest(AsyncTxnRequest *req){
  assert(req != nullptr);
  if(req->Type() == RequestScheduler::Read){
    ProcessRead(static_cast<AsyncTxnReadRequest *>(req));
  }else if(req->Type() == RequestScheduler::Scan){
    ProcessScan(static_cast<AsyncTxnScanRequest *>(req));
  }else if(req->Type() == RequestScheduler::IteratorReq){
    ProcessIterator(static_cast<AsyncTxnIteratorRequest *>(req));
  }else if(req->Type() == RequestScheduler::MultiGet){
    ProcessMultiGet(static_cast<AsyncTxnMultiGetRequest *>(req));
  }else{
    assert(0);
  }
}

void AsyncWriteCommittedTxn::ProcessRead(AsyncTxnReadRequest *req){
  assert(req!= nullptr);
  assert(req->value_ != nullptr);
  PinnableSlice pinnable_val(req->value_);
  assert(!pinnable_val.IsPinned());
  auto s = Get(req->read_options_, req->column_family_, req->key_, &pinnable_val);
  assert(req!= nullptr);
  if (s.ok() && pinnable_val.IsPinned()) {
    req->value_->assign(pinnable_val.data(), pinnable_val.size());
  }
  req->status_.store(new Status(s));
}

void AsyncWriteCommittedTxn::ProcessMultiGet(AsyncTxnMultiGetRequest *req){
  size_t num_keys = req->keys_.size();
  req->statuses_->resize(num_keys);
  req->values_->resize(num_keys);
  for (size_t i = 0; i < num_keys; ++i) {
    std::string* value = req->values_ ? &(*(req->values_))[i] : nullptr;
	Status s = Get(req->read_options_, req->column_family_, req->keys_[i], value);
    (*(req->statuses_))[i] = s;
  }
  req->status_.store(new Status((*req->statuses_)[0]));
}

void AsyncWriteCommittedTxn::ProcessScan(AsyncTxnScanRequest *req){
  Iterator* iter = GetIterator(req->read_options_, req->column_family_);
	iter->Seek(req->key_);
	for (int i = 0; i < req->length_ && iter->Valid(); i++) {
    iter->Next();
  }
  req->status_.store(new Status(iter->status()));
  delete iter;
}

void AsyncWriteCommittedTxn::ProcessIterator(AsyncTxnIteratorRequest *req){
  req->iterator_ = GetIterator(req->read_options_, req->column_family_);
  req->status_.store(new Status(req->iterator_->status()));
}

Status AsyncWriteCommittedTxn::PrepareInternal() {
	WriteOptions write_options = write_options_;
	write_options.disableWAL = false;
	WriteBatchInternal::MarkEndPrepare(GetWriteBatch()->GetWriteBatch(), name_);
	class MarkLogCallback : public PreReleaseCallback {
	  public:
		MarkLogCallback(DBImpl* db, bool two_write_queues)
				: db_(db), two_write_queues_(two_write_queues) {
			(void)two_write_queues_;	// to silence unused private field warning
		}
		virtual Status Callback(SequenceNumber, bool is_mem_disabled,
									uint64_t log_number, size_t /*index*/,
									size_t /*total*/) override {
			#ifdef NDEBUG
			(void)is_mem_disabled;
			#endif
			assert(log_number != 0);
			assert(!two_write_queues_ || is_mem_disabled);	// implies the 2nd queue
			db_->logs_with_prep_tracker()->MarkLogAsContainingPrepSection(log_number);
			return Status::OK();
		}

	  private:
		DBImpl* db_;
		bool two_write_queues_;
	} mark_log_callback(db_impl_,db_impl_->immutable_db_options().two_write_queues);

	WriteCallback* const kNoWriteCallback = nullptr;
	const uint64_t kRefNoLog = 0;
	const bool kDisableMemtable = true;
	SequenceNumber* const KIgnoreSeqUsed = nullptr;
	const size_t kNoBatchCount = 0;
	Status s = db_impl_->WriteImpl(
			write_options, GetWriteBatch()->GetWriteBatch(), kNoWriteCallback,
			&log_number_, kRefNoLog, kDisableMemtable, KIgnoreSeqUsed, kNoBatchCount,
			&mark_log_callback);
	return s;
}

Status AsyncWriteCommittedTxn::CommitWithoutPrepareInternal() {
	uint64_t seq_used = kMaxSequenceNumber;
	auto s =
		db_impl_->WriteImpl(write_options_, GetWriteBatch()->GetWriteBatch(),
							/*callback*/ nullptr, /*log_used*/ nullptr,
							/*log_ref*/ 0, /*disable_memtable*/ false, &seq_used);
	assert(!s.ok() || seq_used != kMaxSequenceNumber);
	if (s.ok()) {
		SetId(seq_used);
	}
	return s;
}

Status AsyncWriteCommittedTxn::CommitBatchInternal(WriteBatch* batch, size_t) {
	uint64_t seq_used = kMaxSequenceNumber;
	auto s = db_impl_->WriteImpl(write_options_, batch, /*callback*/ nullptr,
								/*log_used*/ nullptr, /*log_ref*/ 0,
								/*disable_memtable*/ false, &seq_used);
	assert(!s.ok() || seq_used != kMaxSequenceNumber);
	if (s.ok()) {
		SetId(seq_used);
	}
	return s;
}

Status AsyncWriteCommittedTxn::CommitInternal() {
	// We take the commit-time batch and append the Commit marker.
	// The Memtable will ignore the Commit marker in non-recovery mode
	WriteBatch* working_batch = GetCommitTimeWriteBatch();
	WriteBatchInternal::MarkCommit(working_batch, name_);

	// any operations appended to this working_batch will be ignored from WAL
	working_batch->MarkWalTerminationPoint();

	// insert prepared batch into Memtable only skipping WAL.
	// Memtable will ignore BeginPrepare/EndPrepare markers
	// in non recovery mode and simply insert the values
	WriteBatchInternal::Append(working_batch, GetWriteBatch()->GetWriteBatch());

	uint64_t seq_used = kMaxSequenceNumber;
	auto s =
			db_impl_->WriteImpl(write_options_, working_batch, /*callback*/ nullptr,
								/*log_used*/ nullptr, /*log_ref*/ log_number_,
								/*disable_memtable*/ false, &seq_used);	
	assert(!s.ok() || seq_used != kMaxSequenceNumber);
	if (s.ok()) {
		SetId(seq_used);
	}
	return s;
}

Status AsyncWriteCommittedTxn::RollbackInternal() {
	WriteBatch rollback_marker;
	WriteBatchInternal::MarkRollback(&rollback_marker, name_);
	auto s = db_impl_->WriteImpl(write_options_, &rollback_marker);
	return s;
}

Status AsyncWriteCommittedTxn::AsyncCommit() {
	assert(!is_finished.load());
  commit_time = SPDK_TIME;
	AsyncWriteCommittedTxnDB *txn_db = static_cast<AsyncWriteCommittedTxnDB *>(this->txn_db_);
	// printf("async commit\n");
	txn_db->EnqueueCommit(this);
	return Status::OK();
}

Status AsyncWriteCommittedTxn::Commit() {
  is_finished.store(true); // add for async
  return Status::NotSupported("please use AsyncCommit() in AsyncWriteCommittedTxn");

  Status s;
  bool commit_without_prepare = false;
  bool commit_prepared = false;

  if (IsExpired()) {
    return Status::Expired();
  }

  if (expiration_time_ > 0) {
    // we must atomicaly compare and exchange the state here because at
    // this state in the transaction it is possible for another thread
    // to change our state out from under us in the even that we expire and have
    // our locks stolen. In this case the only valid state is STARTED because
    // a state of PREPARED would have a cleared expiration_time_.
    TransactionState expected = STARTED;
    commit_without_prepare = std::atomic_compare_exchange_strong(
        &txn_state_, &expected, AWAITING_COMMIT);
    TEST_SYNC_POINT("TransactionTest::ExpirableTransactionDataRace:1");
  } else if (txn_state_ == PREPARED) {
    // expiration and lock stealing is not a concern
    commit_prepared = true;
  } else if (txn_state_ == STARTED) {
    // expiration and lock stealing is not a concern
    commit_without_prepare = true;
    // TODO(myabandeh): what if the user mistakenly forgets prepare? We should
    // add an option so that the user explictly express the intention of
    // skipping the prepare phase.
  }

  if (commit_without_prepare) {
    assert(!commit_prepared);
    if (WriteBatchInternal::Count(GetCommitTimeWriteBatch()) > 0) {
      s = Status::InvalidArgument(
          "Commit-time batch contains values that will not be committed.");
    } else {
      txn_state_.store(AWAITING_COMMIT);
      if (log_number_ > 0) {
        dbimpl_->logs_with_prep_tracker()->MarkLogAsHavingPrepSectionFlushed(
            log_number_);
      }
      s = CommitWithoutPrepareInternal();
      if (!name_.empty()) {
        txn_db_impl_->UnregisterTransaction(this);
      }
      Clear();
      if (s.ok()) {
        txn_state_.store(COMMITED);
      }
    }
  } else if (commit_prepared) {
    txn_state_.store(AWAITING_COMMIT);

    s = CommitInternal();

    if (!s.ok()) {
      ROCKS_LOG_WARN(db_impl_->immutable_db_options().info_log,
                     "Commit write failed");
      return s;
    }

    // FindObsoleteFiles must now look to the memtables
    // to determine what prep logs must be kept around,
    // not the prep section heap.
    assert(log_number_ > 0);
    dbimpl_->logs_with_prep_tracker()->MarkLogAsHavingPrepSectionFlushed(
        log_number_);
    txn_db_impl_->UnregisterTransaction(this);

    Clear();
    txn_state_.store(COMMITED);
  } else if (txn_state_ == LOCKS_STOLEN) {
    s = Status::Expired();
  } else if (txn_state_ == COMMITED) {
    s = Status::InvalidArgument("Transaction has already been committed.");
  } else if (txn_state_ == ROLLEDBACK) {
    s = Status::InvalidArgument("Transaction has already been rolledback.");
  } else {
    s = Status::InvalidArgument("Transaction is not in state for commit.");
  }

  return s;
}

bool AsyncWriteCommittedTxn::IsFinished(){
	return is_finished.load();;
}

}	// namespace rocksdb
#endif	// ROCKSDB_LITE
