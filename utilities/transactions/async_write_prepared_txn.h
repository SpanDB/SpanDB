// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE

#include "utilities/transactions/write_prepared_txn.h"

namespace rocksdb {

class AsyncWritePreparedTxnDB;

// This impl could write to DB also uncommitted data and then later tell apart
// committed data from uncommitted data. Uncommitted data could be after the
// Prepare phase in 2PC (AsyncWritePreparedTxn) or before that
// (WriteUnpreparedTxnImpl).
class AsyncWritePreparedTxn : public PessimisticTransaction {
 public:
  AsyncWritePreparedTxn(AsyncWritePreparedTxnDB* db, const WriteOptions& write_options,
                   const TransactionOptions& txn_options);
  // No copying allowed
  AsyncWritePreparedTxn(const AsyncWritePreparedTxn&) = delete;
  void operator=(const AsyncWritePreparedTxn&) = delete;

  virtual ~AsyncWritePreparedTxn() {}

  // add for async
  Status AsyncCommit() override;
  Status AsyncGetV2(const ReadOptions& options,
                     ColumnFamilyHandle* column_family, const std::string key,
                     std::string* value, bool *finished) override;

  Status AsyncGetV2(const ReadOptions& options, const std::string key,
                     std::string* value, bool *finished) override;

  Status Commit() override;
  bool IsFinished() override;
  
  struct AsyncReadRequest{
    AsyncWritePreparedTxn *txn_;
    bool *finished_;
    const ReadOptions &options_;
    ColumnFamilyHandle* column_family_;
    const std::string key_;
    std::string *value_;
    AsyncReadRequest(AsyncWritePreparedTxn *txn, bool *finished, 
                const ReadOptions &options, ColumnFamilyHandle* column_family, 
                const std::string key, std::string *value)
                :txn_(txn),finished_(finished), options_(options),
                column_family_(column_family),key_(key),
                value_(value){
                  *finished_ = false;
                }
  };

  private:

  std::atomic<bool> is_finished;   // add for async

  // To make WAL commit markers visible, the snapshot will be based on the last
  // seq in the WAL that is also published, LastPublishedSequence, as opposed to
  // the last seq in the memtable.
  using Transaction::Get;
  virtual Status Get(const ReadOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     PinnableSlice* value) override;

  using Transaction::MultiGet;
  virtual void MultiGet(const ReadOptions& options,
                        ColumnFamilyHandle* column_family,
                        const size_t num_keys, const Slice* keys,
                        PinnableSlice* values, Status* statuses,
                        bool sorted_input = false) override;

  // Note: The behavior is undefined in presence of interleaved writes to the
  // same transaction.
  // To make WAL commit markers visible, the snapshot will be
  // based on the last seq in the WAL that is also published,
  // LastPublishedSequence, as opposed to the last seq in the memtable.
  using Transaction::GetIterator;
  virtual Iterator* GetIterator(const ReadOptions& options) override;
  virtual Iterator* GetIterator(const ReadOptions& options,
                                ColumnFamilyHandle* column_family) override;

  virtual void SetSnapshot() override;

 protected:
  void Initialize(const TransactionOptions& txn_options) override;
  // Override the protected SetId to make it visible to the friend class
  // AsyncWritePreparedTxnDB
  inline void SetId(uint64_t id) override { Transaction::SetId(id); }

 private:
  friend class WritePreparedTransactionTest_BasicRecoveryTest_Test;
  friend class AsyncWritePreparedTxnDB;
  friend class WriteUnpreparedTxnDB;
  friend class WriteUnpreparedTxn;

  Status ProcessBeforeLogging();   //add for async
  Status ProcessAfterLogging();   //add for async
  uint64_t seq_used_;    //add for async

  Status PrepareInternal() override;

  Status CommitWithoutPrepareInternal() override;

  Status CommitBatchInternal(WriteBatch* batch, size_t batch_cnt) override;

  // Since the data is already written to memtables at the Prepare phase, the
  // commit entails writing only a commit marker in the WAL. The sequence number
  // of the commit marker is then the commit timestamp of the transaction. To
  // make WAL commit markers visible, the snapshot will be based on the last seq
  // in the WAL that is also published, LastPublishedSequence, as opposed to the
  // last seq in the memtable.
  Status CommitInternal() override;

  Status RollbackInternal() override;

  virtual Status ValidateSnapshot(ColumnFamilyHandle* column_family,
                                  const Slice& key,
                                  SequenceNumber* tracked_at_seq) override;

  virtual Status RebuildFromWriteBatch(WriteBatch* src_batch) override;

  AsyncWritePreparedTxnDB* wpt_db_;
  // Number of sub-batches in prepare
  size_t prepare_batch_cnt_ = 0;
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
