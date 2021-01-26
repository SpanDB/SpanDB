// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

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
#include "utilities/transactions/pessimistic_transaction.h"
#include "utilities/transactions/pessimistic_transaction_db.h"
#include "utilities/transactions/transaction_base.h"
#include "utilities/transactions/transaction_util.h"

namespace rocksdb {

class AsyncWriteCommittedTxn : public PessimisticTransaction {
 public:
  AsyncWriteCommittedTxn(TransactionDB* db, const WriteOptions& write_options,
                   const TransactionOptions& txn_options);

  struct AsyncTxnRequest : RequestScheduler::AsyncRequest{
    Transaction *txn_;
    bool check_;
    AsyncTxnRequest(ColumnFamilyHandle* column_family,
                   const std::string key, std::atomic<Status*> &status,
                   RequestScheduler::ReqType type, Transaction *txn):
      AsyncRequest(column_family, key, status, type),
      txn_(txn),
      check_(true){ }
    void SetCheck(bool check){check_ = check;}
    bool CheckQueue(){return check_;}
    virtual ~AsyncTxnRequest(){}
  };

  struct AsyncTxnReadRequest : AsyncTxnRequest{
    public:
      std::string *value_;
      const ReadOptions& read_options_;
      AsyncTxnReadRequest(const ReadOptions& options,
                        ColumnFamilyHandle* column_family,
                        const std::string key, std::string* value, 
                        std::atomic<Status*>  &status, Transaction *txn):
            AsyncTxnRequest(column_family, key, status, RequestScheduler::Read, txn),
            value_(value),
            read_options_(options){}
  };

  struct AsyncTxnScanRequest : AsyncTxnRequest{
    public:
      std::string *value_;
      const ReadOptions& read_options_;
      const int length_;
      AsyncTxnScanRequest(const ReadOptions& options,
                        ColumnFamilyHandle* column_family,
                        const std::string key, std::string* value, 
                        const int length, std::atomic<Status*> &status, Transaction *txn):
            AsyncTxnRequest(column_family, key, status, RequestScheduler::Scan, txn),
            value_(value),
            read_options_(options),
            length_(length){ }
  };

  struct AsyncTxnIteratorRequest : AsyncTxnRequest{
    public:
      const ReadOptions& read_options_;
      Iterator *&iterator_;
      AsyncTxnIteratorRequest(const ReadOptions& options,
                        ColumnFamilyHandle* column_family,
                        Iterator *&iterator, std::atomic<Status*> &status,
                        Transaction *txn):
            AsyncTxnRequest(column_family,"", status, RequestScheduler::IteratorReq, txn),
            read_options_(options),
            iterator_(iterator){ }
  };

  struct AsyncTxnMultiGetRequest : AsyncTxnRequest{
    public:
      std::string *value_;
      const ReadOptions& read_options_;
      const std::vector<std::string> keys_;
      std::vector<std::string>* values_;
      std::vector<Status>* statuses_;
      AsyncTxnMultiGetRequest(const ReadOptions& options,
                        ColumnFamilyHandle* column_family,
                        const std::vector<std::string> keys,
                        std::vector<std::string>* values,
                        std::vector<Status>* statuses,
                        std::atomic<Status*>  &status, Transaction *txn):
            AsyncTxnRequest(column_family, "", status, RequestScheduler::MultiGet, txn),
            read_options_(options),
            keys_(keys),
            values_(values),
            statuses_(statuses){}
  };

  virtual ~AsyncWriteCommittedTxn() {}

  // No copying allowed
  AsyncWriteCommittedTxn(const AsyncWriteCommittedTxn&) = delete;
  void operator=(const AsyncWriteCommittedTxn&) = delete;

  bool IsFinished() override;
  Status AsyncCommit() override;
  Status Commit() override;

  Status AsyncGet(const ReadOptions& options,
                     ColumnFamilyHandle* column_family, const std::string key,
                     std::string* value, std::atomic<Status *> &status) override;

  Status AsyncGet(const ReadOptions& options, const std::string key,
                     std::string* value, std::atomic<Status *> &status) override;               
 
  Status AsyncScan(const ReadOptions& options, const std::string key,
                     std::string* value, const int length, std::atomic<Status *> &status) override;

  Status AsyncScan(const ReadOptions& options,
                     ColumnFamilyHandle* column_family, const std::string key,
                     std::string* value, const int length, std::atomic<Status *> &status) override;

  Status AsyncMultiGet(
      const ReadOptions& options,
      ColumnFamilyHandle* column_family,
      const std::vector<std::string> keys,
      std::vector<std::string>* values,
      std::vector<Status>* statuses, std::atomic<Status *> &status, bool check_queue) override;

  virtual Status AsyncGetIterator(const ReadOptions& read_options, Iterator *&iterator, std::atomic<Status *> &status) override;

  virtual Status AsyncGetIterator(const ReadOptions& read_options, ColumnFamilyHandle* column_family,
                                Iterator *&iterator, std::atomic<Status *> &status) override;

  uint64_t start_time;
  uint64_t commit_time;
  uint64_t finish_logging;
  uint64_t finish_time;

 protected:
  void ProcessProLog();
  void ProcessEpiLog();
  void ProcessRequest(AsyncTxnRequest *req);
  void ProcessRead(AsyncTxnReadRequest *req);
  void ProcessScan(AsyncTxnScanRequest *req);
  void ProcessIterator(AsyncTxnIteratorRequest *req);
  void ProcessMultiGet(AsyncTxnMultiGetRequest *req);

  void Initialize(const TransactionOptions& txn_options) override;

  private:
    
    friend class AsyncWriteCommittedTxnDB;

    uint64_t seq_used_;
    uint64_t log_used_;

    Status final_status_;

    TransactionDB *txn_db_;

    std::atomic<bool> is_finished;

    Status PrepareInternal() override;

    Status CommitWithoutPrepareInternal() override;

    Status CommitBatchInternal(WriteBatch* batch, size_t batch_cnt) override;

    Status CommitInternal() override;

    Status RollbackInternal() override;

};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE