//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <atomic>
#include <map>
#include <string>
#include <vector>
#include <iostream>
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "port/port.h"
#include "util/mutexlock.h"
#include "env/io_spdk.h"

namespace rocksdb {

class SpdkEnv : public EnvWrapper {
 public:
  explicit SpdkEnv(Env* base_env);

  explicit SpdkEnv(Env* base_env, std::string pcie_addr, const Options &opt, int open_mod);

  virtual ~SpdkEnv();

  virtual void SpanDBMigration(std::vector<LiveFileMetaData> files_metadata) override;

  // Partial implementation of the Env interface.
  virtual Status NewSequentialFile(const std::string& fname,
                                   std::unique_ptr<SequentialFile>* result,
                                   const EnvOptions& soptions) override;

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     std::unique_ptr<RandomAccessFile>* result,
                                     const EnvOptions& soptions) override;

  virtual Status NewRandomRWFile(const std::string& fname,
                                 std::unique_ptr<RandomRWFile>* result,
                                 const EnvOptions& options) override;

  virtual Status ReuseWritableFile(const std::string& fname,
                                   const std::string& old_fname,
                                   std::unique_ptr<WritableFile>* result,
                                   const EnvOptions& options) override;

  virtual Status NewWritableFile(const std::string& fname,
                                 std::unique_ptr<WritableFile>* result,
                                 const EnvOptions& env_options) override;

  virtual Status NewWritableFile(const std::string& fname,
                                 std::unique_ptr<WritableFile>* result,
                                 const EnvOptions& options, uint64_t pre_allocate_size) override;

  virtual Status NewDirectory(const std::string& name,
                              std::unique_ptr<Directory>* result) override;

  virtual Status FileExists(const std::string& fname) override;

  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) override;

  Status DeleteFileInternal(const std::string& fname);

  virtual Status DeleteFile(const std::string& fname) override;

  virtual Status Truncate(const std::string& fname, size_t size) override;

  virtual Status CreateDir(const std::string& dirname) override;

  virtual Status CreateDirIfMissing(const std::string& dirname) override;

  virtual Status DeleteDir(const std::string& dirname) override;

  virtual Status GetFileSize(const std::string& fname,
                             uint64_t* file_size) override;

  virtual Status GetFileModificationTime(const std::string& fname,
                                         uint64_t* time) override;

  virtual Status RenameFile(const std::string& src,
                            const std::string& target) override;

  virtual Status LinkFile(const std::string& src,
                          const std::string& target) override;

  virtual Status NewLogger(const std::string& fname,
                           std::shared_ptr<Logger>* result) override;

  virtual Status LockFile(const std::string& fname, FileLock** flock) override;

  virtual Status UnlockFile(FileLock* flock) override;

  virtual Status GetTestDirectory(std::string* path) override;

  Status CorruptBuffer(const std::string& fname);

  void ResetStat() override;

  // Doesn't really sleep, just affects output of GetCurrentTime(), NowMicros()
  // and NowNanos()

 private:
  std::string NormalizePath(const std::string path);

  uint64_t lo_current_lpn_;
  port::Mutex mutex_;
  const Options options_;
};

}  // namespace rocksdb
