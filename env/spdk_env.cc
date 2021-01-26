//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "env/spdk_env.h"

namespace rocksdb {

SpdkEnv::SpdkEnv(Env* base_env) : EnvWrapper(base_env) {
  fprintf(stderr,
          "%s:%d: Please use NewSpdkEnv(Env* base_env, std::string pcie_addr, "
          "int open_mod)\n",
          __FILE__, __LINE__);
}

SpdkEnv::SpdkEnv(Env* base_env, std::string pcie_addr, const Options& opt,
                 int open_mod)
    : EnvWrapper(base_env), options_(opt) {
  Init(pcie_addr, options_.ssdlogging_num, options_.l0_queue_num, options_.topfs_cache_size);
  free_list.Put(LO_FILE_START, SPDK_MAX_LPN);
  if(open_mod == 0) { // open an empty rocksdb
    //
  }else if(open_mod == 1){ // open an existing db that produced by rocksdb
    //
  }else if(open_mod == 2){ // open an existing spandb
    printf("read topfs metadata\n");
    ReadMeta(&file_meta_, options_.lo_path);
    for(auto &meta : file_meta_){
      free_list.Remove(meta.second->start_lpn_, meta.second->end_lpn_);
    }
  }else{
    fprintf(stderr, "wrong open_mod\n");
    exit(0);
  }
  // TopsFSTest(this);
}

SpdkEnv::~SpdkEnv() {
  Exit();
}

void SpdkEnv::ResetStat(){
  TopFSResetStat();
}

void SpdkEnv::SpanDBMigration(std::vector<LiveFileMetaData> files_metadata){
  SpanDBMigrationImpl(files_metadata, options_.max_level);
}

// Partial implementation of the Env interface.
Status SpdkEnv::NewSequentialFile(const std::string& fname,
                                  std::unique_ptr<SequentialFile>* result,
                                  const EnvOptions& /*soptions*/) {
  printf("%s not implemented in SPDK env\n", __FUNCTION__);
  return Status::OK();
}

Status SpdkEnv::NewRandomAccessFile(const std::string& fname,
                                    std::unique_ptr<RandomAccessFile>* result,
                                    const EnvOptions& /*soptions*/) {
  auto fn = NormalizePath(fname);
  {
    // 1. search in FileSystem
    MutexLock lock(&fs_mutex_);
    if (file_system_.find(fn) != file_system_.end()) {
      // printf("NewRandomAccessFile %s from existing files\n", fname.c_str());
      result->reset(new SpdkRandomAccessFile(file_system_[fn]));
      return Status::OK();
    }
  }
  SPDKFileMeta* metadata = nullptr;
  {
    // 2. search in metadata
    MutexLock lock(&meta_mutex_);
    auto meta = file_meta_.find(fn);
    if (meta == file_meta_.end()) {
      *result = nullptr;
      return Status::IOError(fn, "SPDK-env: file not found");
    }
    metadata = meta->second;
  }
  // printf("NewRandomAccessFile %s from device\n", fname.c_str());
  assert(metadata != nullptr);
  SpdkFile* file = new SpdkFile(this, fn, metadata, metadata->size_);
  result->reset(new SpdkRandomAccessFile(file));
  {
    MutexLock lock(&fs_mutex_);
    file_system_[fn] = file;
  }
  return Status::OK();
}

Status SpdkEnv::NewRandomRWFile(const std::string& fname,
                                std::unique_ptr<RandomRWFile>* result,
                                const EnvOptions& /*soptions*/) {
  printf("%s not implemented in SPDK env\n", __FUNCTION__);
  return Status::OK();
}

Status SpdkEnv::ReuseWritableFile(const std::string& fname,
                                  const std::string& old_fname,
                                  std::unique_ptr<WritableFile>* result,
                                  const EnvOptions& options) {
  auto s = RenameFile(old_fname, fname);
  if (!s.ok()) {
    return s;
  }
  result->reset();
  return NewWritableFile(NormalizePath(fname), result, options);
}

Status SpdkEnv::NewWritableFile(const std::string& fname,
                                std::unique_ptr<WritableFile>* result,
                                const EnvOptions& env_options) {
  return NewWritableFile(fname, result, env_options, 0);
}

Status SpdkEnv::NewWritableFile(const std::string& fname,
                                std::unique_ptr<WritableFile>* result,
                                const EnvOptions& env_options,
                                uint64_t pre_allocate_size) {
  auto fn = NormalizePath(fname);
  bool is_flush = false;
  if (pre_allocate_size == 0) {
    pre_allocate_size = env_options.allocate_size;
    assert(pre_allocate_size != 0);
    is_flush = true;
  }
  assert(pre_allocate_size > 0);
  pre_allocate_size = pre_allocate_size + pre_allocate_size / 10;
  pre_allocate_size =
      (pre_allocate_size / FILE_BUFFER_ENTRY_SIZE + 1) * FILE_BUFFER_ENTRY_SIZE;
  uint64_t start_lpn = 0, end_lpn = 0;
  {
    uint64_t page_num = pre_allocate_size / SPDK_PAGE_SIZE;
#ifdef SPANDB_STAT
    auto start = SPDK_TIME;
#endif
    start_lpn = free_list.Get(page_num);
#ifdef SPANDB_STAT
    free_list_latency_.add(
        SPDK_TIME_DURATION(start, SPDK_TIME, spdk_tsc_rate_));
#endif
    if (start_lpn == 0) {
      return Status::IOError("Get free start_lpn failed");
    }
    end_lpn = start_lpn + page_num - 1;
    if (start_lpn > end_lpn) {
      printf("fname: %s, allocate_size: %ld, start_lpn %ld < end_lpn %ld\n",
             fname.c_str(), pre_allocate_size, start_lpn, end_lpn);
    }
  }
  SPDKFileMeta* meta = new SPDKFileMeta(fn, start_lpn, end_lpn, false);
  SpdkFile* file = new SpdkFile(this, fn, meta, pre_allocate_size, is_flush);
  // check overlap
  Status s = check_overlap(start_lpn, end_lpn, fn);
  {
    MutexLock lock(&meta_mutex_);
    if (!s.ok()) {
      return s;
    }
    file_meta_[fn] = meta;
  }
  {
    MutexLock lock(&fs_mutex_);
    file_system_[fn] = file;
  }
  result->reset(new SpdkWritableFile(file, env_options.rate_limiter));
  // WriteMeta(&file_meta_);
  return Status::OK();
}

Status SpdkEnv::NewDirectory(const std::string& /*name*/,
                             std::unique_ptr<Directory>* result) {
  printf("%s not implemented in SPDK env\n", __FUNCTION__);
  return Status::OK();
}

Status SpdkEnv::FileExists(const std::string& fname) {
  auto fn = NormalizePath(fname);
  MutexLock lock(&meta_mutex_);
  if (file_meta_.find(fn) != file_meta_.end()) {
    return Status::OK();
  }
  return Status::NotFound();
}

Status SpdkEnv::GetChildren(const std::string& dir,
                            std::vector<std::string>* result) {
  printf("%s not implemented in SPDK env\n", __FUNCTION__);
  return Status::OK();
}

Status SpdkEnv::DeleteFileInternal(const std::string& fname) {
  assert(fname == NormalizePath(fname));
  {
    // 1. delete from file system
    const auto& fs_pair = file_system_.find(fname);
    if (fs_pair != file_system_.end()) {
      // will require fs_mutex_
      assert(fs_pair->second->GetRef() == 1);
      fs_pair->second->Unref();  // will be removed from file_system by unref()
    }
  }
  uint64_t start_lpn = 0;
  uint64_t end_lpn = 0;
  {
    // 2. delete from file meta
    MutexLock lock(&meta_mutex_);
    const auto& meta_pair = file_meta_.find(fname);
    if (meta_pair != file_meta_.end()) {
      start_lpn = meta_pair->second->start_lpn_;
      end_lpn = meta_pair->second->end_lpn_;
      file_meta_.erase(fname);
    } else {
      return Status::IOError(fname, "SPDK-env: file not found");
    }
  }
#ifdef SPANDB_STAT
  auto start = SPDK_TIME;
#endif
  free_list.Put(start_lpn, end_lpn);
#ifdef SPANDB_STAT
  free_list_latency_.add(SPDK_TIME_DURATION(start, SPDK_TIME, spdk_tsc_rate_));
#endif
  RemoveFromLRUCache(start_lpn, end_lpn);
  return Status::OK();
}

Status SpdkEnv::DeleteFile(const std::string& fname) {
  auto fn = NormalizePath(fname);
  return DeleteFileInternal(fn);
}

Status SpdkEnv::Truncate(const std::string& fname, size_t size) {
  printf("%s not implemented in SPDK env\n", __FUNCTION__);
  return Status::OK();
}

Status SpdkEnv::CreateDir(const std::string& dirname) {
  printf("%s not implemented in SPDK env\n", __FUNCTION__);
  return Status::OK();
}

Status SpdkEnv::CreateDirIfMissing(const std::string& dirname) {
  printf("%s not implemented in SPDK env\n", __FUNCTION__);
  CreateDir(dirname);
  return Status::OK();
}

Status SpdkEnv::DeleteDir(const std::string& dirname) {
  return DeleteFile(dirname);
}

Status SpdkEnv::GetFileSize(const std::string& fname, uint64_t* file_size) {
  auto fn = NormalizePath(fname);
  MutexLock lock(&meta_mutex_);
  auto iter = file_meta_.find(fn);
  if (iter == file_meta_.end()) {
    return Status::IOError(fn, "SPDK-env: file not found");
  }
  *file_size = iter->second->Size();
  return Status::OK();
}

Status SpdkEnv::GetFileModificationTime(const std::string& fname,
                                        uint64_t* time) {
  auto fn = NormalizePath(fname);
  MutexLock lock(&meta_mutex_);
  auto iter = file_meta_.find(fn);
  if (iter == file_meta_.end()) {
    return Status::IOError(fn, "SPDK-env: file not found");
  }
  *time = iter->second->ModifiedTime();
  return Status::OK();
}

Status SpdkEnv::RenameFile(const std::string& src, const std::string& dest) {
  auto s = NormalizePath(src);
  auto t = NormalizePath(dest);
  {
    // 1. rename in file meta
    MutexLock lock(&meta_mutex_);
    if (file_meta_.find(s) == file_meta_.end()) {
      return Status::IOError(s, "SPDK-env: file not found");
    }
    file_meta_[t] = file_meta_[s];
    file_meta_.erase(s);
  }
  {
    // 2. rename in file system
    MutexLock lock(&fs_mutex_);
    if (file_system_.find(s) == file_system_.end()) {
      return Status::OK();
    }
    file_system_[t] = file_system_[s];
    file_system_.erase(s);
    return Status::OK();
  }
}

Status SpdkEnv::LinkFile(const std::string& src, const std::string& dest) {
  printf("%s not implemented in SPDK env\n", __FUNCTION__);
  return Status::OK();
}

Status SpdkEnv::NewLogger(const std::string& fname,
                          std::shared_ptr<Logger>* result) {
  printf("%s not implemented in SPDK env\n", __FUNCTION__);
  return Status::OK();
}

Status SpdkEnv::LockFile(const std::string& fname, FileLock** flock) {
  printf("%s not implemented in SPDK env\n", __FUNCTION__);
  return Status::OK();
}

Status SpdkEnv::UnlockFile(FileLock* flock) {
  printf("%s not implemented in SPDK env\n", __FUNCTION__);
  return Status::OK();
}

Status SpdkEnv::GetTestDirectory(std::string* path) {
  *path = "/test";
  return Status::OK();
}

Status SpdkEnv::CorruptBuffer(const std::string& fname) {
  printf("%s not implemented in SPDK env\n", __FUNCTION__);
  return Status::OK();
}

std::string SpdkEnv::NormalizePath(const std::string path) {
  std::string dst;
  for (auto c : path) {
    if (!dst.empty() && c == '/' && dst.back() == '/') {
      continue;
    }
    dst.push_back(c);
  }
  return dst;
}

#ifndef ROCKSDB_LITE
// This is to maintain the behavior before swithcing from InSpdkEnv to SpdkEnv
Env* NewSpdkEnv(Env* base_env) { return new SpdkEnv(base_env); }
Env* NewSpdkEnv(Env* base_env, std::string pcie_addr, const Options& opt,
                int open_mod) {
  return new SpdkEnv(base_env, pcie_addr, opt, open_mod);
}

#else  // ROCKSDB_LITE

Env* NewSpdkEnv(Env* /*base_env*/) { return nullptr; }

#endif  // !ROCKSDB_LITE

}  // namespace rocksdb
