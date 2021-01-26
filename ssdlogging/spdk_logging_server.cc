#include "spdk_logging_server.h"

#include "utilities/transactions/async_write_committed_txn.h"
#include "db/db_impl/request_scheduler.h"
#include "typeinfo"

namespace ssdlogging {

void SPDKLoggingServer::AddLog(const char *data, int size, void *ptr) {
  assert(data != nullptr && ptr != nullptr);
  __sync_add_and_fetch(&log_size_, size);
  alive_log_files_.back().Add(size);
  LoggingRequest *req = new LoggingRequest(data, size, ptr);
  LoggingRequest *newest_req = submission_queue.load(std::memory_order_relaxed);
  while (true) {
    req->prev = newest_req;
    if (submission_queue.compare_exchange_weak(newest_req, req)) {
      break;
    }
  }
  __sync_fetch_and_add(&total_log_written, size);
}

void SPDKLoggingServer::AddSize(size_t size) {
  __sync_add_and_fetch(&log_size_, size);
  alive_log_files_.back().Add(size);
}

inline uint64_t SPDKLoggingServer::MemCopy(LoggingRequest *leader,
                                           int queue_id) {
  char *buf = buffer[queue_id];
  uint64_t offset = 0;
  uint32_t left_in_page = PAGESIZE;
  LoggingRequest *head = leader;
#ifdef SPANDB_STAT
  uint64_t batchsize = 0;
#endif
  while (head != nullptr) {
#ifdef SPANDB_STAT
    wait_time.add(SPDK_TIME_DURATION(head->start_time, SPDK_TIME, spdk_tsc_rate_));
#endif
    const char *data = head->data_;
    uint32_t size = head->size_;
    assert(offset + size + 4 <
           BUFFER_SIZE - 4 * (BUFFER_SIZE / PAGESIZE));
    // "size" is not allowed to be stored on two pages
    if (left_in_page < 4) {
      memset(buf + offset, 0, left_in_page);
      offset += left_in_page;
      left_in_page = PAGESIZE;
    }
    // begin of a page
    if (left_in_page == PAGESIZE) {
      EncodeFixed32(buf + offset, logfile_seq_);
      offset += 4;
      left_in_page -= 4;
    }
    // write "size" to buffer
    EncodeFixed32(buf + offset, size);
    offset += 4;
    left_in_page -= 4;
    // write data to buffer
    while (size > 0) {
      if (left_in_page == PAGESIZE) {  // begin of a page
        EncodeFixed32(buf + offset, logfile_seq_);
        offset += 4;
        left_in_page -= 4;
      }
      if (size <= left_in_page) {
        memcpy(buf + offset, data + (head->size_ - size), size);
        offset += size;
        left_in_page -= size;
        size = 0;
      } else {
        memcpy(buf + offset, data + (head->size_ - size), left_in_page);
        size -= left_in_page;
        offset += left_in_page;
        left_in_page = PAGESIZE;
        // data = data + left_in_page;
      }
    }
    head = head->prev;
#ifdef SPANDB_STAT
    batchsize++;
#endif
  }
  if (left_in_page > 0 && left_in_page < PAGESIZE) {
    memset(buf + offset, 0, left_in_page);
    offset += left_in_page;
  }
#ifdef SPANDB_STAT
  batch_size.add(batchsize);
#endif
  assert(offset % PAGESIZE == 0);
  return offset;
}

inline void SPDKLoggingServer::SPDKWrite(struct NSEntry *ns_entry,
                                         uint64_t size, int queue_id,
                                         bool *flag) {
  assert(size % PAGESIZE == 0);
  uint64_t sector_num = size / ns_entry->sector_size;
  uint64_t page_num = size / PAGESIZE;
  uint64_t lpn = __sync_fetch_and_add(&current_offset, page_num);
  uint64_t lsn = lpn * (PAGESIZE / ns_entry->sector_size);

#ifdef SPANDB_STAT
  request_size.add(page_num);
  if (page_num < 9) {
    __sync_fetch_and_add(&(req_sizes[page_num]), 1);
  } else {
    __sync_fetch_and_add(&(req_sizes[9]), 1);
  }
  queue_length_.fetch_add(1);
  spdk_queue_length_.insert(SPDK_TIME, queue_length_.load());
#endif

  if (UNLIKELY(current_offset > MAX_LPN_FOR_LOGGING))
    current_offset = LOGGING_START_LPN;

  assert(sector_num > 0);
  assert(*flag);
  *flag = false;
  int rc = spdk_nvme_ns_cmd_write(
      ns_entry->ns, ns_entry->qpair[queue_id], buffer[queue_id], lsn,
      sector_num,
      [](void *arg, const struct spdk_nvme_cpl *completion) -> void {
        bool *finished = (bool *)arg;
        if (UNLIKELY(spdk_nvme_cpl_is_error(completion))) {
          fprintf(stderr, "I/O error status: %s\n",
                  spdk_nvme_cpl_get_status_string(&completion->status));
          fprintf(stderr, "Write I/O failed, aborting run\n");
          *finished = true;
          exit(1);
        }
        *finished = true;
      },
      flag, 0);
  if (UNLIKELY(rc != 0)) {
    printf("spdk write failed: %d\n", rc * -1);
    assert(0);
  }
}

void SPDKLoggingServer::Server(int coreid, int serverid, int pipeline_num) {
  // SetAffinity("Logging server",coreid);
  bool *completion_flag = new bool[pipeline_num];
  LoggingRequest **leaders = new LoggingRequest *[pipeline_num];
  uint64_t *senttime = new uint64_t[pipeline_num];
  for (int i = 0; i < pipeline_num; i++) {
    completion_flag[i] = true;
    leaders[i] = nullptr;
    senttime[i] = 0;
  }
  struct NSEntry *ns_entry = spdk->namespaces;

  while (true) {
    if (UNLIKELY(stop_.load())) break;
    // 1. select a leader
    int log_num = -1;
    for (int i = 0; i < pipeline_num; i++) {
      if (leaders[i] == nullptr) {
        log_num = i;
        break;
      }
    }
    // 2. send a requests
    if (log_num != -1) {
      LoggingRequest *leader =
          submission_queue.exchange(nullptr, std::memory_order_acquire);
      if (leader != nullptr) {
        last_pickup_time = SPDK_TIME;
#ifdef SPANDB_STAT
        if (UNLIKELY(server_start_time_.load() == 0)) {
          server_start_time_.store(SPDK_TIME);
#ifdef PRINT_BANDWIDTH
          last_print_time_.store(SPDK_TIME);
#endif
        }
#endif
        // temp_time.add(SPDK_TIME_DURATION(leader->start_time, SPDK_TIME,
        // spdk_tsc_rate_));
        leaders[log_num] = leader;
        senttime[log_num] = SPDK_TIME;
        uint64_t size = MemCopy(leader, log_num + serverid * pipeline_num);
        SPDKWrite(ns_entry, size, log_num + serverid * pipeline_num,
                  &(completion_flag[log_num]));
        assert(!completion_flag[log_num]);
      }
    }
    // 3. check completion
    for (int i = 0; i < pipeline_num; i++) {
      if (leaders[i] != nullptr) {  // we sent one request before
        spdk_nvme_qpair_process_completions(
            ns_entry->qpair[i + serverid * pipeline_num], 0);  // spdk check
        if (completion_flag[i]) {  // finish one request
#ifdef SPANDB_STAT
          auto time_now = SPDK_TIME;
          sync_time.add(
              SPDK_TIME_DURATION(senttime[i], time_now, spdk_tsc_rate_));
          // queue_length_.fetch_sub(1);
          // spdk_queue_length_.insert(SPDK_TIME, queue_length_.load());
          // spdk_logging_time_dis_.insert(SPDK_TIME,
          // SPDK_TIME_DURATION(senttime[i], time_now, spdk_tsc_rate_));
#endif
          LoggingRequest *head = leaders[i];
          leaders[i] = nullptr;
          LoggingRequest *next;
          uint64_t batchsize = 0;
          while (head != nullptr) {
            batchsize += head->size_;
            next = head->prev;
            head->prev = nullptr;
#ifdef SPANDB_STAT
            spdklogging_time.add(SPDK_TIME_DURATION(head->start_time, SPDK_TIME,
                                                    spdk_tsc_rate_));
            sync_time_per_request.insert(
                SPDK_TIME_DURATION(senttime[i], time_now, spdk_tsc_rate_));
            // sync_time_dis.insert(SPDK_TIME_DURATION(senttime[i], time_now,
            // spdk_tsc_rate_)); (static_cast<rocksdb::AsyncWriteCommittedTxn
            //*>(head->ptr_))->finish_logging = SPDK_TIME;
            (static_cast<rocksdb::RequestScheduler::AsyncWriteRequest *>(
                 head->ptr_))
                ->finish_logging = SPDK_TIME;
            /*rocksdb::RequestScheduler::AsyncRequest *tmp =
            static_cast<rocksdb::RequestScheduler::AsyncRequest *>(head->ptr_);
            auto t = SPDK_TIME;
            while(tmp != nullptr){
              sync_time_per_request.insert(SPDK_TIME_DURATION(tmp->start_time,
            SPDK_TIME, spdk_tsc_rate_)); tmp = tmp->prev;
            }*/
#endif
            completion_queue->WriteInBuf(head);
            head = next;
          }
          rocksdb::spandb_controller_.IncreaseLDWritten(batchsize);
#ifdef SPANDB_STAT
          sync_time_dis.insert(
              SPDK_TIME_DURATION(senttime[i], time_now, spdk_tsc_rate_));
          batch_size_dis.insert(batchsize);
          server_stop_time_.store(SPDK_TIME);
#endif
        }
      }
    } /* check completion */
    if (SPDK_TIME_DURATION(last_pickup_time, SPDK_TIME, spdk_tsc_rate_) > 30000000) {
      break;
    }
  }
}

void *SPDKLoggingServer::GetCompletedRequest() {
  const int64_t seq = completion_queue->GetReadableSeq();
  if (UNLIKELY(seq < 0)) return nullptr;
  LoggingRequest *req = completion_queue->ReadFromBuf(seq);
  completion_queue->FinishReading(seq);
  void *ptr = req->ptr_;
  delete req;
  return ptr;
}

void *SPDKLoggingServer::AsyncGetCompletedRequest(int64_t &seq) {
  if (seq == -1) {
    seq = completion_queue->AsyncGetNextReadableSeq();
    if (seq < 0) return nullptr;
  }
  LoggingRequest *req = completion_queue->AsyncRead(seq);
  if (req == nullptr) return nullptr;
  void *ptr = req->ptr_;
  free(req);
  return ptr;
}

void *SPDKLoggingServer::GetCompletedRequestWithLock() {
  LoggingRequest *req = completion_queue->ReadWithLock();
  if (req == nullptr) return nullptr;
  void *ptr = req->ptr_;
  free(req);
  return ptr;
}

bool SPDKLoggingServer::AsyncFinishReading(int64_t seq) {
  return completion_queue->AsyncFinishReading(seq);
}

int SPDKLoggingServer::GetCompleteQueueLen() {
  return completion_queue->Length();
}

void SPDKLoggingServer::Stop() {
  if (stop_.load()) return;
  stop_.store(true);
  completion_queue->stop();
}

void SPDKLoggingServer::DeleteLog(uint32_t min_log_number, uint32_t size) {
  // should consider logs_ when recovery;
  if (!alive_log_files_.empty()) {
    while (alive_log_files_.begin()->seq < min_log_number + base_log_tag_seq_) {
      auto &earliest = *alive_log_files_.begin();
      __sync_fetch_and_sub(&log_size_, earliest.Size());
      alive_log_files_.pop_front();
      assert(alive_log_files_.size());
    }
  }
  assert(size == alive_log_files_.size());
  WriteMetadata();
}

uint32_t SPDKLoggingServer::GetCurrentLogSeq() {
  return logfile_seq_ - base_log_tag_seq_;
}

uint64_t SPDKLoggingServer::GetLogSize() { return log_size_; }

void SPDKLoggingServer::CreateWAL(uint32_t log_file_num) {
  if (LIKELY(alive_log_files_.size() != 0)) {
    assert(alive_log_files_.back().seq == logfile_seq_);
    assert(current_offset > 1);
    // assert(current_offset > alive_log_files_.back().start_lpn);
    alive_log_files_.back().end_lpn = current_offset - 1;
  }
  logfile_seq_ = log_file_num + base_log_tag_seq_;
  alive_log_files_.emplace_back(logfile_seq_, current_offset);
  WriteMetadata();
}

void SPDKLoggingServer::WriteMetadata() {
  uint32_t offset = 0;
  EncodeFixed32(metadata + offset, base_log_tag_seq_);
  offset += 4;
  uint32_t size = alive_log_files_.size();
  EncodeFixed32(metadata + offset, size);
  offset += 4;
  for (auto &log : alive_log_files_) {
    EncodeFixed32(metadata + offset, log.seq);
    offset += 4;
    EncodeFixed64(metadata + offset, log.start_lpn);
    offset += 8;
    EncodeFixed64(metadata + offset, log.end_lpn);
    offset += 8;
  }
  // write page 0
  bool flag = false;
  struct NSEntry *ns_entry = spdk->namespaces;
  int rc = spdk_nvme_ns_cmd_write(
      ns_entry->ns, ns_entry->qpair[logs_num_], metadata, 0,
      PAGESIZE / ns_entry->sector_size,
      [](void *arg, const struct spdk_nvme_cpl *completion) -> void {
        bool *finished = (bool *)arg;
        if (UNLIKELY(spdk_nvme_cpl_is_error(completion))) {
          fprintf(stderr, "I/O error status: %s\n",
                  spdk_nvme_cpl_get_status_string(&completion->status));
          fprintf(stderr, "Write I/O failed, aborting run\n");
          *finished = true;
          exit(1);
        }
        *finished = true;
      },
      &flag, 0);
  if (UNLIKELY(rc != 0)) {
    printf("write metadata failed: %d\n", rc * -1);
    assert(0);
  }
  while (!flag) {
    spdk_nvme_qpair_process_completions(ns_entry->qpair[logs_num_], 0);
  }
}

void SPDKLoggingServer::SetLastLogfileSeq() {
  // read
  struct NSEntry *ns_entry = spdk->namespaces;
  bool flag = false;
  int rc = spdk_nvme_ns_cmd_read(
      ns_entry->ns, ns_entry->qpair[0], metadata, 0,
      PAGESIZE / ns_entry->sector_size,
      [](void *arg, const struct spdk_nvme_cpl *completion) -> void {
        bool *finished = (bool *)arg;
        if (spdk_nvme_cpl_is_error(completion)) {
          fprintf(stderr, "I/O error status: %s\n",
                  spdk_nvme_cpl_get_status_string(&completion->status));
          fprintf(stderr, "Write I/O failed, aborting run\n");
          *finished = true;
          exit(1);
        }
        *finished = true;
      },
      &flag, 0);
  if (UNLIKELY(rc != 0)) {
    printf("spdk write failed: %d\n", rc * -1);
    assert(0);
  }
  while (!flag) {
    spdk_nvme_qpair_process_completions(ns_entry->qpair[0], 0);
  }
  // parse
  base_log_tag_seq_ = 0;
  uint32_t offset = 4;  // the first 4 bytes is the last base_log_tag_seq_
  uint32_t num = DecodeFixed32(metadata + offset);
  offset += 4;
  for (uint32_t i = 0; i < num; i++) {
    uint32_t seq = DecodeFixed32(metadata + offset);
    offset += 4;  // seq
    offset += 8;  // start_lpn
    offset += 8;  // end_lpn
    base_log_tag_seq_ = seq;
  }
  base_log_tag_seq_++;
  printf("Log tag seq starts from: %u\n", base_log_tag_seq_);
}

void SPDKLoggingServer::PrintMetadata() {
  printf("---------log files-------\n");
  for (auto &log : alive_log_files_) {
    printf("%d %ld %ld %ld\n", log.seq, log.start_lpn, log.end_lpn,
           log.current_lpn);
  }
  printf("---------log files-------\n");
}

void SPDKLoggingServer::ResetStats() {
#ifdef SPANDB_STAT
  spdklogging_time.reset();
  sync_time.reset();
  wait_time.reset();
  temp_time.reset();
  // request_size.reset();
  batch_size.reset();
  sync_time_dis.reset();
  batch_size_dis.reset();
  sync_time_per_request.reset();
  for (int i = 0; i < 10; i++) {
    req_sizes[i] = 0;
  }
  server_start_time_.store(0);
  spdk_queue_length_.reset();
  spdk_logging_time_dis_.reset();
#endif
}

void SPDKLoggingServer::PrintStatistics() {
#ifdef SPANDB_STAT
    printf("-----------SSDLogging Performance-------------\n");
    double time = SPDK_TIME_DURATION(server_start_time_.load(),
                                     server_stop_time_.load(), spdk_tsc_rate_);
    printf("Logging IOPS: %.2lf K\n",
           sync_time.size() / (time * 1.0 / 1000000.0) / 1000.0);
    printf("Logging bandwidth: %.2lf MB/s\n",
           (request_size.sum() * PAGESIZE / 1024.0 / 1024.0) /
               (time / 1000.0 / 1000.0));
    spdklogging_time.print_data("spdk logging time");
    sync_time.print_data("sync time");
    // sync_time_per_request.print_data("sync_per_request");
    wait_time.print_data("logging wait time");
    temp_time.print_data("tmp time");
    // sync_time_dis.print_data("sync_time.txt");
    // batch_size_dis.print_data("batch_size.txt");
    // spdk_queue_length_.print_data("logging_queue_length.txt");
    // spdk_logging_time_dis_.print_data("spdk_logging_time.txt");
    printf("Avg write size: %.2lf KB\n", request_size.avg() * PAGESIZE / 1024.0);
    printf("Total written: %.2lf GB\n",
           request_size.sum() * PAGESIZE / 1024.0 / 1024.0 / 1024.0);
    printf("Logging batch size: %.2lf\n", batch_size.avg());
    uint64_t total_write = std::accumulate(req_sizes, req_sizes + 10, 0);
    std::string s1 = "      num: ";
    std::string s2 = "     rate: ";
    for (int i = 1; i <= 9; i++) {
      s1 += std::to_string(req_sizes[i]) + "  ";
      s2 += std::to_string(req_sizes[i] * 1.0 / total_write) + "  ";
    }
    printf("write size: 4KB 8KB 12KB 16KB 20KB 24KB 28KB 32KB >36KB\n");
    printf("%s\n", s1.c_str());
    printf("%s\n", s2.c_str());
    printf("----------------------------------------------\n");
    /*printf("-----Last Metadata-------\n");
        printf("Last written LPN: %ld\n", current_offset-1);
        printf("Last logfile seq: %u\n", logfile_seq_);
    for(auto &log : alive_log_files_){
        printf("%u %ld %ld\n", log.seq, log.start_lpn, log.end_lpn);
    }
    printf("------------------------\n");*/
#endif
}

}  // namespace ssdlogging