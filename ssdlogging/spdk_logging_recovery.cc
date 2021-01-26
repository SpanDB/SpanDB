#include "spdk_logging_recovery.h"

namespace ssdlogging {

std::vector<uint64_t> SSDLoggingRecovery::GetLogs(){
  std::vector<uint64_t>  res;
  for (auto &log : logs_) {
    res.push_back(log.seq);
  }
  return res;
}

void SSDLoggingRecovery::RecoveryServer(int coreid) {
  printf("SPDK start recovery...\n");
  SetAffinity("Recovery server", coreid);
  // 1. Preparation
  std::vector<std::thread> process_threads;
  std::vector<std::thread> read_threads;
  int numCores = sysconf(_SC_NPROCESSORS_ONLN);
  int n = 1, m = 1;

  int curr_coreid = 37;
  // 2. process data
  auto fn1 =
      std::bind(&SSDLoggingRecovery::ProcessData, this, std::placeholders::_1);
  for (int i = 0; i < n; i++) {
    // process_threads.emplace_back(fn1, (i+1) % numCores);
    process_threads.emplace_back(fn1, curr_coreid--);
  }

  // 3. read from spdk
  auto fn2 = std::bind(
      &SSDLoggingRecovery::ReadFromSPDK, this, std::placeholders::_1,
      std::placeholders::_2, std::placeholders::_3, std::placeholders::_4,
      std::placeholders::_5, std::placeholders::_6, std::placeholders::_7);
  int num = logs_.size();
  reader_num_ = 6;
  for (int i = 0; i < num - 1; i++) {
    uint64_t start = logs_[i].start_lpn;
    uint64_t end = logs_[i].end_lpn;
    uint32_t log_num = logs_[i].seq;
    printf("Recovery: log: %u start: %ld end: %ld\n", log_num, start, end);
    assert(start > 0);
    assert(end > 0);
    for (int j = 0; j < m; j++) {
      read_threads.emplace_back(fn2, curr_coreid--, j, reader_num_ / m, start,
                                end, log_num,
                                page_buffer_->GetCurrentWriteSeq());
    }
    for (auto &t : read_threads) {
      t.join();
    }
  }
  {  // last logfile, we don't know the end_lpn
    uint64_t start = logs_[num - 1].start_lpn;
    uint64_t end = 0;
    uint32_t log_num = logs_[num - 1].seq;
    assert(start > 0);
    printf("Recovery: log: %u start: %ld\n", log_num, start);
    for (int j = 0; j < m; j++) {
      read_threads.emplace_back(fn2, curr_coreid--, j, reader_num_ / m, start,
                                end, log_num,
                                page_buffer_->GetCurrentWriteSeq());
    }
    for (auto &t : read_threads) {
      t.join();
    }
  }

  // 4. finish
  page_buffer_->Stop();
  for (auto &t : process_threads) t.join();
  entry_buffer_->stop();
}

/*
 * if end_lpn == 0, read page until page's log_seq != logfile_seq
 * if end_lpn > 0, read page until page_no == end_lpn
 */
void SSDLoggingRecovery::ReadFromSPDK(int coreid, int id, int reader_num,
                                      uint64_t start_lpn, uint64_t end_lpn,
                                      uint32_t logfile_seq, int64_t first_seq) {
  SetAffinity("Recovery read", coreid);
  struct NSEntry *ns_entry = spdk->namespaces;
  assert(ns_entry != nullptr);
  // 0. Prepare
  int num = 0;
  double time = 0;
  uint64_t pending_req = 0;
  bool stop = false;
  char **buffer = new char *[reader_num];
  bool *completion_flags = new bool[reader_num];
  bool *free_flags = new bool[reader_num];
  bool *copy_finish = new bool[reader_num];
  int64_t *sequence = new int64_t[reader_num];
  uint64_t *senttime = new uint64_t[reader_num];
  for (int i = 0; i < reader_num; i++) {
    buffer[i] = (char *)spdk_zmalloc(PAGESIZE, PAGESIZE, NULL,
                                     SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    completion_flags[i] = true;
    free_flags[i] = true;
    copy_finish[i] = false;
    sequence[i] = -1;
  }
  while (true) {
    if (UNLIKELY(stop && pending_req == 0)) break;
    // 1. find a free reader
    int index = -1;
    for (int i = 0; i < reader_num; i++) {
      if (free_flags[i]) {
        index = i;
        break;
      }
    }
    // 2. send a read request
    if (!stop && index != -1) {
      int64_t seq = page_buffer_->GetWriteSeq();
      if (LIKELY(seq != -1)) {
        assert(completion_flags[index]);
        assert(sequence[index] == -1);
        uint64_t lpn = start_lpn + (seq - first_seq);
        if (LIKELY(end_lpn == 0 || lpn <= end_lpn)) {
          sequence[index] = seq;
          pending_req++;
          free_flags[index] = false;
          senttime[index] = SPDK_TIME;
          completion_flags[index] = false;
          // printf("read lpn: %ld\n", lpn);
          int rc = spdk_nvme_ns_cmd_read(
              ns_entry->ns, ns_entry->qpair[id * reader_num + index],
              buffer[index], lpn * (PAGESIZE / ns_entry->sector_size),
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
              &(completion_flags[index]), 0);
          if (UNLIKELY(rc != 0)) {
            printf("spdk write failed: %d\n", rc * -1);
            assert(0);
          }
        } else {
          stop = true;
        }
      }
    }
    // 3. check completion
    for (int i = 0; i < reader_num; i++) {
      if (!free_flags[i]) {  // we sent one request before
        if (!completion_flags[i]) {
          spdk_nvme_qpair_process_completions(
              ns_entry->qpair[id * reader_num + i], 0);  // spdk check
        }
        if (completion_flags[i] && !copy_finish[i]) {  // finish one request
          assert(sequence[i] != -1);
          uint32_t s = DecodeFixed32(buffer[i]);
          if (end_lpn > 0 || s == logfile_seq) {
            assert(s == logfile_seq);
            char *data = (char *)malloc(sizeof(char) * PAGESIZE);
            memcpy(data, buffer[i], PAGESIZE);
            page_buffer_->Write(sequence[i], data);
            copy_finish[i] = true;
            // printf("write to buffer: %ld %u\n", sequence[i],
            // DecodeFixed32(data+4));
          } else {
            assert(end_lpn == 0);
            page_buffer_->Write(sequence[i], nullptr);
            copy_finish[i] = true;
            stop = true;
          }
        }
        if (completion_flags[i] && copy_finish[i]) {
          if (!page_buffer_->FinishWrite(sequence[i])) continue;
          copy_finish[i] = false;
          sequence[i] = -1;
          free_flags[i] = true;
          pending_req--;
          time +=
              SPDK_TIME_DURATION(senttime[i], SPDK_TIME, spdk_get_ticks_hz());
          num++;
        }
      }
    }
  }
  for (int i = 0; i < reader_num; i++) {
    spdk_free(buffer[i]);
  }
  // time = SPDK_TIME_DURATION(start, SPDK_TIME, spdk_get_ticks_hz());
  printf("Read finish: %.2lf / %d = %.2lf!\n", time, num, time / num);
}

void SSDLoggingRecovery::ProcessData(int coreid) {
  SetAffinity("Recovery process data", coreid);
  char *page = page_buffer_->Read();
  uint32_t pos = 4;  // first 4 bytes is seq
  uint32_t left = PAGESIZE - 4;
  uint32_t log_tag_seq = 0;
  int num = 0;
  while (page != nullptr) {
    if (pos == 4) {
      log_tag_seq = DecodeFixed32(page);
    }
    if (left < 4) {
      free(page);
      page = page_buffer_->Read();
      if (page == nullptr) break;
      left = PAGESIZE - 4;
      pos = 4;
    }
    uint32_t size = DecodeFixed32(page + pos);
    if (size == 0) {
      free(page);
      page = page_buffer_->Read();
      if (page == nullptr) break;
      // printf("read from buffer: %u %u\n",  DecodeFixed32(page),
      // DecodeFixed32(page+4));
      left = PAGESIZE - 4;
      pos = 4;
      continue;
    }
    pos += 4;
    left -= 4;
    LogEntry *log_entry = new LogEntry(size, log_tag_seq);
    if (left >= size) {
      log_entry->Append(page + pos, size);
      pos += size;
      left -= size;
      assert(log_entry->Check());
      entry_buffer_->WriteInBuf(log_entry);
      num++;
      continue;
    }
    // we need to read next page
    log_entry->Append(page + pos, left);
    uint32_t r = size - left;
    pos += left;
    left = 0;
    assert(pos == PAGESIZE);
    while (r != 0) {
      free(page);
      page = page_buffer_->Read();
      assert(page != nullptr);
      left = PAGESIZE - 4;
      pos = 4;
      if (left >= r) {
        log_entry->Append(page + pos, r);
        pos += r;
        left -= r;
        r = 0;
        assert(log_entry->Check());
        entry_buffer_->WriteInBuf(log_entry);
        num++;
        continue;
      }
      // we need to read next page
      log_entry->Append(page + pos, left);
      r = r - left;
      pos += left;
      left = 0;
      assert(pos == PAGESIZE);
    }
  }
  printf("Process finish, recovery %d log entry!\n", num);
}

char *SSDLoggingRecovery::GetLogEntry(uint32_t &size, uint32_t &log_tag_seq) {
  const int64_t seq = entry_buffer_->GetReadableSeq();
  // printf("read: %ld\n", seq);
  if (seq < 0) {
    size = 0;
    log_tag_seq = 0;
    return nullptr;
  }
  LogEntry *log_entry = entry_buffer_->ReadFromBuf(seq);
  assert(log_entry != nullptr);
  // printf("finish reading: %ld\n", seq);
  entry_buffer_->FinishReading(seq);
  size = log_entry->size_;
  log_tag_seq = log_entry->log_tag_seq - base_log_tag_seq_;
  char *data = log_entry->data_;
  free(log_entry);
  return data;
}

void SSDLoggingRecovery::ReadMetadata() {
  // read
  char *metadata = (char *)spdk_zmalloc(
      PAGESIZE, PAGESIZE, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
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
  uint32_t offset = 0;
  base_log_tag_seq_ = DecodeFixed32(metadata + offset);
  offset += 4;
  uint32_t num = DecodeFixed32(metadata + offset);
  offset += 4;
  uint32_t last_lpn = 0;
  for (uint32_t i = 0; i < num; i++) {
    uint32_t seq = DecodeFixed32(metadata + offset);
    offset += 4;
    uint64_t start = DecodeFixed64(metadata + offset);
    offset += 8;
    uint64_t end = DecodeFixed64(metadata + offset);
    offset += 8;
    logs_.emplace_back(seq, start, end);
    // check
    if (last_lpn != 0) assert(last_lpn + 1 == start);
    last_lpn = end;
  }
  spdk_free(metadata);
  // test
  printf("-----Read Metadata-------\n");
  printf("Base log tag seq: %u\n", base_log_tag_seq_);
  for (auto &log : logs_) {
    printf("%u %ld %ld\n", log.seq, log.start_lpn, log.end_lpn);
  }
  printf("------------------------\n");
}

}  // namespace ssdlogging