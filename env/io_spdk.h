#pragma once

#include "algorithm"
#include "chrono"
// #include "lru_cache.h"
#include "port/likely.h"
#include "port/port.h"
#include "port/sys_time.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "spdk_free_list.h"
#include "ssdlogging/spdk_device.h"
#include "ssdlogging/util.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/murmurhash.h"
#include "util/random.h"
#include "util/rate_limiter.h"
#include "utilities/lock_free_queue/disruptor_queue.h"
#include "seg_lru_cache.h"
#include "hhvm_lru_cache.h"

#define HUGE_PAGE_SIZE (1 << 26)
#define SPDK_PAGE_SIZE (4096)
#define FILE_BUFFER_ENTRY_SIZE (1ull << 12)
#define META_SIZE (1 << 20)
#define LO_START_LPN ((10ull << 30) / (SPDK_PAGE_SIZE))
#define LO_FILE_START (LO_START_LPN + (META_SIZE) / (SPDK_PAGE_SIZE))
#define DISRUPTOR_QUEUE_LENGTH (64ull<<20)

namespace rocksdb {

struct SPDKFileMeta;
struct BufferPool;
struct HugePage;
struct LRUEntry;
class SpdkFile;

static ssdlogging::SPDKInfo *sp_info_ = nullptr;
static int topfs_queue_num_ = 0;
static int total_queue_num_ = 0;
static uint64_t SPDK_MAX_LPN;

static SPDKFreeList free_list;
static HugePage *huge_pages_ = nullptr;
static uint64_t SPDK_MEM_POOL_ENTRY_NUM;

TopFSCache<uint64_t, std::shared_ptr<LRUEntry>> *topfs_cache = nullptr;
typedef DisruptorQueue<char *, DISRUPTOR_QUEUE_LENGTH> MemPool;
MemPool *spdk_mem_pool_ = nullptr;

typedef std::map<std::string, SPDKFileMeta *> FileMeta;
typedef std::map<std::string, SpdkFile *> FileSystem;
FileMeta file_meta_;
FileSystem file_system_;
port::Mutex fs_mutex_;
port::Mutex meta_mutex_;
char *meta_buffer_;
static uint64_t spdk_tsc_rate_;
static port::Mutex **spdk_queue_mutexes_ = nullptr;
static uint64_t spdk_queue_id_ = 0;

static void Exit();
static void ExitError();

#ifdef SPANDB_STAT
static uint64_t read_hit_;
static uint64_t read_miss_;
static ssdlogging::statistics::AvgStat free_list_latency_;
static ssdlogging::statistics::AvgStat write_latency_;
static ssdlogging::statistics::AvgStat read_latency_;
static ssdlogging::statistics::AvgStat read_miss_latency_;
static ssdlogging::statistics::AvgStat read_hit_latency_;
static ssdlogging::statistics::AvgStat read_disk_latency_;
static ssdlogging::statistics::AvgStat memcpy_latency_;
static ssdlogging::statistics::AvgStat test_latency_;
#endif

#ifdef PRINT_STAT
static std::mutex print_mutex_;
static std::atomic<uint64_t> total_flush_written_;
static std::atomic<uint64_t> total_compaction_written_;
static std::atomic<uint64_t> last_print_flush_time_;
static std::atomic<uint64_t> last_print_compaction_time_;
static std::atomic<uint64_t> spdk_start_time_;
#endif

struct HugePage {
 public:
  HugePage(uint64_t size)
      : size_(size), hugepages_(nullptr), index_(0), offset_(0) {
    page_num_ = size_ / HUGE_PAGE_SIZE;
    if (size % HUGE_PAGE_SIZE != 0) page_num_++;
    hugepages_ = new char *[page_num_];
    printf("SPDK memory allocation starts (%.2lf GB)\n",
           size_ / 1024.0 / 1024.0 / 1024.0);
    for (uint64_t i = 0; i < page_num_; i++) {
      if (i % (uint64_t)(page_num_ * 0.2) == 0) {
        printf("...%.1f%%", i * 1.0 / page_num_ * 100);
        fflush(stdout);
      }
      hugepages_[i] =
          (char *)spdk_zmalloc(HUGE_PAGE_SIZE, SPDK_PAGE_SIZE, NULL,
                               SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
      if (UNLIKELY(hugepages_[i] == nullptr)) {
        printf("\n");
        fprintf(stderr, "already allocated %.2lf GB\n",
                HUGE_PAGE_SIZE * i / 1024.0 / 1024.0 / 1024.0);
        fprintf(stderr, "%s:%d: SPDK allocate memory failed\n", __FILE__,
                __LINE__);
        ExitError();
      }
    }
    printf("...100%%\nSPDK memory allocation finished.\n");
  };
  char *Get(uint64_t size) {
    assert(size <= HUGE_PAGE_SIZE);
    if (HUGE_PAGE_SIZE - offset_ < size) {
      index_++;
      offset_ = 0;
    }
    if (index_ >= page_num_) return nullptr;
    char *data = hugepages_[index_] + offset_;
    offset_ += size;
    return data;
  }
  ~HugePage() {
    for (uint64_t i = 0; i < page_num_; i++) {
      if (hugepages_[i] != nullptr) spdk_free(hugepages_[i]);
    }
  }

 private:
  uint64_t size_;
  uint64_t page_num_;
  char **hugepages_;
  uint64_t index_;
  uint64_t offset_;
};

struct LRUEntry {
  uint64_t key;
  char *data;
  LRUEntry(uint64_t k, char *d) : key(k), data(d) {}
  ~LRUEntry() {spdk_mem_pool_->WriteInBuf(data);}
};

// file buffer entry
struct FileBuffer {
  int queue_id_;
  uint64_t start_lpn_;
  bool synced_;
  bool readable_;
  uint64_t start_time_;
  std::mutex mutex_;
  char *data_;
  const uint64_t max_size_ = FILE_BUFFER_ENTRY_SIZE;
  uint64_t written_offset_;

  explicit FileBuffer(uint64_t start_lpn)
      : queue_id_(-1),
        start_lpn_(start_lpn),
        synced_(false),
        readable_(false),
        data_(nullptr),
        written_offset_(0) {}

  Status AllocateSPDKBuffer() {
    if(spdk_mem_pool_->Length() < 2000)
      topfs_cache->Evict(2000 - spdk_mem_pool_->Length());
#ifdef SPANDB_STAT
    auto start = SPDK_TIME;
#endif
    assert(data_ == nullptr);
    queue_id_ = -1;
    readable_ = false;
    synced_ = false;
    assert(written_offset_ == 0);
    data_ = spdk_mem_pool_->ReadValue();
#ifdef SPANDB_STAT
    test_latency_.add(
            SPDK_TIME_DURATION(start, SPDK_TIME, spdk_tsc_rate_));
#endif
    assert(data_ != nullptr);
    return Status::OK();
  }

  ~FileBuffer() {}

  bool Full() {
    assert(data_ != nullptr);
    return written_offset_ == max_size_;
  }
  bool Empty() {
    if (data_ == nullptr) return true;
    return written_offset_ == 0;
  }
  size_t Append(const char *data, size_t size) {
    size_t s = 0;
    if (written_offset_ == max_size_) return s;
    if (written_offset_ + size > max_size_)
      s = max_size_ - written_offset_;
    else
      s = size;
    memcpy(data_ + written_offset_, data, s);
    written_offset_ += s;
    return s;
  }
};

struct SPDKFileMeta {
  std::string fname_;
  uint64_t start_lpn_;
  uint64_t end_lpn_;
  bool lock_file_;
  uint64_t size_;
  uint64_t modified_time_;
  uint64_t last_access_time_;
  bool readable_file;
  SPDKFileMeta(){};
  SPDKFileMeta(std::string fname, uint64_t start, uint64_t end, bool lock_file)
      : fname_(fname),
        start_lpn_(start),
        end_lpn_(end),
        lock_file_(lock_file),
        size_(0),
        modified_time_(0),
        last_access_time_(0),
        readable_file(false) {}
  SPDKFileMeta(std::string fname, uint64_t start, uint64_t end, bool lock_file,
               uint64_t size)
      : fname_(fname),
        start_lpn_(start),
        end_lpn_(end),
        lock_file_(lock_file),
        size_(size),
        modified_time_(0),
        last_access_time_(0) {}

  uint64_t Size() { return size_; };

  uint64_t ModifiedTime() { return modified_time_; }

  void SetReadable(bool readable) { readable_file = readable; }

  uint32_t Serialization(char *des) {
    uint32_t offset = 0;
    uint64_t size = fname_.size();
    EncodeFixed64(des + offset, size);
    offset += 8;
    memcpy(des + offset, fname_.c_str(), size);
    offset += size;
    EncodeFixed64(des + offset, start_lpn_);
    offset += 8;
    EncodeFixed64(des + offset, end_lpn_);
    offset += 8;
    char lock = lock_file_ ? '1' : '0';
    memcpy(des + offset, &lock, 1);
    offset += 1;
    EncodeFixed64(des + offset, size_);
    offset += 8;
    EncodeFixed64(des + offset, modified_time_);
    offset += 8;
    return offset;
  }

  uint32_t Deserialization(char *src) {
    uint32_t offset = 0;
    uint64_t size = 0;
    size = DecodeFixed64(src + offset);
    offset += 8;
    fname_ = std::string(src + offset, size);
    offset += fname_.size();
    start_lpn_ = DecodeFixed64(src + offset);
    offset += 8;
    end_lpn_ = DecodeFixed64(src + offset);
    offset += 8;
    char lock;
    memcpy(&lock, src + offset, 1);
    lock_file_ = (lock == '1');
    offset += 1;
    size_ = DecodeFixed64(src + offset);
    offset += 8;
    modified_time_ = DecodeFixed64(src + offset);
    offset += 8;
    return offset;
  }

  std::string ToString() {
    char out[200];
    sprintf(out,
            "FileName: %s, StartLPN: %ld, EndLPN: %ld, IsLock: %d, Size: %ld, "
            "ModifiedTime: %ld\n",
            fname_.c_str(), start_lpn_, end_lpn_, lock_file_, size_,
            modified_time_);
    return std::string(out);
  }
};

static bool SPDKWrite(ssdlogging::SPDKInfo *spdk, FileBuffer *buf);

static void Init(std::string pcie_addr, int logging_queue_num,
                 int topfs_queue_num,  int cache_size) {
  sp_info_ = ssdlogging::InitSPDK(pcie_addr, logging_queue_num);
  SPDK_MAX_LPN =
      ((uint64_t)(sp_info_->namespaces->capacity * 0.8)) / SPDK_PAGE_SIZE;
  spdk_tsc_rate_ = spdk_get_ticks_hz();
  assert(sp_info_ != nullptr);
  total_queue_num_ = sp_info_->num_io_queues;
  topfs_queue_num_ = topfs_queue_num;
  if (total_queue_num_ < topfs_queue_num_ + logging_queue_num + 1) {
    fprintf(stderr,
            "total queue num (%d) < topfs queue num (%d) + logging queue num (%d) "
            "+ 1\n",
            total_queue_num_, topfs_queue_num_, logging_queue_num);
    fprintf(stderr, "one queue is dedicated for logging metadata\n");
    ExitError();
  }
  printf("total queue: %d, topfs queue: %d\n", total_queue_num_, topfs_queue_num_);

  // 1.Allocate mem pool
  if(DISRUPTOR_QUEUE_LENGTH * FILE_BUFFER_ENTRY_SIZE < cache_size * (1ull<<30)){
    fprintf(stderr, "spdk memory pool size is smaller than cache size\n");
    ExitError();
  }
  huge_pages_ = new HugePage(cache_size * (1ull << 30));
  SPDK_MEM_POOL_ENTRY_NUM =  (cache_size * (1ull << 30)) / FILE_BUFFER_ENTRY_SIZE;
  spdk_mem_pool_ = new MemPool();
  for (uint64_t i = 0; i < SPDK_MEM_POOL_ENTRY_NUM; i++) {
    char *buffer = huge_pages_->Get(FILE_BUFFER_ENTRY_SIZE);
    if (UNLIKELY(buffer == nullptr)) {
      fprintf(stderr, "%s:%d: SPDK allocate memory failed\n", __FILE__,
              __LINE__);
      ExitError();
    }
    spdk_mem_pool_->WriteInBuf(buffer);
  }
  // topfs_cache = new SegLRUCache<uint64_t, std::shared_ptr<LRUEntry>>(SPDK_MEM_POOL_ENTRY_NUM - 200);
  topfs_cache = new HHVMLRUCache<uint64_t, std::shared_ptr<LRUEntry>>(SPDK_MEM_POOL_ENTRY_NUM - 2000);
  // 2. Allocate meta buffer
  meta_buffer_ = (char *)spdk_zmalloc(META_SIZE, SPDK_PAGE_SIZE, NULL,
                                      SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
  if (UNLIKELY(meta_buffer_ == nullptr)) {
    fprintf(stderr, "%s:%d: SPDK allocate memory failed\n", __FILE__, __LINE__);
    ExitError();
  }
  // 3.Initialize queue mutex and  queue stat
  spdk_queue_mutexes_ = new port::Mutex *[total_queue_num_];
  for (int i = 0; i < total_queue_num_; i++) {
    spdk_queue_mutexes_[i] = new port::Mutex();
  }
#ifdef PRINT_STAT
  last_print_flush_time_.store(0);
  last_print_compaction_time_.store(0);
  spdk_start_time_.store(0);
  total_flush_written_.store(0);
  total_compaction_written_.store(0);
#endif
}

static void RemoveFromLRUCache(uint64_t start_lpn, uint64_t end_lpn) {
  uint64_t start = start_lpn;
  uint64_t interval = FILE_BUFFER_ENTRY_SIZE / SPDK_PAGE_SIZE;
  while (start <= end_lpn) {
    topfs_cache->DeleteKey(start);
    start += interval;
  }
}

static void ExitError() {
  Exit();
  exit(-1);
}

static uint64_t GetSPDKQueueID() {
  return total_queue_num_ -
         (__sync_fetch_and_add(&spdk_queue_id_, 1) % topfs_queue_num_) - 1;
}

static void WriteComplete(void *arg, const struct spdk_nvme_cpl *completion) {
  FileBuffer *buf = (FileBuffer *)arg;
  assert(buf->synced_ == false);
  if (spdk_nvme_cpl_is_error(completion)) {
    fprintf(stderr, "I/O error status: %s\n",
            spdk_nvme_cpl_get_status_string(&completion->status));
    fprintf(stderr, "%s:%d: Write I/O failed, aborting run\n", __FILE__,
            __LINE__);
    buf->synced_ = false;
    ExitError();
  }
#ifdef SPANDB_STAT
  write_latency_.add(
      SPDK_TIME_DURATION(buf->start_time_, SPDK_TIME, spdk_tsc_rate_));
#endif
  buf->synced_ = true;
  spandb_controller_.IncreaseLDWritten(FILE_BUFFER_ENTRY_SIZE);
  spandb_controller_.IncreaseLDBgIO(FILE_BUFFER_ENTRY_SIZE);
}

static void ReadComplete(void *arg, const struct spdk_nvme_cpl *completion) {
  FileBuffer *buf = (FileBuffer *)arg;
  assert(buf->readable_ == false);
  if (spdk_nvme_cpl_is_error(completion)) {
    fprintf(stderr, "I/O error status: %s\n",
            spdk_nvme_cpl_get_status_string(&completion->status));
    fprintf(stderr, "%s:%d: Write I/O failed, aborting run\n", __FILE__,
            __LINE__);
    buf->readable_ = false;
    ExitError();
  }
  buf->readable_ = true;
  spandb_controller_.IncreaseLDRead(FILE_BUFFER_ENTRY_SIZE);
}

static void CheckComplete(ssdlogging::SPDKInfo *spdk, int queue_id) {
  assert(queue_id != -1);
  MutexLock lock(spdk_queue_mutexes_[queue_id]);
  spdk_nvme_qpair_process_completions(spdk->namespaces->qpair[queue_id], 0);
}

static bool SPDKWrite(ssdlogging::SPDKInfo *spdk, FileBuffer *buf) {
  assert(buf != NULL);
  assert(buf->synced_ == false);
  assert(buf->data_ != NULL);
  uint64_t sec_per_page = SPDK_PAGE_SIZE / spdk->namespaces->sector_size;
  uint64_t lba = buf->start_lpn_ * sec_per_page;
  uint64_t size = buf->max_size_;
  if (size % SPDK_PAGE_SIZE != 0)
    size = (size / SPDK_PAGE_SIZE + 1) * SPDK_PAGE_SIZE;
  uint64_t sec_num = size / SPDK_PAGE_SIZE * sec_per_page;
  int queue_id = GetSPDKQueueID();
  buf->queue_id_ = queue_id;
#ifdef SPANDB_STAT
  buf->start_time_ = SPDK_TIME;
#endif
  // printf("write lba: %ld, size: %ld sectors, queue: %d\n", lba, sec_num, queue_id);
  int rc = 0;
  {
    MutexLock lock(spdk_queue_mutexes_[queue_id]);
    rc = spdk_nvme_ns_cmd_write(spdk->namespaces->ns,
                                spdk->namespaces->qpair[queue_id], buf->data_,
                                lba,     /* LBA start */
                                sec_num, /* number of LBAs */
                                WriteComplete, buf, 0);
  }
  if (rc != 0) {
    fprintf(stderr, "%s:%d: SPDK IO failed: %s\n", __FILE__, __LINE__,
            strerror(rc * -1));
    ExitError();
  }
  return true;
}

static bool SPDKRead(ssdlogging::SPDKInfo *spdk, FileBuffer *buf) {
  // ssdlogging::ns_entry* namespace = spdk->namespaces;
  assert(buf != NULL);
  assert(buf->readable_ == false);
  assert(buf->data_ != NULL);
  uint64_t sec_per_page = SPDK_PAGE_SIZE / spdk->namespaces->sector_size;
  uint64_t lba = buf->start_lpn_ * sec_per_page;
  uint64_t sec_num = buf->max_size_ / SPDK_PAGE_SIZE * sec_per_page;
  int queue_id = GetSPDKQueueID();
  // printf("read lba: %ld, size: %ld sectors, queue: %d\n", lba, sec_num, queue_id);
  buf->queue_id_ = queue_id;
  int rc = 0;
  {
    MutexLock lock(spdk_queue_mutexes_[queue_id]);
    rc = spdk_nvme_ns_cmd_read(spdk->namespaces->ns,
                               spdk->namespaces->qpair[queue_id], buf->data_,
                               lba,     /* LBA start */
                               sec_num, /* number of LBAs */
                               ReadComplete, buf, 0);
  }
  if (rc != 0) {
    fprintf(stderr, "%s:%d: SPDK IO failed: %s\n", __FILE__, __LINE__,
            strerror(rc * -1));
    ExitError();
  }
  return true;
}

static void SPDKWriteSync(ssdlogging::SPDKInfo *spdk, char *buf, uint64_t lpn,
                          uint64_t size) {
  assert(size % SPDK_PAGE_SIZE == 0);
  uint64_t sec_per_page = SPDK_PAGE_SIZE / spdk->namespaces->sector_size;
  uint64_t lba = lpn * sec_per_page;
  uint64_t sec_num = size / SPDK_PAGE_SIZE * sec_per_page;
  int queue_id = GetSPDKQueueID();
  bool flag = false;
  int rc = 0;
  {
    MutexLock lock(spdk_queue_mutexes_[queue_id]);
    rc = spdk_nvme_ns_cmd_write(
        spdk->namespaces->ns, spdk->namespaces->qpair[queue_id], buf,
        lba,
        sec_num,
        [](void *arg, const struct spdk_nvme_cpl *completion) -> void {
          bool *finished = (bool *)arg;
          if (UNLIKELY(spdk_nvme_cpl_is_error(completion))) {
            fprintf(stderr, "I/O error status: %s\n",
                    spdk_nvme_cpl_get_status_string(&completion->status));
            fprintf(stderr, "%s:%d: Write I/O failed, aborting run\n", __FILE__,
                    __LINE__);
            *finished = true;
            ExitError();
          }
          *finished = true;
        },
        &flag, 0);
  }
  if (rc != 0) {
    fprintf(stderr, "%s:%d: SPDK IO failed: %s\n", __FILE__, __LINE__,
            strerror(rc * -1));
    ExitError();
  }
  while (!flag) {
    CheckComplete(sp_info_, queue_id);
  }
}

static void SPDKReadSync(ssdlogging::SPDKInfo *spdk, char *buf, uint64_t lpn,
                         uint64_t size) {
  uint64_t sec_per_page = SPDK_PAGE_SIZE / spdk->namespaces->sector_size;
  uint64_t lba = lpn * sec_per_page;
  uint64_t sec_num = size / SPDK_PAGE_SIZE * sec_per_page;
  int queue_id = GetSPDKQueueID();
  bool flag = false;
  int rc = 0;
  {
    MutexLock lock(spdk_queue_mutexes_[queue_id]);
    rc = spdk_nvme_ns_cmd_read(
        spdk->namespaces->ns, spdk->namespaces->qpair[queue_id], buf,
        lba,
        sec_num,
        [](void *arg, const struct spdk_nvme_cpl *completion) -> void {
          bool *finished = (bool *)arg;
          if (UNLIKELY(spdk_nvme_cpl_is_error(completion))) {
            fprintf(stderr, "I/O error status: %s\n",
                    spdk_nvme_cpl_get_status_string(&completion->status));
            fprintf(stderr, "%s:%d: Write I/O failed, aborting run\n", __FILE__,
                    __LINE__);
            *finished = true;
            ExitError();
          }
          *finished = true;
        },
        &flag, 0);
  }
  if (rc != 0) {
    fprintf(stderr, "%s:%d: SPDK IO failed: %s\n", __FILE__, __LINE__,
              strerror(rc * -1));
    ExitError();
  }
  while (!flag) {
    CheckComplete(sp_info_, queue_id);
  }
}

static void WriteMeta(FileMeta *file_meta) {
  MutexLock lock(&meta_mutex_);
  assert(meta_buffer_ != nullptr);
  uint64_t size = 0;
  EncodeFixed32(meta_buffer_, (uint32_t)file_meta->size());
  size += 4;
  // printf("---------------------\n");
  // printf("write meta num: %ld\n", file_meta->size());
  for (auto &meta : *file_meta) {
    size += meta.second->Serialization(meta_buffer_ + size);
    // printf("%s\n", meta.second->ToString().c_str());
  }
  // printf("---------------------\n");
  if (size % SPDK_PAGE_SIZE != 0)
    size = (size / SPDK_PAGE_SIZE + 1) * SPDK_PAGE_SIZE;
  assert(size <= META_SIZE);
  if (UNLIKELY(size > META_SIZE)) {
    fprintf(stderr, "%s:%d: Metadata size is too big\n", __FILE__, __LINE__);
    ExitError();
  }
  SPDKWriteSync(sp_info_, meta_buffer_, LO_START_LPN, size);
}

static std::string SplitFname(std::string path) {
  std::size_t found = path.find_last_of("/");
  return path.substr(found + 1);
}

__attribute__((unused)) static void ReadMeta(FileMeta *file_meta,
                                             std::string dbpath) {
  SPDKReadSync(sp_info_, meta_buffer_, LO_START_LPN, META_SIZE);
  uint64_t offset = 0;
  uint32_t meta_num = DecodeFixed32(meta_buffer_ + offset);
  offset += 4;
  for (uint32_t i = 0; i < meta_num; i++) {
    assert(offset < META_SIZE);
    SPDKFileMeta *meta = new SPDKFileMeta();
    offset += meta->Deserialization(meta_buffer_ + offset);
    meta->fname_ = dbpath + "/" + SplitFname(meta->fname_);
    (*file_meta)[meta->fname_] = meta;
  }
}

class SpdkFile {
 public:
  explicit SpdkFile(Env *env, const std::string &fname, SPDKFileMeta *metadata,
                    uint64_t allocate_size, bool is_flush = false)
      : env_(env),
        fname_(fname),
        rnd_(static_cast<uint32_t>(
            MurmurHash(fname.data(), static_cast<int>(fname.size()), 0))),
        current_buf_index_(0),
        last_sync_index_(-1),
        refs_(0),
        metadata_(metadata),
        is_flush_(is_flush),
        allocated_buffer(0) {
    // printf("create spdk file fname: %s, ref: %ld\n", fname_.c_str(), refs_);
    uint64_t current_page = metadata->start_lpn_;
    uint64_t pages_per_buffer = FILE_BUFFER_ENTRY_SIZE / SPDK_PAGE_SIZE;
    file_buffer_num_ = allocate_size / FILE_BUFFER_ENTRY_SIZE;
    if (allocate_size % FILE_BUFFER_ENTRY_SIZE != 0) file_buffer_num_++;
    // printf("open spdkfile: %s\n", metadata_->ToString().c_str());
    // printf("file_buffer_num_: %ld\n", file_buffer_num_);
    file_buffer_ = (FileBuffer **)malloc(sizeof(FileBuffer) * file_buffer_num_);
    for (uint64_t i = 0; i < file_buffer_num_; i++) {
      file_buffer_[i] = new FileBuffer(current_page);
      current_page += pages_per_buffer;
    }
    // Ref();
  }

  int BufferSize() { return allocated_buffer.load(); }

  SpdkFile(const SpdkFile &) = delete;  // No copying allowed.
  void operator=(const SpdkFile &) = delete;

  ~SpdkFile() {
    assert(refs_ == 0);
    // printf("close spdkfile: %s\n", fname_.c_str());
    for (uint64_t i = 0; i < file_buffer_num_; i++) {
      if (file_buffer_[i]->data_ != nullptr) {
        std::shared_ptr<LRUEntry> value(
            new LRUEntry(file_buffer_[i]->start_lpn_, file_buffer_[i]->data_));
        topfs_cache->Insert(file_buffer_[i]->start_lpn_, value);
      }
      delete file_buffer_[i];
      file_buffer_[i] = nullptr;
    }
    free(file_buffer_);
    assert(file_meta_.find(fname_) != file_meta_.end());
  }

  bool is_lock_file() const { return metadata_->lock_file_; }

  bool Lock() {
    assert(metadata_->lock_file_);
    MutexLock lock(&mutex_);
    if (locked_) {
      return false;
    } else {
      locked_ = true;
      return true;
    }
  }

  void Unlock() {
    assert(metadata_->lock_file_);
    MutexLock lock(&mutex_);
    locked_ = false;
  }

  uint64_t Size() const { return metadata_->size_; }

  void Truncate(size_t size) {
    MutexLock lock(&mutex_);
    fprintf(stderr, "%s:%d: SPDK File Truncate() not implemented\n", __FILE__,
            __LINE__);
  }

  void CorruptBuffer() {
    fprintf(stderr, "%s:%d: SPDK File CorruptBuffer() not implemented\n",
            __FILE__, __LINE__);
  }

  Status Read(uint64_t offset, size_t n, Slice *result, char *scratch) {
#ifdef SPANDB_STAT
    auto start = SPDK_TIME;
#endif
    assert(scratch != nullptr);
    size_t size = 0;
    char *ptr = scratch;
    while (n > 0) {
#ifdef SPANDB_STAT
      auto s_time = SPDK_TIME;
      bool hit = false;
#endif
      uint64_t buffer_index = offset / FILE_BUFFER_ENTRY_SIZE;
      uint64_t buffer_offset = offset % FILE_BUFFER_ENTRY_SIZE;
      assert(buffer_index < file_buffer_num_);
      FileBuffer *buffer = file_buffer_[buffer_index];
      uint64_t lpn = buffer->start_lpn_;
      char *data = nullptr;
      std::shared_ptr<LRUEntry> value;
      if (topfs_cache->Find(lpn, value)) {
        data = value->data;
#ifdef SPANDB_STAT
        hit = true;
#endif
      } else {
        std::lock_guard<std::mutex> lk(buffer->mutex_);
        assert(buffer->data_ == nullptr);
        buffer->AllocateSPDKBuffer();
        if (thread_local_info_.GetName() == "Worker") {
          spandb_controller_.IncreaseLDBgIO(FILE_BUFFER_ENTRY_SIZE);
        }
#ifdef SPANDB_STAT
        auto sss = SPDK_TIME;
#endif
        SPDKRead(sp_info_, buffer);
        while (!buffer->readable_) {
          CheckComplete(sp_info_, buffer->queue_id_);
        }
#ifdef SPANDB_STAT
        read_disk_latency_.add(
            SPDK_TIME_DURATION(sss, SPDK_TIME, spdk_tsc_rate_));
#endif
        value.reset(new LRUEntry(lpn, buffer->data_));
        topfs_cache->Insert(lpn, value);
        data = buffer->data_;
        buffer->data_ = nullptr;
      }
      assert(data != nullptr);
      // copy to ptr
      size_t s = 0;
      if (buffer_offset + n <= FILE_BUFFER_ENTRY_SIZE) {
        s = n;
      } else {
        s = FILE_BUFFER_ENTRY_SIZE - buffer_offset;
      }
      memcpy(ptr + size, data + buffer_offset, s);
      size += s;
      offset += s;
      n -= s;
#ifdef SPANDB_STAT
      if (hit) {
        __sync_fetch_and_add(&read_hit_, 1);
        read_hit_latency_.add(
            SPDK_TIME_DURATION(s_time, SPDK_TIME, spdk_tsc_rate_));
      } else {
        __sync_fetch_and_add(&read_miss_, 1);
        read_miss_latency_.add(SPDK_TIME_DURATION(s_time, SPDK_TIME, spdk_tsc_rate_));
      }
#endif
    }
    assert(n == 0);
    *result = Slice(scratch, size);
#ifdef SPANDB_STAT
    read_latency_.add(SPDK_TIME_DURATION(start, SPDK_TIME, spdk_tsc_rate_));
#endif
    return Status::OK();
  }

  Status Write(uint64_t offset, const Slice &data) {
    fprintf(stderr, "%s:%d: SPDK File Write() not implemented\n", __FILE__,
            __LINE__);
    return Status::OK();
  }

  Status Append(const Slice &data) {
    metadata_->last_access_time_ = SPDK_TIME;
    size_t left = data.size();
    size_t written = 0;
    while (left > 0) {
      if (UNLIKELY((uint64_t)current_buf_index_ >= file_buffer_num_)) {
        fprintf(stderr,
                "File size is too big. Allocate size: %llu, Current size: %lu, "
                "write size: %lu(left: %lu)\n",
                file_buffer_num_ * FILE_BUFFER_ENTRY_SIZE, metadata_->size_,
                data.size(), left);
        return Status::IOError("File is oversize when appending");
      }
      // printf("fname: %s, write size: %ld, buf index: %d\n",fname_.c_str(),
      // data.size(), current_buf_index_);
      const char *dat = data.data() + written;
      if (file_buffer_[current_buf_index_]->data_ == nullptr) {
        Status s = file_buffer_[current_buf_index_]->AllocateSPDKBuffer();
        if (!s.ok()) return s;
        allocated_buffer.fetch_add(1);
      }
      size_t size = file_buffer_[current_buf_index_]->Append(dat, left);
      file_buffer_[current_buf_index_]->readable_ = true;
      left -= size;
      written += size;
      assert(left == 0 || file_buffer_[current_buf_index_]->Full());
      if (file_buffer_[current_buf_index_]->Full()) {
        current_buf_index_++;
        if (current_buf_index_ - last_sync_index_ - 1 >= 4) {
          Fsync(false);
        }
      }
    }
#ifdef PRINT_STAT
    if (is_flush_) {
      total_flush_written_.fetch_add(data.size());
    } else {
      total_compaction_written_.fetch_add(data.size());
    }
    if (spdk_start_time_.load() == 0) {
      spdk_start_time_.store(SPDK_TIME);
      last_print_compaction_time_.store(SPDK_TIME);
      last_print_flush_time_.store(SPDK_TIME);
    } else {
      if (SPDK_TIME_DURATION(last_print_compaction_time_.load(), SPDK_TIME,
                             spdk_tsc_rate_) > 1000000) {
        print_mutex_.lock();
        printf("compaction: %s %.2lf\n", ssdlogging::GetDayTime().c_str(),
               total_compaction_written_.load() * 1.0 / 1024 / 1024);
        printf("flush: %s %.2lf\n", ssdlogging::GetDayTime().c_str(),
               total_flush_written_.load() * 1.0 / 1024 / 1024);
        last_print_compaction_time_.store(SPDK_TIME);
        print_mutex_.unlock();
      }
    }
#endif
    assert(left == 0);
    metadata_->modified_time_ = Now();
    metadata_->size_ += data.size();
    return Status::OK();
  }

  Status Fsync(bool write_last) {
    // return Status::OK();
    // write [last_sync_index_ + 1, current_buf_index_ - 1]
    int sync_start = last_sync_index_ + 1;
    while (last_sync_index_ + 1 < current_buf_index_) {
      last_sync_index_++;
      assert(!file_buffer_[last_sync_index_]->synced_);
      SPDKWrite(sp_info_, file_buffer_[last_sync_index_]);
    }
    // write last buffer
    if (write_last && !file_buffer_[current_buf_index_]->Empty()) {
      assert(!file_buffer_[current_buf_index_]->synced_);
      SPDKWrite(sp_info_, file_buffer_[current_buf_index_]);
      last_sync_index_++;
    }
    // wait for finish
    for (int64_t i = sync_start; i <= last_sync_index_; i++) {
      while (!file_buffer_[i]->synced_) {
        int queue_id = file_buffer_[i]->queue_id_;
        CheckComplete(sp_info_, queue_id);
      }
    }
    return Status::OK();
  }

  Status Close() {
    WriteMeta(&file_meta_);
    return Status::OK();
  }

  void Ref() {
    refs_++;
    assert(refs_ > 0);
  }

  void Unref() {
    refs_--;
    assert(refs_ >= 0);
    if (refs_ == 0) {
      MutexLock lock(&fs_mutex_);
      file_system_.erase(fname_);
      delete this;
    }
  }

  uint64_t ModifiedTime() const { return metadata_->modified_time_; }

  int64_t GetRef() { return refs_; }

  std::string GetName() { return fname_; }

  SPDKFileMeta *GetMetadata() { return metadata_; }

 private:
  uint64_t Now() {
    int64_t unix_time = 0;
    auto s = env_->GetCurrentTime(&unix_time);
    assert(s.ok());
    return static_cast<uint64_t>(unix_time);
  }

  Env *env_;
  FileBuffer **file_buffer_;
  const std::string fname_;
  mutable port::Mutex mutex_;
  bool locked_;
  Random rnd_;
  int64_t current_buf_index_;
  int64_t last_sync_index_;
  int64_t refs_;
  SPDKFileMeta *metadata_;
  uint64_t file_buffer_num_;
  bool is_flush_;
  std::atomic<int> allocated_buffer;
};

class SpdkRandomAccessFile : public RandomAccessFile {
 public:
  explicit SpdkRandomAccessFile(SpdkFile *file) : file_(file) {
    file_->Ref();
    file_->GetMetadata()->SetReadable(true);
  }

  ~SpdkRandomAccessFile() override { file_->Unref(); }

  Status Read(uint64_t offset, size_t n, Slice *result,
              char *scratch) const override {
    return file_->Read(offset, n, result, scratch);
  }

 private:
  SpdkFile *file_;
};

class SpdkWritableFile : public WritableFile {
 public:
  SpdkWritableFile(SpdkFile *file, RateLimiter *rate_limiter)
      : file_(file), rate_limiter_(rate_limiter) {
    file_->Ref();
  }

  ~SpdkWritableFile() override { file_->Unref(); }

  Status Append(const Slice &data) override {
    size_t bytes_written = 0;
    while (bytes_written < data.size()) {
      auto bytes = RequestToken(data.size() - bytes_written);
      Status s = file_->Append(Slice(data.data() + bytes_written, bytes));
      if (!s.ok()) {
        return s;
      }
      bytes_written += bytes;
    }
    return Status::OK();
  }

  Status Allocate(uint64_t /*offset*/, uint64_t /*len*/) override {
    return Status::OK();
  }

  Status Truncate(uint64_t size) override {
    file_->Truncate(static_cast<size_t>(size));
    return Status::OK();
  }
  Status Close() override { return file_->Close(); }

  Status Flush() override { return Status::OK(); }

  Status Sync() override {
    return file_->Fsync(true);
    Status::OK();
  }

  uint64_t GetFileSize() override { return file_->Size(); }

 private:
  inline size_t RequestToken(size_t bytes) {
    if (rate_limiter_ && io_priority_ < Env::IO_TOTAL) {
      bytes = std::min(
          bytes, static_cast<size_t>(rate_limiter_->GetSingleBurstBytes()));
      rate_limiter_->Request(bytes, io_priority_);
    }
    return bytes;
  }

  SpdkFile *file_;
  RateLimiter *rate_limiter_;
};

__attribute__((unused)) static void TopFSResetStat(){
#ifdef SPANDB_STAT
  read_hit_ = read_miss_ = 0;
  free_list_latency_.reset();
  write_latency_.reset();
  read_latency_.reset();
  read_miss_latency_.reset();
  read_hit_latency_.reset();
  read_disk_latency_.reset();
  memcpy_latency_.reset();
  test_latency_.reset();
#endif
  topfs_cache->ResetStat();
}

static void Exit() {
  // 1.free meta and filesystem
  for (auto it = file_system_.begin(); it != file_system_.end();) {
    auto next = ++it;
    it--;
    assert(it->second != nullptr);
    it->second->Unref();
    it = next;
  }
  // 2.write metadata and free meta buffer
  if (meta_buffer_ != nullptr) {
    WriteMeta(&file_meta_);
    spdk_free(meta_buffer_);
  }
  // 3.delete metadata
  for (auto &meta : file_meta_) {
    assert(meta.second != nullptr);
    delete meta.second;
  }
  // 4.free mem pool
  if (topfs_cache != nullptr) {
    delete topfs_cache;
  }
  if (spdk_mem_pool_ != nullptr) {
    delete spdk_mem_pool_;
  }
  if (huge_pages_ != nullptr) {
    delete huge_pages_;
  }
  // 5.free mutex
  if (spdk_queue_mutexes_ != nullptr) {
    for (int i = 0; i < total_queue_num_; i++) {
      delete spdk_queue_mutexes_[i];
    }
    delete spdk_queue_mutexes_;
  }
  // 6. output
  if (FILE_BUFFER_ENTRY_SIZE > (1ull << 20)) {
    printf("FILE_BUFFER_ENTRY_SIZE: %.2lf MB\n",
           FILE_BUFFER_ENTRY_SIZE / (1024 * 1024 * 1.0));
  } else {
    printf("FILE_BUFFER_ENTRY_SIZE: %.2lf KB\n",
           FILE_BUFFER_ENTRY_SIZE / (1024 * 1.0));
  }
#ifdef SPANDB_STAT
  printf("--------------L0 env---------------------\n");
  printf("write latency: %ld %.2lf us\n", write_latency_.size(),
         write_latency_.avg());
  printf("read latency: %ld %.2lf us\n", read_latency_.size(),
         read_latency_.avg());
  printf("read miss latency: %ld %.2lf us\n", read_miss_latency_.size(),
         read_miss_latency_.avg());
  printf("read hit latency: %ld %.2lf us\n", read_hit_latency_.size(),
         read_hit_latency_.avg());
  printf("read from disk: %ld %.2lf us\n", read_disk_latency_.size(),
         read_disk_latency_.avg());
  if (read_hit_ + read_miss_ != 0) {
    printf("read hit: %ld, read miss: %ld\n", read_hit_, read_miss_);
    printf("read hit ratio: %lf\n",
           (read_hit_ * 1.0) / (read_hit_ + read_miss_));
  }
  printf("free_list_latency: %ld %.2lf\n", free_list_latency_.size(),
         free_list_latency_.avg());
  printf("memcpy latency: %ld %.2lf\n", memcpy_latency_.size(),
         memcpy_latency_.avg());
  printf("test latency: %ld %.2lf\n", test_latency_.size(),
         test_latency_.avg());
  printf("------------------------------------------\n");
#endif
}

__attribute__((unused)) static Status check_overlap(uint64_t start_lpn,
                                                    uint64_t end_lpn,
                                                    std::string fn) {
  MutexLock lock(&meta_mutex_);
  if (end_lpn < start_lpn) {
    fprintf(stderr, "%s: end_lpn:%ld <= start_lpn:%ld\n", fn.c_str(), end_lpn,
            start_lpn);
    return Status::IOError("SPDK can not new file\n");
  }
  bool overlap = false;
  for (auto &m : file_meta_) {
    if (end_lpn < m.second->start_lpn_ || start_lpn > m.second->end_lpn_) {
      continue;
    }
    overlap = true;
    fprintf(stderr, "file overlap\n");
    fprintf(stderr, "new file: fname: %s, start: %ld, end: %ld\n", fn.c_str(),
            start_lpn, end_lpn);
    fprintf(stderr, "old file: %s\n", m.second->ToString().c_str());
  }
  if (overlap) {
    uint64_t occupy_page = 0;
    for (auto &m : file_meta_) {
      occupy_page += (m.second->end_lpn_ - m.second->start_lpn_);
    }
    printf("the occupied size is %.2lf GB\n",
           occupy_page * SPDK_PAGE_SIZE * 1.0 / 1024 / 1024 / 1024);
    fflush(stdout);
    return Status::IOError("SPDK can not new file because of overlap\n");
  }
  return Status::OK();
}

static std::string NormalizeFilePath(const std::string path) {
  std::string dst;
  for (auto c : path) {
    if (!dst.empty() && c == '/' && dst.back() == '/') {
      continue;
    }
    dst.push_back(c);
  }
  return dst;
}

static void SpanDBMigrationImpl(std::vector<LiveFileMetaData> files_metadata, int max_level){
  bool ld_full = false;
  uint64_t total_moved = 0;
  uint64_t buffer_size = 1ull << 20;
  char *buffer = (char *)spdk_zmalloc(buffer_size, SPDK_PAGE_SIZE, NULL,
                                      SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
  printf("start migration...\n");
  int level = 1;
  for(auto &meta : files_metadata){
    if(meta.level > max_level)
      continue;
    if(meta.level == level){
      printf("migrate level %d...\n", meta.level);
      level++;
    }
    if(free_list.Size() < (60ul << 30) / SPDK_PAGE_SIZE)
      break;
    std::string filename = NormalizeFilePath(meta.db_path + meta.name);
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
      continue;
    }
    uint64_t file_size = meta.size;
    uint64_t allocate_size = file_size / SPDK_PAGE_SIZE + 1;
    uint64_t start_lpn = free_list.Get(allocate_size);
    uint64_t end_lpn = start_lpn + allocate_size - 1;
    uint64_t curr_lpn = start_lpn;
    size_t size = 0;
    while ((size = read(fd, buffer, buffer_size)) > 0) {
      if (size % SPDK_PAGE_SIZE != 0) {
        size = (size / SPDK_PAGE_SIZE + 1) * SPDK_PAGE_SIZE;
      }
      assert(curr_lpn + size / SPDK_PAGE_SIZE - 1 <= end_lpn);
      SPDKWriteSync(sp_info_, buffer, curr_lpn, size);
      curr_lpn += size / SPDK_PAGE_SIZE;
    }
    close(fd);
    total_moved += file_size;
    system(("rm -f " + filename).c_str());
    file_meta_[filename] = new SPDKFileMeta(filename, start_lpn, end_lpn, false, file_size);
  }
  spdk_free(buffer);
  WriteMeta(&file_meta_);
  if (total_moved / (1ull << 30) > 0) {
    printf("Moved %.2lf GB\n", total_moved * 1.0 / (1ull << 30));
  } else {
    printf("Moved %.2lf MB\n", total_moved * 1.0 / (1ull << 20));
  }
  fflush(stdout);
}

}  // namespace rocksdb