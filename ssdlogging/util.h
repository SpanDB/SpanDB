#pragma once
#include "thread"
#include "random"
#include "sys/mman.h"
#include "string.h"
#include "atomic"
#include "sys/syscall.h"
#include "unistd.h"
#include "algorithm"
#include "sys/time.h"
#include "../env/thread_local_info.h"
#include "../env/spandb_options.h"
#include "stdlib.h"

#if defined(__GNUC__) && __GNUC__ >= 4
#define LIKELY(x)   (__builtin_expect((x), 1))
#define UNLIKELY(x) (__builtin_expect((x), 0))
#else
#define LIKELY(x)   (x)
#define UNLIKELY(x) (x)
#endif

namespace ssdlogging{

#define SPDK_TIME (spdk_get_ticks())
#define SPDK_TIME_DURATION(start, end, tsc_rate) (((end) - (start))*1.0 / (tsc_rate) * 1000 * 1000)

#define SSDLOGGING_TIME_NOW (std::chrono::high_resolution_clock::now())
#define SSDLOGGING_TIME_DURATION(start, end) (std::chrono::duration<double>((end)- (start)).count() * 1000 * 1000)

inline void SetAffinity(std::string str, int coreid){
  coreid = coreid % sysconf(_SC_NPROCESSORS_ONLN);
  printf("%s coreid: %d\n", str.c_str(), coreid);
  fflush(stdout);
	cpu_set_t mask;
	CPU_ZERO(&mask);
	CPU_SET(coreid, &mask);
	int rc = sched_setaffinity(syscall(__NR_gettid), sizeof(mask), &mask);
	assert(rc == 0);
}

inline std::string GetDayTime(){
  const int kBufferSize = 100;
  char buffer[kBufferSize];
  struct timeval now_tv;
  gettimeofday(&now_tv, nullptr);
  const time_t seconds = now_tv.tv_sec;
  struct tm t;
  localtime_r(&seconds, &t);
  snprintf(buffer, kBufferSize,
          "%04d/%02d/%02d-%02d:%02d:%02d.%06d",
          t.tm_year + 1900,
          t.tm_mon + 1,
          t.tm_mday,
          t.tm_hour,
          t.tm_min,
          t.tm_sec,
          static_cast<int>(now_tv.tv_usec));
  return std::string(buffer);
}

namespace port{
static inline void AsmVolatilePause() {
	#if defined(__i386__) || defined(__x86_64__)
		asm volatile("pause");
	#elif defined(__aarch64__)
		asm volatile("wfe");
	#elif defined(__powerpc64__)
		asm volatile("or 27,27,27");
	#endif
  // it's okay for other platforms to be no-ops
}
};

const bool kLittleEndian = true;

inline void EncodeFixed32(char* buf, uint32_t value) {
	if (kLittleEndian) {
		memcpy(buf, &value, sizeof(value));
	} else {
		buf[0] = value & 0xff;
		buf[1] = (value >> 8) & 0xff;
		buf[2] = (value >> 16) & 0xff;
		buf[3] = (value >> 24) & 0xff;
	}
}

inline uint32_t DecodeFixed32(const char* ptr) {
	if (kLittleEndian) {
		// Load the raw bytes
		uint32_t result;
		memcpy(&result, ptr, sizeof(result));// gcc optimizes this to a plain load
		return result;
	} else {
		return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[0])))
			| (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) << 8)
			| (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) << 16)
			| (static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])) << 24));
	}
}

inline uint64_t DecodeFixed64(const char* ptr) {
  if (kLittleEndian) {
    // Load the raw bytes
    uint64_t result;
    memcpy(&result, ptr, sizeof(result));  // gcc optimizes this to a plain load
    return result;
  } else {
    uint64_t lo = DecodeFixed32(ptr);
    uint64_t hi = DecodeFixed32(ptr + 4);
    return (hi << 32) | lo;
  }
}

inline void EncodeFixed64(char* buf, uint64_t value) {
  if (kLittleEndian) {
    memcpy(buf, &value, sizeof(value));
  } else {
    buf[0] = value & 0xff;
    buf[1] = (value >> 8) & 0xff;
    buf[2] = (value >> 16) & 0xff;
    buf[3] = (value >> 24) & 0xff;
    buf[4] = (value >> 32) & 0xff;
    buf[5] = (value >> 40) & 0xff;
    buf[6] = (value >> 48) & 0xff;
    buf[7] = (value >> 56) & 0xff;
  }
}

namespace random{
// A very simple random number generator.  Not especially good at
// generating truly random bits, but good enough for our needs in this
// package.
class Random {
 private:
  enum : uint32_t {
    M = 2147483647L  // 2^31-1
  };
  enum : uint64_t {
    A = 16807  // bits 14, 8, 7, 5, 2, 1, 0
  };

  uint32_t seed_;

  static uint32_t GoodSeed(uint32_t s) { return (s & M) != 0 ? (s & M) : 1; }

 public:
  // This is the largest value that can be returned from Next()
  enum : uint32_t { kMaxNext = M };

  explicit Random(uint32_t s) : seed_(GoodSeed(s)) {}

  void Reset(uint32_t s) { seed_ = GoodSeed(s); }

  uint32_t Next() {
    // We are computing
    //       seed_ = (seed_ * A) % M,    where M = 2^31-1
    //
    // seed_ must not be zero or M, or else all subsequent computed values
    // will be zero or M respectively.  For all other values, seed_ will end
    // up cycling through every number in [1,M-1]
    uint64_t product = seed_ * A;

    // Compute (product % M) using the fact that ((x << 31) % M) == x.
    seed_ = static_cast<uint32_t>((product >> 31) + (product & M));
    // The first reduction may overflow by 1 bit, so we may need to
    // repeat.  mod == M is not possible; using > allows the faster
    // sign-bit-based test.
    if (seed_ > M) {
      seed_ -= M;
    }
    return seed_;
  }

  // Returns a uniformly distributed value in the range [0..n-1]
  // REQUIRES: n > 0
  uint32_t Uniform(int n) { return Next() % n; }

  // Randomly returns true ~"1/n" of the time, and false otherwise.
  // REQUIRES: n > 0
  bool OneIn(int n) { return (Next() % n) == 0; }

  // Skewed: pick "base" uniformly from range [0,max_log] and then
  // return "base" random bits.  The effect is to pick a number in the
  // range [0,2^max_log-1] with exponential bias towards smaller numbers.
  uint32_t Skewed(int max_log) {
    return Uniform(1 << Uniform(max_log + 1));
  }

  // Returns a Random instance for use by the current thread without
  // additional locking
	static Random* GetTLSInstance() {
		static __thread Random* tls_instance;
		static __thread std::aligned_storage<sizeof(Random)>::type tls_instance_bytes;
		auto rv = tls_instance;
		if (UNLIKELY(rv == nullptr)) {
			size_t seed = std::hash<std::thread::id>()(std::this_thread::get_id());
			rv = new (&tls_instance_bytes) Random((uint32_t)seed);
			tls_instance = rv;
	}
	return rv;
}
};
}/* namespace random */

namespace statistics{
    struct MyStat{
      private:
        uint64_t sum_;
        uint64_t size_;
        const int SCALE = 1000000;
      public:
        MyStat(){
          sum_ = 0;
          size_ = 0;
        }
        void add(double value){
          uint64_t tmp = (uint64_t)(value * SCALE);
          __sync_fetch_and_add(&sum_, tmp);
          __sync_fetch_and_add(&size_, 1);
        }
        double sum(){
          return sum_*1.0/SCALE;
        }
        uint64_t size(){
          return size_;
        }
        double avg(){
          return sum_*1.0/size_/SCALE;
        }
        void reset(){
          size_ = 0.0;
          sum_ = 0.0;
        }
        void print_data(std::string describe){
          if(size_ == 0)
            return;
          printf("%s ", describe.c_str());
          printf("%lu  %lf \n", size_, sum_*1.0/size_/SCALE);
        }
    };

    struct AvgStat{
      private:
        const int NUM = 100;
        uint64_t *sum_ = nullptr;
        uint64_t *size_ = nullptr;
        const double scale = 10000.0;
        const std::string str;
      public:
        AvgStat():str(""){
            sum_ = new uint64_t[NUM];
            size_ = new uint64_t[NUM];
            for(int i=0; i<NUM; i++){
              sum_[i] = 0;
              size_[i] = 0;
            }
        }
        AvgStat(const std::string &s):str(s){
          sum_ = new uint64_t[NUM];
          size_ = new uint64_t[NUM];
          for(int i=0; i<NUM; i++){
            sum_[i] = 0;
            size_[i] = 0;
          }
        }
         ~AvgStat(){
          if(sum_  != nullptr)
            delete sum_;
          if(size_ != nullptr)
            delete size_;
        }
        void add(double value){
          // int i = rand() % NUM;
          int i = random::Random::GetTLSInstance()->Next() % NUM;
          __sync_fetch_and_add(&(sum_[i]), (uint64_t)(value * scale));
          __sync_fetch_and_add(&(size_[i]), 1);
        }
        double avg(){
          double total_sum = 0.0;
          uint64_t total_size = 0;
          for(int i=0; i<NUM; i++){
            total_sum += sum_[i]*1.0/scale;
            total_size += size_[i];
          }
          return total_sum/total_size;
        }
        double sum(){
          double total_sum = 0;
          for(int i=0; i<NUM; i++){
            total_sum += sum_[i] * 1.0 / scale;
          }
          return total_sum;
        }
        uint64_t size(){
          uint64_t total_size = 0;
          for(int i=0; i<NUM; i++){
            total_size += size_[i];
          }
          return total_size;
        }
        void reset(){
          if(size_ == nullptr || sum_ == nullptr)
            return;
          for(int i=0; i<NUM; i++){
            sum_[i] = 0;
            size_[i] = 0;
          }
        }
        void print(std::string s){
          printf("%s: %lf / %ld = %lf\n", 
                s.c_str(), sum(), size(), avg());
        }
        void print(){
          printf("%s: %lf / %ld = %lf\n", 
                str.c_str(), sum(), size(), avg());
        }
    };

    struct MySet{
      private:
        uint64_t size_;
        double *data_;
        std::string describe_;
        const uint64_t max_size = 1ul << 30;
        const uint64_t capacity = max_size * sizeof(double);
        bool sorted;
      public:
        MySet(){
          size_ = 0;
          data_ = (double *) mmap(nullptr, capacity, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
          assert(data_ != nullptr);
          describe_ = "";
        }
        MySet(std::string describe){
          size_ = 0;
          data_ = (double *) mmap(nullptr, capacity, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
          assert(data_ != nullptr);
          describe_ = describe;
          sorted = false;
        }
        ~MySet(){
          munmap(data_, capacity);
        }
        void insert(double value){
          uint64_t index = __sync_fetch_and_add(&size_, 1);
          assert(index < max_size);
          data_[index] = value;
          sorted = false;
        }
        double sum(){
          return std::accumulate(data_, data_ + size_, 0.0);
        }
        double get(uint64_t index){
          assert(index < size_);
          return data_[index];
        }
        double& operator[](uint64_t index){
          assert(index < size_);
          return data_[index];
        }
        uint64_t size(){
          return size_;
        }
        void reset(){
          memset(data_, 0, sizeof(double)*size_);
          size_ = 0;
          sorted = false;
        }
        double get_tail(double f){
          if(!sorted){
            std::sort(data_, data_ + size_);
            sorted = true;
          }
          return data_[(uint64_t)(size_ * f)];
        }
        void print_data(std::string filename){
          if(size_ == 0)
            return ;
          FILE *out = fopen(filename.c_str(), "w");
          for(uint64_t i =0; i<size_; i++){
            fprintf(out, "%lf\n", data_[i]);
          }
          fclose(out);
          std::sort(data_, data_ + size_);
          sorted = true;
          printf("%s: %lf %lu %lf\n", filename.c_str(), sum(), (long int)size_, sum()/size_);
          printf("        p9999: %.2lf, p999: %.2lf, p99: %.2lf, p90: %.2lf\n",data_[uint64_t(size_*0.9999)],
                data_[uint64_t(size_*0.999)],data_[uint64_t(size_*0.99)],data_[uint64_t(size_*0.90)]);
        }
        void deallocate(){
          assert(munmap(data_, capacity) == 0);
        }
    };

    struct MyMap{
      private:
        uint64_t size_;
        double *key_;
        double *value_;
        const uint64_t capacity = 1ul << 33;
      public:
        MyMap(){
          size_ = 0;
          key_ = (double *) mmap(nullptr, capacity, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
          value_ = (double *) mmap(nullptr, capacity, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
          assert(key_ != nullptr && value_ != nullptr);
        }
        ~MyMap(){
          munmap(key_, capacity);
          munmap(value_, capacity);
        }
        void reset(){
          memset(key_, 0, sizeof(double)*size_);
          memset(value_, 0, sizeof(double)*size_);
          size_ = 0;
        }
        void insert(double key, double value){
          uint64_t index = __sync_fetch_and_add(&size_, 1);
          key_[index] = key;
          value_[index] = value;
        }
        void print_data(std::string filename){
          if(size_ == 0)
            return ;
          FILE *out = fopen(filename.c_str(), "w");
          for(uint64_t i =0; i<size_; i++){
            fprintf(out, "%lf %lf\n", key_[i], value_[i]);
          }
          fclose(out);
        }
    };
}/* namespace statistics */

}/* namespace ssdlogging */