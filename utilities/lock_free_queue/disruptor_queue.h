#pragma once

#include "port/port.h"
#include "util/mutexlock.h"
#include "disruptor_atomic_sequence.h"

// #include "ssdlogging/spdk_logging_server.h"

namespace rocksdb {
 
	constexpr int64_t DefaultRingBufferSize = 1024;
 
	template<class ValueType, int64_t N = DefaultRingBufferSize>
	class DisruptorQueue{
	  public:
		DisruptorQueue() : _lastRead(-1L) , _lastWrote(-1L), _stopWorking(0L), _lastDispatch(-1L), _writableSeq(0L) {};
		~DisruptorQueue() {};
 
		DisruptorQueue(const DisruptorQueue&) = delete;
		DisruptorQueue(const DisruptorQueue&&) = delete;
		void operator=(const DisruptorQueue&) = delete;
 
		static_assert(((N > 0) && ((N& (~N + 1)) == N)),
			"RingBuffer's size must be a positive power of 2");
 
		void WriteInBuf(ValueType&& val){
			const int64_t writableSeq = _writableSeq.fetch_add(1);
			while (writableSeq - _lastRead.load() > N){
				if (_stopWorking.load())
					throw std::runtime_error("writting when stopped disruptor queue");
				//std::this_thread::yield();
			}
			_ringBuf[writableSeq & (N - 1)] = val;
 
			while (writableSeq - 1 != _lastWrote.load()){

			}
			_lastWrote.store(writableSeq);
		};
 
		void WriteInBuf(ValueType& val){
			const int64_t writableSeq = _writableSeq.fetch_add(1);
			while (writableSeq - _lastRead.load() > N){
				if (_stopWorking.load())
					throw std::runtime_error("writting when stopped disruptor queue");
				//std::this_thread::yield();
			}
			_ringBuf[writableSeq & (N - 1)] = val;
 
			while (writableSeq - 1 != _lastWrote.load()){

			}
			_lastWrote.store(writableSeq);
		};
 
		int64_t GetReadableSeq(){
			const int64_t readableSeq = _lastDispatch.fetch_add(1) + 1;
			while (readableSeq > _lastWrote.load()){
				if (_stopWorking.load() && empty()){
					return -1L;
				}
			}
			return readableSeq;
		};
 
		ValueType& ReadFromBuf(const int64_t readableSeq)
		{
			if (readableSeq < 0){
				throw("error : incorrect seq for ring Buffer when ReadFromBuf(seq)!");
			}
			return _ringBuf[readableSeq & (N - 1)];
		}
 
		void FinishReading(const int64_t seq){
			if (seq < 0){
				return;
			}
			while (seq - 1 != _lastRead.load()){

			}
			_lastRead.store(seq);
		};

		//add
		ValueType ReadValue(){
			const int64_t seq = GetReadableSeq();
			if(seq == -1)
				return nullptr;
			ValueType val = ReadFromBuf(seq);
			FinishReading(seq);
			return val;
		}

		ValueType ReadWithLock(){
			int64_t readableSeq = -1;
			{
				MutexLock lock(&mutex_);
				readableSeq = _lastDispatch.load() + 1;
				if(readableSeq > _lastWrote.load()){
					return nullptr;
				}
				_lastDispatch.fetch_add(1);
			}
			assert(readableSeq >= 0);
			ValueType value =  _ringBuf[readableSeq & (N - 1)];
			while (readableSeq - 1 != _lastRead.load()) ;
			_lastRead.store(readableSeq);
			return value;
		}

		int64_t Length(){
			return _lastWrote.load() - _lastRead.load();
		}
		int64_t Left(){
			return _lastWrote.load() - _lastDispatch.load();
		}
		int64_t AsyncGetNextReadableSeq(){
			if(_lastDispatch.load() >= _writableSeq.load())
				return -1;
			return _lastDispatch.fetch_add(1) + 1;
		};
		bool AsyncFinishReading(const int64_t seq){
			if (seq < 0){
				assert(0);
				throw("error : incorrect seq for AsyncFinishReading!");
			}
			if (seq - 1 != _lastRead.load())
				return false;
			assert(seq - 1 == _lastRead.load());
			_lastRead.store(seq);
			return true;
		}
		ValueType AsyncRead(const int64_t readableSeq){
			if (readableSeq < 0){
				assert(0);
				throw("error : incorrect seq for AsyncFinishReading!");
			}
			if(readableSeq > _lastWrote.load()){
				return nullptr;
			}
			ValueType value = _ringBuf[readableSeq & (N - 1)];
			//FinishReading(readableSeq);
			return value;
		}
		bool IsReadable(int64_t readableSeq){
			return readableSeq <= _lastWrote.load();
		}
		//end

		bool empty(){
			return _writableSeq.load() - _lastRead.load() == 1;
		}
 
		void stop(){
			_stopWorking.store(1L);
		}

		int64_t max_size(){
			return N;
		}
 
	  private:
		AtomicSequence _lastRead;
 
		AtomicSequence _lastWrote;
 
		AtomicSequence _stopWorking;
 
		AtomicSequence _lastDispatch;
 
		AtomicSequence _writableSeq;
 
		std::array<ValueType, N> _ringBuf;

		port::Mutex mutex_;
	};
}