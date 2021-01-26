#pragma once

#include "atomic"

namespace rocksdb {

	constexpr size_t DefaultRingBufferSize = 256;
 
	template<class ValueType, size_t N = DefaultRingBufferSize>
	class ArrayQueue{

        private:
            enum State : uint8_t {
                FREE = 1,
                WRITING = 2,
                WRITTEN = 4,
                READING = 8
            };	

            struct Entry{
                ValueType data;
                std::atomic<uint8_t> state;
            };

            std::atomic<std::uint64_t> next_write_;
            std::atomic<std::uint64_t> next_read_;
            std::atomic<bool> stopWorking_;
            Entry *buffer_;

	  public:
		ArrayQueue() : next_write_(0), next_read_(0), stopWorking_(false), buffer_(nullptr){
            buffer_ = (Entry *) malloc(sizeof(Entry) * N);
            for(size_t i=0; i<N; i++){
                buffer_[i].state = FREE;
            }
        };

        ~ArrayQueue() { 
            if(buffer_ != nullptr){
                free(buffer_);
            }
        }
 
		ArrayQueue(const ArrayQueue&) = delete;
		ArrayQueue(const ArrayQueue&&) = delete;
		void operator=(const ArrayQueue&) = delete;
 
		static_assert(((N > 0) && ((N& (~N + 1)) == N)),
			"RingBuffer's size must be a positive power of 2");
 
		void Enqueue(ValueType val){
			const int64_t writableSeq = next_write_.fetch_add(1);
            while(buffer_[writableSeq & (N - 1)].state.load() != FREE){

            };
            assert(buffer_[writableSeq & (N - 1)].state.load() != FREE);
            uint8_t state = buffer_[writableSeq & (N - 1)].state.load(std::memory_order_acquire);
            while(buffer_[writableSeq & (N - 1)].state.compare_exchange_strong(state, LoggingServer::STATE_LOCKED_WAITING)){

            }

			while (writableSeq - _lastRead.load() > N){
				if (stopWorking_.load())
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
		int64_t Length(){
			return _lastWrote.load() - _lastRead.load();
		}
		int64_t AsyncGetNextReadableSeq(){
			return _lastDispatch.fetch_add(1) + 1;
		};
		ValueType AsyncRead(const int64_t readableSeq){
			if (readableSeq < 0){
				throw("error : incorrect seq for ring Buffer when ReadFromBuf(seq)!");
			}
			if(readableSeq > _lastWrote.load()){
				return nullptr;
			}
			if(readableSeq -1 != _lastRead.load()){
				return nullptr;
			}
			ValueType value = _ringBuf[readableSeq & (N - 1)];
			FinishReading(readableSeq);
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
 
	  private:
		AtomicSequence _lastRead;
 
		AtomicSequence _lastWrote;
 
		AtomicSequence _stopWorking;
 
		AtomicSequence _lastDispatch;
 
		AtomicSequence _writableSeq;
 
		std::array<ValueType, N> _ringBuf;
	};
}