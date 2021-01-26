#pragma once

#include "mutex"

namespace rocksdb {

class TopFSSpinLock {
public:
    TopFSSpinLock() : flag_(false){}
    void lock(){
        bool expect = false;
        while (!flag_.compare_exchange_weak(expect, true)){
            expect = false;
        }
    }
    void unlock(){
        flag_.store(false);
    }

private:
    std::atomic<bool> flag_;
};

template <class TKey, class TValue>
class TopFSCache{
public:
	virtual bool Insert(const TKey& key, const TValue& value)=0;
	virtual bool Find(const TKey& key, TValue& value)=0;
	virtual void Evict() = 0;
	virtual void Evict(int num) = 0;
	virtual void DeleteKey(const TKey& key) = 0;
	virtual void ResetStat() = 0;
	virtual ~TopFSCache() = default;
};


}