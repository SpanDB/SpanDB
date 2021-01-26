#pragma once

#include "topfs_cache.h"
#include "unordered_map"
#include "ssdlogging/util.h"

namespace rocksdb {

template <class TKey, class TValue>
class SegLRUCache: public TopFSCache<TKey, TValue> {
	struct ListNode {
	    ListNode() : prev(nullptr), next(nullptr), valid(true) {}

	    ListNode(const TKey& k, const TValue& v)
	        : key(k), value(v),
	          prev(nullptr), next(nullptr), valid(true) {}
	    
	    bool Valid(){return valid;}

	    TKey key;
	    TValue value;
	    ListNode* prev;
	    ListNode* next;
	    bool valid;
	    
	};

	class LRUCache{
	private:
		uint64_t capacity;
		uint64_t size;
		ListNode *head;
		ListNode *tail;
		std::unordered_map<TKey, ListNode*> hash_map;

		// typedef std::mutex LRUMutex;
		typedef TopFSSpinLock LRUMutex;
		LRUMutex mutex_;

		//ssdlogging::statistics::AvgStat find_lat;

		void DeleteNode(ListNode *node){
			if(node->prev != NULL)
				node->prev->next = node->next;
			else
				head = node->next;
			if(node->next != NULL)
				node->next->prev = node->prev;
			else
				tail = node->prev;
			node->prev = node->next = NULL;
			size--;
		}

		void PushToFront(ListNode *node){
			ListNode *tmp = head;
			node->next = tmp;
			node->prev = NULL;
			head = node;
			if(tmp != NULL)
				tmp->prev = node;
			else
				tail = head;
			size++;
		}

		void InternalEvict(){
			ListNode* node = tail;
			TKey key = node->key;
			DeleteNode(node);
			hash_map.erase(key);
			delete node;
		}

	public:
		LRUCache(uint64_t max_size):
			capacity(max_size),
			size(0),
			head(nullptr),
			tail(nullptr){}
		
		~LRUCache(){
			Clear();
		}

		void Evict(){
			std::unique_lock<LRUMutex> lock(mutex_);
			ListNode* node = tail;
			TKey key = node->key;
			DeleteNode(node);
			hash_map.erase(key);
			delete node;
		}

		bool Find(const TKey& key, TValue& value){
			std::unique_lock<LRUMutex> lock(mutex_);
			//auto start = SSDLOGGING_TIME_NOW;
			auto it = hash_map.find(key);
			if(it == hash_map.end())
				return false;
			if(!it->second->Valid())
				return false;
			ListNode* node = it->second;
			value =  node->value;
			DeleteNode(node);
			PushToFront(node);
			//find_lat.add(SSDLOGGING_TIME_DURATION(start, SSDLOGGING_TIME_NOW));
			return true;
		}

		bool Insert(const TKey& key, const TValue& value){
			std::unique_lock<LRUMutex> lock(mutex_);
			auto it = hash_map.find(key);
			if(it != hash_map.end()){
				ListNode* node = it->second;
				if(node->Valid())
					return false;
				node->value = value;
				DeleteNode(node);
				PushToFront(node);
				return true;
			}
			ListNode* node = new ListNode(key, value);
			hash_map[key] = node;
			PushToFront(node);
			if(size >= capacity)
				InternalEvict();
			return true;
		}

		void DeleteKey(const TKey& key){
			std::unique_lock<LRUMutex> lock(mutex_);
			auto it = hash_map.find(key);
			if(it == hash_map.end())
				return ;
			ListNode* node = it->second;
			DeleteNode(node);
			hash_map.erase(node->key);
			delete node;
		}

		void Clear(){
			//printf("find_lat: %ld %.2lf us\n", find_lat.size(), find_lat.avg());
			std::unique_lock<LRUMutex> lock(mutex_);
			while(tail != nullptr){
				InternalEvict();
			}
		}
	};

private:
	const int SEG_NUM = 40;
	uint64_t capacity;
	LRUCache **cache_list = nullptr;

	int GetLRUIndex(TKey key){
		return key % SEG_NUM;
	}

	ssdlogging::statistics::AvgStat insert_lat;
	ssdlogging::statistics::AvgStat find_lat;
	ssdlogging::statistics::AvgStat delete_lat;
	ssdlogging::statistics::AvgStat evict_lat;
	ssdlogging::statistics::AvgStat test_lat;

public:
	SegLRUCache(uint64_t max_size):capacity(max_size){
		printf("Own LRU Cache: %d\n", SEG_NUM);
		cache_list = new LRUCache* [SEG_NUM];
		for(int i=0; i<SEG_NUM; i++){
			cache_list[i] = new LRUCache(max_size / SEG_NUM);
		}
	}

	bool Insert(const TKey& key, const TValue& value){
		auto start = SSDLOGGING_TIME_NOW;
		int index = GetLRUIndex(key);
		bool res =  cache_list[index]->Insert(key, value);
		insert_lat.add(SSDLOGGING_TIME_DURATION(start, SSDLOGGING_TIME_NOW));
		return res;
	}

	bool Find(const TKey& key, TValue& value){
		auto start = SSDLOGGING_TIME_NOW;
		int index = GetLRUIndex(key);
		bool res = cache_list[index]->Find(key, value);
		find_lat.add(SSDLOGGING_TIME_DURATION(start, SSDLOGGING_TIME_NOW));
		return res;
	}

	void Evict(){
		auto start = SSDLOGGING_TIME_NOW;
		int index = ssdlogging::random::Random::GetTLSInstance()->Next() % SEG_NUM;
		cache_list[index]->Evict();
		evict_lat.add(SSDLOGGING_TIME_DURATION(start, SSDLOGGING_TIME_NOW));
	}

	void Evict(int num){
		for(int i = 0; i<num; i++){
			int index = ssdlogging::random::Random::GetTLSInstance()->Next() % SEG_NUM;
			cache_list[index]->Evict();
		}
	}

	void DeleteKey(const TKey& key){
		auto start = SSDLOGGING_TIME_NOW;
		int index = GetLRUIndex(key);
		cache_list[index]->DeleteKey(key);
		delete_lat.add(SSDLOGGING_TIME_DURATION(start, SSDLOGGING_TIME_NOW));
	}

	void ResetStat(){
		insert_lat.reset();
		find_lat.reset();
		delete_lat.reset();
		evict_lat.reset();
		test_lat.reset();
	}

	virtual ~SegLRUCache(){
		printf("------------Seg LRU Cache---------------\n");
		printf("delete_lat: %ld %.2lf us\n", delete_lat.size(), delete_lat.avg());
		printf("insert_lat: %ld %.2lf us\n", insert_lat.size(), insert_lat.avg());
		printf("find_lat: %ld %.2lf us\n", find_lat.size(), find_lat.avg());
		printf("evict_lat: %ld %.2lf us\n", evict_lat.size(), evict_lat.avg());
		for(int i=0; i<SEG_NUM; i++){
			delete cache_list[i];
		}
		delete cache_list;
		printf("---------------------------------------\n");
	};
};


}