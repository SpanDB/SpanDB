/*
 * Copyright (c) 2014 Tim Starling
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <tbb/concurrent_hash_map.h>

#include <atomic>
#include <mutex>
#include <new>
#include <thread>
#include <vector>

#include "ssdlogging/util.h"
#include <cstdlib>

namespace rocksdb {

template <class TKey, class TValue, class THash = tbb::tbb_hash_compare<TKey>>
class HHVMLRUCache: public TopFSCache<TKey, TValue> {
  /**
   * The LRU list node.
   *
   * We make a copy of the key in the list node, allowing us to find the
   * TBB::CHM element from the list node. TBB::CHM invalidates iterators
   * on most operations, even find(), ruling out more efficient
   * implementations.
   */
  struct ListNode {
    ListNode() : m_prev(OutOfListMarker), m_next(nullptr) {}

    ListNode(const TKey& key)
        : m_key(key), m_prev(OutOfListMarker), m_next(nullptr) {}

    TKey m_key;
    ListNode* m_prev;
    ListNode* m_next;

    bool isInList() const { return m_prev != OutOfListMarker; }
  };

  static ListNode* const OutOfListMarker;

  /**
   * The value that we store in the hashtable. The list node is allocated from
   * an internal object_pool. The ListNode* is owned by the list.
   */
  struct HashMapValue {
    HashMapValue() : m_listNode(nullptr) {}

    HashMapValue(const TValue& value, ListNode* node)
        : m_value(value), m_listNode(node) {}

    TValue m_value;
    ListNode* m_listNode;
  };

  typedef tbb::concurrent_hash_map<TKey, HashMapValue, THash> HashMap;
  typedef typename HashMap::const_accessor HashMapConstAccessor;
  typedef typename HashMap::accessor HashMapAccessor;
  typedef typename HashMap::value_type HashMapValuePair;
  typedef std::pair<const TKey, TValue> SnapshotValue;

 public:
  /**
   * The proxy object for TBB::CHM::const_accessor. Provides direct access to
   * the user's value by dereferencing, thus hiding our implementation
   * details.
   */
  struct ConstAccessor {
    ConstAccessor() {}

    const TValue& operator*() const { return *get(); }

    const TValue* operator->() const { return get(); }

    // const TValue* get() const {
    //   return &m_hashAccessor->second.m_value;
    // }
    const TValue get() const { return m_hashAccessor->second.m_value; }

    bool empty() const { return m_hashAccessor.empty(); }

   private:
    friend class HHVMLRUCache;
    HashMapConstAccessor m_hashAccessor;
  };

  /**
   * Create a container with a given maximum size
   */
  explicit HHVMLRUCache(size_t maxSize);

  HHVMLRUCache(const HHVMLRUCache& other) = delete;
  HHVMLRUCache& operator=(const HHVMLRUCache&) = delete;

  ~HHVMLRUCache() { clear(); }

  void ResetStat(){
    insert_lat.reset();
    find_lat.reset();
    delete_lat.reset();
    evict_lat.reset();
  }

  /**
   * Find a value by key, and return it by filling the ConstAccessor, which
   * can be default-constructed. Returns true if the element was found, false
   * otherwise. Updates the eviction list, making the element the
   * most-recently used.
   */
  bool Find(const TKey& key, TValue &value);

  /**
   * Insert a value into the container. Both the key and value will be copied.
   * The new element will put into the eviction list as the most-recently
   * used.
   *
   * If there was already an element in the container with the same key, it
   * will not be updated, and false will be returned. Otherwise, true will be
   * returned.
   */
  bool Insert(const TKey& key, const TValue& value);

  /**
   * Get a snapshot of the keys in the container by copying them into the
   * supplied vector. This will block inserts and prevent LRU updates while it
   * completes. The keys will be inserted in order from most-recently used to
   * least-recently used.
   */
  //void snapshotKeys(std::vector<TKey>& keys);

  /**
   * Get the approximate size of the container. May be slightly too low when
   * insertion is in progress.
   */
  size_t size() const { return m_size.load(); }

  void DeleteKey(const TKey& key);

  void Evict(int required_nodes);

  void Evict();

 private:
  /**
   * Unlink a node from the list. The caller must lock the list mutex while
   * this is called.
   */
  void delink(ListNode* node);

  /**
   * Add a new node to the list in the most-recently used position. The caller
   * must lock the list mutex while this is called.
   */
  void pushFront(ListNode* node);

  /**
   * Evict the least-recently used item from the container. This function does
   * its own locking.
   */
  void internal_evict();


  /**
   * Clear the container. NOT THREAD SAFE -- do not use while other threads
   * are accessing the container.
   */
  void clear();

  /**
   * The maximum number of elements in the container.
   */
  size_t m_maxSize;

  /**
   * This atomic variable is used to signal to all threads whether or not
   * eviction should be done on insert. It is approximately equal to the
   * number of elements in the container.
   */
  std::atomic<size_t> m_size;

  /**
   * The unkederlying TBB hash map.
   */
  HashMap *m_map = nullptr;

  /**
   * The linked list. The "head" is the most-recently used node, and the
   * "tail" is the least-recently used node. The list mutex must be held
   * during both read and write.
   */
  ListNode *m_head = nullptr;
  ListNode *m_tail = nullptr;
  typedef std::mutex ListMutex;
  ListMutex *m_listMutex = nullptr;
  const int SEGNUM = 32;

  ssdlogging::statistics::AvgStat delete_lat;
  ssdlogging::statistics::AvgStat insert_lat;
  ssdlogging::statistics::AvgStat find_lat;
  ssdlogging::statistics::AvgStat evict_lat;
};

template <class TKey, class TValue, class THash>
typename HHVMLRUCache<TKey, TValue, THash>::ListNode* const
    HHVMLRUCache<TKey, TValue, THash>::OutOfListMarker = (ListNode*)-1;

template <class TKey, class TValue, class THash>
HHVMLRUCache<TKey, TValue, THash>::HHVMLRUCache(size_t maxSize)
    : m_maxSize(maxSize),
      m_size(0)
{
  m_map = new HashMap[SEGNUM];
  m_head = new ListNode[SEGNUM];
  m_tail = new ListNode[SEGNUM];
  m_listMutex = new ListMutex[SEGNUM];
  for(int i=0; i<SEGNUM; i++){
    m_head[i].m_prev = nullptr;
    m_head[i].m_next = &m_tail[i];
    m_tail[i].m_prev = &m_head[i];
  }
  printf("Multiple page-level cache:%d\n", SEGNUM);
}

template <class TKey, class TValue, class THash>
bool HHVMLRUCache<TKey, TValue, THash>::Find(const TKey& key, TValue &value){
  auto start = SSDLOGGING_TIME_NOW;
  ConstAccessor ac;
  HashMapConstAccessor& hashAccessor = ac.m_hashAccessor;
  if (!m_map[key%SEGNUM].find(hashAccessor, key)) {
    find_lat.add(SSDLOGGING_TIME_DURATION(start, SSDLOGGING_TIME_NOW));
    return false;
  }
  value = ac.get();
  // Acquire the lock, but don't block if it is already held
  std::unique_lock<ListMutex> lock(m_listMutex[key%SEGNUM], std::try_to_lock);
  if (lock) {
    ListNode* node = hashAccessor->second.m_listNode;
    // The list node may be out of the list if it is in the process of being
    // inserted or evicted. Doing this check allows us to lock the list for
    // shorter periods of time.
    if (node->isInList()) {
      delink(node);
      pushFront(node);
    }
    lock.unlock();
  }
  find_lat.add(SSDLOGGING_TIME_DURATION(start, SSDLOGGING_TIME_NOW));
  return true;
}

template <class TKey, class TValue, class THash>
bool HHVMLRUCache<TKey, TValue, THash>::Insert(const TKey& key,
                                                     const TValue& value) {
  // Insert into the CHM
  auto start = SSDLOGGING_TIME_NOW;
  ListNode* node = new ListNode(key);
  HashMapAccessor hashAccessor;
  HashMapValuePair hashMapValue(key, HashMapValue(value, node));
  if (!m_map[key%SEGNUM].insert(hashAccessor, hashMapValue)) {
    delete node;
    insert_lat.add(SSDLOGGING_TIME_DURATION(start, SSDLOGGING_TIME_NOW));
    return false;
  }

  // Evict if necessary, now that we know the hashmap insertion was successful.
  size_t s = m_size.load();
  bool evictionDone = false;
  if (s >= m_maxSize) {
    // The container is at (or over) capacity, so eviction needs to be done.
    // Do not decrement m_size, since that would cause other threads to
    // inappropriately omit eviction during their own inserts.
    internal_evict();
    evictionDone = true;
  }

  // Note that we have to update the LRU list before we increment m_size, so
  // that other threads don't attempt to evict list items before they even
  // exist.

  std::unique_lock<ListMutex> lock(m_listMutex[key%SEGNUM]);
  pushFront(node);
  lock.unlock();
  if (!evictionDone) {
    s = m_size++;
  }
  if (s > m_maxSize) {
    // It is possible for the size to temporarily exceed the maximum if there is
    // a heavy insert() load, once only as the cache fills. In this situation,
    // we have to be careful not to have every thread simultaneously attempt to
    // evict the extra entries, since we could end up underfilled. Instead we do
    // a compare-and-exchange to acquire an exclusive right to reduce the size
    // to a particular value.
    //
    // We could continue to evict in a loop, but if there are a lot of threads
    // here at the same time, that could lead to spinning. So we will just evict
    // one extra element per insert() until the overfill is rectified.
    if (m_size.compare_exchange_strong(s, s - 1)) {
      internal_evict();
    }
  }
  insert_lat.add(SSDLOGGING_TIME_DURATION(start, SSDLOGGING_TIME_NOW));
  return true;
}

template <class TKey, class TValue, class THash>
void HHVMLRUCache<TKey, TValue, THash>::clear() {
  printf("------------Seg LRU Cache---------------\n");
  printf("delete_lat: %ld %.2lf us\n", delete_lat.size(), delete_lat.avg());
  printf("insert_lat: %ld %.2lf us\n", insert_lat.size(), insert_lat.avg());
  printf("find_lat: %ld %.2lf us\n", find_lat.size(), find_lat.avg());
  printf("evict_lat: %ld %.2lf us\n", evict_lat.size(), evict_lat.avg());
  printf("----------------------------------------\n");
  for(int i=0; i<SEGNUM; i++){
    m_map[i].clear();
    ListNode* node = m_head[i].m_next;
    ListNode* next;
    while (node != &m_tail[i]) {
        next = node->m_next;
        delete node;
        node = next;
    }
    m_head[i].m_next = &m_tail[i];
    m_tail[i].m_prev = &m_head[i];
    m_size = 0;
  }
  /*if(m_map != nullptr){
    delete m_map;
  }
  if(m_head != nullptr){
    delete m_head;
  }
  if(m_tail != nullptr){
    delete m_tail;
  }
  if(m_listMutex != nullptr){
    delete m_listMutex;
  }*/
}

template <class TKey, class TValue, class THash>
inline void HHVMLRUCache<TKey, TValue, THash>::delink(ListNode* node) {
  ListNode* prev = node->m_prev;
  ListNode* next = node->m_next;
  prev->m_next = next;
  next->m_prev = prev;
  node->m_prev = OutOfListMarker;
}

template <class TKey, class TValue, class THash>
inline void HHVMLRUCache<TKey, TValue, THash>::pushFront(ListNode* node) {
  int i = (node->m_key)%SEGNUM;
  ListNode* oldRealHead = m_head[i].m_next;
  node->m_prev = &m_head[i];
  node->m_next = oldRealHead;
  oldRealHead->m_prev = node;
  m_head[i].m_next = node;
}

template <class TKey, class TValue, class THash>
void HHVMLRUCache<TKey, TValue, THash>::internal_evict() {
  int i = ssdlogging::random::Random::GetTLSInstance()->Next() % SEGNUM;
  std::unique_lock<ListMutex> lock(m_listMutex[i]);
  ListNode* moribund = m_tail[i].m_prev;
  if (moribund == &m_head[i]) {
    // List is empty, can't evict
    return;
  }
  delink(moribund);
  lock.unlock();

  HashMapAccessor hashAccessor;
  TKey key = moribund->m_key;
  if (!m_map[key%SEGNUM].find(hashAccessor, key)) {
    // Presumably unreachable
    return;
  }
  m_map[key%SEGNUM].erase(hashAccessor);
  delete moribund;
}

template <class TKey, class TValue, class THash>
void HHVMLRUCache<TKey, TValue, THash>::DeleteKey(const TKey& key) {
  auto start = SSDLOGGING_TIME_NOW;
  HashMapConstAccessor hashAccessor;
  if (!m_map[key%SEGNUM].find(hashAccessor, key)) {
    delete_lat.add(SSDLOGGING_TIME_DURATION(start, SSDLOGGING_TIME_NOW));
    return;
  }
  int i = key%SEGNUM;
  std::unique_lock<ListMutex> lock(m_listMutex[i]);
  ListNode* node = hashAccessor->second.m_listNode;
  if (!node->isInList()) {
    delete_lat.add(SSDLOGGING_TIME_DURATION(start, SSDLOGGING_TIME_NOW));
    return;
  }
  delink(node);
  lock.unlock();
  m_size--;
  m_map[key%SEGNUM].erase(hashAccessor);
  delete node;
  delete_lat.add(SSDLOGGING_TIME_DURATION(start, SSDLOGGING_TIME_NOW));
}

template <class TKey, class TValue, class THash>
void HHVMLRUCache<TKey, TValue, THash>::Evict() {
  auto start = SSDLOGGING_TIME_NOW;
  int i = ssdlogging::random::Random::GetTLSInstance()->Next() % SEGNUM;
  std::unique_lock<ListMutex> lock(m_listMutex[i]);
  ListNode* moribund = m_tail[i].m_prev;
  if (moribund == &m_head[i]) {
    // List is empty, can't evict
    evict_lat.add(SSDLOGGING_TIME_DURATION(start, SSDLOGGING_TIME_NOW));
    return;
  }
  m_size--;
  delink(moribund);
  lock.unlock();

  HashMapAccessor hashAccessor;
  TKey key = moribund->m_key;
  if (!m_map[key%SEGNUM].find(hashAccessor, key)) {
    // Presumably unreachable
    evict_lat.add(SSDLOGGING_TIME_DURATION(start, SSDLOGGING_TIME_NOW));
    return;
  }
  m_map[key%SEGNUM].erase(hashAccessor);
  delete moribund;
  evict_lat.add(SSDLOGGING_TIME_DURATION(start, SSDLOGGING_TIME_NOW));
}

template <class TKey, class TValue, class THash>
void HHVMLRUCache<TKey, TValue, THash>::Evict(int required_nodes) {
  auto start = SSDLOGGING_TIME_NOW;
  if(required_nodes <= 0)
    return;
  //std::unique_lock<ListMutex> lock(m_listMutex);
  int k = ssdlogging::random::Random::GetTLSInstance()->Next() % SEGNUM;
  std::unique_lock<ListMutex> lock(m_listMutex[k], std::try_to_lock);
  if(lock){
    ListNode e_head; // evict list head
    e_head.m_next = nullptr; // initial
    ListNode* moribund;
    int i;
    for(i = 0; i < required_nodes; i++){
      ListNode* temp;
      moribund = m_tail[k].m_prev;
      if (moribund == &m_head[k]) {
          // List is empty, can't evict
          break;
      }
      delink(moribund);
      if(e_head.m_next == nullptr){
          e_head.m_next = moribund;
          moribund->m_next = nullptr;
      }
      else {
          temp = e_head.m_next;
          e_head.m_next = moribund;
          moribund->m_next = temp;
      }
    }
    lock.unlock();
    m_size = m_size - i;
    ListNode* temp = e_head.m_next;
    ListNode* save;
    while(temp!=nullptr) {
      HashMapAccessor hashAccessor;
      TKey key = temp->m_key;
      if (!m_map[key%SEGNUM].find(hashAccessor, key)) {
          // Presumably unreachable
          printf("m_map can not find error!\n");
          return;
      }
      m_map[key%SEGNUM].erase(hashAccessor);
      save = temp->m_next;
      delete temp;
      temp = save;
    }
  }
  evict_lat.add(SSDLOGGING_TIME_DURATION(start, SSDLOGGING_TIME_NOW));
}


}  // namespace tstarling

