#include "rocksdb_client.h"
#include "algorithm"
#include "math.h"

namespace ycsbc{

void RocksDBClient::Load(){
	Reset();

	assert(request_time_ == nullptr);
	int base_coreid = options_.spandb_worker_num + 
					  options_.env->GetBgThreadCores(rocksdb::Env::LOW) + 
					  options_.env->GetBgThreadCores(rocksdb::Env::HIGH);

	assert(!options_.enable_spdklogging);
	uint64_t num = load_num_ / loader_threads_;
	std::vector<std::thread> threads;
	auto fn = std::bind(&RocksDBClient::RocksdDBLoader, this, 
						std::placeholders::_1, std::placeholders::_2);
	auto start = TIME_NOW;
	for(int i=0; i<loader_threads_; i++){
		if(i == loader_threads_ - 1)
			num = num + load_num_ % loader_threads_;
		threads.emplace_back(fn, num, (base_coreid + i));
	}
	for(auto &t : threads)
		t.join();
	double time = TIME_DURATION(start, TIME_NOW);

	printf("==================================================================\n");
	PrintArgs();
	printf("Load %ld requests in %.3lf seconds.\n", load_num_, time/1000/1000);
	printf("==================================================================\n");
	std::this_thread::sleep_for(std::chrono::seconds(180));
	std::string stat_str2;
 	db_->GetProperty("rocksdb.stats", &stat_str2);
 	printf("\n%s\n", stat_str2.c_str());
}

void RocksDBClient::Work(){
	Reset();
	assert(request_time_ == nullptr);
	assert(read_time_ == nullptr);
	assert(update_time_ == nullptr);
	request_time_ = new TimeRecord(request_num_ + 1);
	read_time_ = new TimeRecord(request_num_ + 1);
	update_time_ = new TimeRecord(request_num_ + 1);

	int base_coreid = options_.spandb_worker_num + 
					  options_.env->GetBgThreadCores(rocksdb::Env::LOW) + 
					  options_.env->GetBgThreadCores(rocksdb::Env::HIGH);

	if(workload_wrapper_ == nullptr){
		workload_wrapper_ = new WorkloadWrapper(workload_proxy_, request_num_, false);
	}

	uint64_t num = request_num_ / worker_threads_;
	std::vector<std::thread> threads;
	std::function< void(uint64_t, int, bool, bool)> fn;
	if(options_.enable_spdklogging){
		fn = std::bind(&RocksDBClient::SpanDBWorker, this, 
						std::placeholders::_1, std::placeholders::_2,
						std::placeholders::_3, std::placeholders::_4);
	}else{
		fn = std::bind(&RocksDBClient::RocksDBWorker, this, 
		   			    std::placeholders::_1, std::placeholders::_2,
		   			    std::placeholders::_3, std::placeholders::_4);
	}
	printf("start time: %s\n", GetDayTime().c_str());
	auto start = TIME_NOW;
	for(int i=0; i<worker_threads_; i++){
		if(i == worker_threads_ - 1)
			num = num + request_num_ % worker_threads_;
		threads.emplace_back(fn, num, (base_coreid + i), false, i==0);
	}
	for(auto &t : threads)
		t.join();
	double time = TIME_DURATION(start, TIME_NOW);
	printf("end time: %s\n", GetDayTime().c_str());

	std::string stat_str2;
	db_->GetProperty("rocksdb.stats", &stat_str2);
	printf("\n%s\n", stat_str2.c_str());
	fflush(stdout);

	assert(request_time_->Size() == request_num_);
	printf("==================================================================\n");
	PrintArgs();
	printf("WAL sync time per request: %.3lf us\n", wal_time_/request_num_);
	printf("WAL sync time per sync: %.3lf us\n", wal_time_/
		   							options_.statistics->getTickerCount(rocksdb::WAL_FILE_SYNCED));
	printf("Wait time: %.3lf us\n", wait_time_/request_num_);
	printf("Complete wait time: %.3lf us\n", complete_memtable_time_/request_num_);
	printf("Write delay time: %.3lf us\n", write_delay_time_/request_num_);
	printf("Write memtable time: %.3lf\n", write_memtable_time_/request_num_);
	printf("Finish %ld requests in %.3lf seconds.\n", request_num_, time/1000/1000);
	if(read_time_->Size() != 0){
		printf("read num: %ld, read avg latency: %.3lf us, read median latency: %.3lf us\n", 
				read_time_->Size(), read_time_->Sum()/read_time_->Size(), read_time_->Tail(0.50));
		printf("read P999: %.3lf us, P99: %.3lf us, P95: %.3lf us, P90: %.3lf us, P75: %.3lf us\n",
				read_time_->Tail(0.999), read_time_->Tail(0.99), read_time_->Tail(0.95),
				read_time_->Tail(0.90), read_time_->Tail(0.75));
	}else{
		printf("read num: 0, read avg latency: 0 us, read median latency: 0 us\n");
		printf("read P999: 0 us, P99: 0 us, P95: 0 us, P90: 0 us, P75: 0 us\n");
	}
	if(update_time_->Size() != 0){
		printf("update num: %ld, update avg latency: %.3lf us, update median latency: %.3lf us\n", 
			    update_time_->Size(), update_time_->Sum()/update_time_->Size(), update_time_->Tail(0.50));
		printf("update P999: %.3lf us, P99: %.3lf us, P95: %.3lfus, P90: %.3lf us, P75: %.3lf us\n",
				update_time_->Tail(0.999), update_time_->Tail(0.99), update_time_->Tail(0.95),
				update_time_->Tail(0.90), update_time_->Tail(0.75));
	}else{
		printf("update num: 0, update avg latency: 0 us, update median latency: 0 us\n");
		printf("update P999: 0 us, P99: 0 us, P95: 0 us, P90: 0 us, P75: 0 us\n");
	}
	printf("Work latency: %.3lf us\n", request_time_->Sum()/request_time_->Size());
	printf("Work IOPS: %.3lf K\n", request_num_/time*1000*1000/1000);
	printf("Work median latency: %.3lf us\n", request_time_->Tail(0.5));
	printf("Work P999: %.3lfus, P99: %.3lfus, P95: %.3lfus, P90: %.3lfus, P75: %.3lfus\n",
			request_time_->Tail(0.999), request_time_->Tail(0.99), request_time_->Tail(0.95),
		    request_time_->Tail(0.90), request_time_->Tail(0.75));
	printf("Stall: %.3lf us\n", options_.statistics->getTickerCount(rocksdb::STALL_MICROS)*1.0);
	printf("Stall rate: %.3lf \n", options_.statistics->getTickerCount(rocksdb::STALL_MICROS)*1.0/time);
	printf("Block read time: %.3lf us\n", block_read_time_/read_time_->Size());
	uint64_t block_hit = options_.statistics->getTickerCount(rocksdb::BLOCK_CACHE_HIT);
	uint64_t block_miss = options_.statistics->getTickerCount(rocksdb::BLOCK_CACHE_MISS);
	uint64_t memtable_hit = options_.statistics->getTickerCount(rocksdb::MEMTABLE_HIT);
	uint64_t memtable_miss = options_.statistics->getTickerCount(rocksdb::MEMTABLE_MISS);
	printf("block cache hit ratio: %.3lf (hit: %ld, miss: %ld)\n", 
			block_hit*1.0/(block_hit+block_miss), block_hit, block_miss);
	printf("memtable hit ratio: %.3lf (hit: %ld, miss: %ld)\n",
		   memtable_hit*1.0/(memtable_hit+memtable_miss), memtable_hit, memtable_miss);
	printf("submit_time: %.3lf\n", submit_time_ / 1000.0 / request_num_);
	printf("==================================================================\n");
	fflush(stdout);
}

void RocksDBClient::Warmup(){
	int base_coreid = options_.spandb_worker_num + 
					  options_.env->GetBgThreadCores(rocksdb::Env::LOW) + 
					  options_.env->GetBgThreadCores(rocksdb::Env::HIGH);
	Reset();
	if(workload_wrapper_ == nullptr){
		workload_wrapper_ = new WorkloadWrapper(workload_proxy_, (uint64_t)request_num_ * (1+warmup_rate_) + 1, false);
	}

	auto start = TIME_NOW;
	const uint64_t warmup_num = floor(request_num_ * warmup_rate_);
	const uint64_t num = warmup_num / worker_threads_;
	printf("Start warmup (%ld)...\n", num*worker_threads_);
	std::vector<std::thread> threads;
	std::function< void(uint64_t, int, bool, bool)> fn;
	printf("warmup start: %s\n", GetDayTime().c_str());
	if(options_.enable_spdklogging){
		fn = std::bind(&RocksDBClient::SpanDBWorker, this, 
						std::placeholders::_1, std::placeholders::_2,
						std::placeholders::_3, std::placeholders::_3);
	}else{
		fn = std::bind(&RocksDBClient::RocksDBWorker, this, 
		   			    std::placeholders::_1, std::placeholders::_2,
		   			    std::placeholders::_3, std::placeholders::_4);
	}
	for(int i=0; i<worker_threads_; i++){
		threads.emplace_back(fn, num, base_coreid + i, true, i==0);
	}
	for(auto &t : threads)
		t.join();
	double time = TIME_DURATION(start, TIME_NOW);
	printf("warmup finish: %s\n", GetDayTime().c_str());
	printf("Warmup complete %ld requests in %.3lf seconds.\n", num*worker_threads_, time/1000/1000);
}

void RocksDBClient::RocksDBWorker(uint64_t num, int coreid, bool is_warmup, bool is_master){
	// SetAffinity(coreid);
	rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTimeExceptForMutex);
	rocksdb::get_perf_context()->Reset();
	rocksdb::get_iostats_context()->Reset();

	TimeRecord request_time(num + 1);
	TimeRecord read_time(num + 1);
	TimeRecord update_time(num + 1);

	if(!is_warmup && is_master){
		printf("starting requests...\n");
	}

	std::string w_value(1024, 'a');
	std::string r_value;
	for(uint64_t i=0; i<num; i++){
		WorkloadWrapper::Request *req = workload_wrapper_->GetNextRequest();
		ycsbc::Operation opt = req->Type();
		assert(req != nullptr);
		auto start = TIME_NOW;
		if(opt == READ){
			// db_->Get(read_options_, req->Key(), &r_value);
			ERR(db_->Get(read_options_, req->Key(), &r_value));
		}else if(opt == UPDATE){
			ERR(db_->Put(write_options_, req->Key(), /*std::string(req->Length(), 'a')*/ w_value));
		}else if(opt == INSERT){
			ERR(db_->Put(write_options_, req->Key(), /*std::string(req->Length(), 'a')*/ w_value));
		}else if(opt == READMODIFYWRITE){
			// db_->Get(read_options_, req->Key(), &r_value);
			ERR(db_->Get(read_options_, req->Key(), &r_value));
			ERR(db_->Put(write_options_, req->Key(), w_value));
		}else if(opt == SCAN){
			rocksdb::Iterator* iter = db_->NewIterator(read_options_);
			iter->Seek(req->Key());
			for (int i = 0; i < req->Length() && iter->Valid(); i++) {
				// Do something with it->key() and it->value().
        		iter->Next();
    		}
    		ERR(iter->status());
    		delete iter;
		}else{
			throw utils::Exception("Operation request is not recognized!");
		}
		double time =  TIME_DURATION(start, TIME_NOW);
		request_time.Insert(time);
		if(opt == READ || opt == SCAN){
			read_time.Insert(time);
			total_read_latency.fetch_add((uint64_t)time);
			read_finished.fetch_add(1);
		}else if(opt == UPDATE || opt == INSERT || opt == READMODIFYWRITE){
			update_time.Insert(time);
			total_write_latency.fetch_add((uint64_t)time);
			write_finished.fetch_add(1);
		}else{
			assert(0);
		}
		total_finished_requests_.fetch_add(1);
	}

	if(is_warmup)
		return ;

	mutex_.lock();
	request_time_->Join(&request_time);
	read_time_->Join(&read_time);
	update_time_->Join(&update_time);
	wal_time_ += rocksdb::get_perf_context()->write_wal_time/1000.0;
	wait_time_ += rocksdb::get_perf_context()->write_thread_wait_nanos/1000.0;
	complete_memtable_time_ += rocksdb::get_perf_context()->complete_parallel_memtable_time/1000.0;
	write_delay_time_ += rocksdb::get_perf_context()->write_delay_time/1000.0;
	block_read_time_ += rocksdb::get_perf_context()->block_read_time/1000.0;
	write_memtable_time_ += rocksdb::get_perf_context()->write_memtable_time/1000.0;
	//printf("%s\n\n",rocksdb::get_perf_context()->ToString().c_str());
	//printf("%s\n\n",rocksdb::get_iostats_context()->ToString().c_str());
	mutex_.unlock();
}

void RocksDBClient::SpanDBWorker(uint64_t num, int coreid, bool is_warmup, bool is_master){
	SetAffinity(coreid);
	rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTimeExceptForMutex);
	rocksdb::get_perf_context()->Reset();
	rocksdb::get_iostats_context()->Reset();

	TimeRecord request_time(num + 1);
	TimeRecord read_time(num + 1);
	TimeRecord update_time(num + 1);

	//1. prepare pipeline
	TimePoint *senttime = new TimePoint[async_num_];
	// rocksdb::Status **status = new rocksdb::Status *[async_num_];
	std::atomic<rocksdb::Status *> *status = new std::atomic<rocksdb::Status *>[async_num_];
	bool *occupied = new bool[async_num_];
	std::string *return_values = new std::string[async_num_];
	WorkloadWrapper::Request **requests = new WorkloadWrapper::Request*[async_num_];
	for(int i=0; i<async_num_; i++){
		status[i].store(nullptr);
		occupied[i] = false;
		requests[i] = nullptr;
	}
	uint64_t k=0, j=0;
	WorkloadWrapper::Request *next_req = nullptr;
	std::string w_value(1024, 'a');
	rocksdb::Status s;

	if(!is_warmup && is_master){
		printf("starting requests...\n"); 
	}

	//2. start
	while(true){
		//send a request
		if(k < num && next_req == nullptr){
			next_req = workload_wrapper_->GetNextRequest();
			k++;
		}
		for(int i=0; i<async_num_; i++){
			if(!occupied[i]){
				if(LIKELY(k <= num && next_req != nullptr)){
					assert(requests[i] == nullptr);
					assert(status[i].load() == nullptr);
					senttime[i] = TIME_NOW;
					if(next_req->Type() == READ){
						s = db_->AsyncGet(read_options_, next_req->Key(), &(return_values[i]), status[i]);
					}else if(next_req->Type() == UPDATE){
						s = db_->AsyncPut(write_options_, next_req->Key(), w_value, status[i]);
					}else if(next_req->Type() == INSERT){
						s = db_->AsyncPut(write_options_, next_req->Key(), w_value, status[i]);
					}else if(next_req->Type() == SCAN){
						s = db_->AsyncScan(read_options_, next_req->Key(), &(return_values[i]), next_req->Length(), status[i]);
					}else if(next_req->Type() == READMODIFYWRITE){
						s = db_->AsyncGet(read_options_, next_req->Key(), &(return_values[i]), status[i]);
					}else{
						throw utils::Exception("Operation request is not recognized!");
					}
					if(UNLIKELY(!s.ok())){
						if(status[i].load() != nullptr)
                        	delete status[i].load();
                        status[i].store(nullptr);
                    }else{
                    	requests[i] = next_req;
                    	occupied[i] = true;
                    	next_req = nullptr;
                    }
				}
				break;
			}
		}
		//check completion
		bool finished = (next_req == nullptr);
		for(int i=0; i<async_num_; i++){
			if(occupied[i]){
				if(UNLIKELY(status[i].load() != nullptr)){
					if(!status[i].load()->ok()){ 
                        fprintf(stderr,"%s: %d: %s\n", __FILE__, __LINE__, status[i].load()->ToString().c_str());
                        assert(false);
                    }
                    assert(requests[i] != nullptr);
                    if(requests[i]->Type() == READMODIFYWRITE){
                    	// delete  status[i].load();
                    	status[i].store(nullptr);
                    	ERR(db_->AsyncPut(write_options_, requests[i]->Key(), w_value, status[i]));
                    	requests[i]->SetType(UPDATE);
                    	finished = false;
                    	continue;
                    }
                    double time = TIME_DURATION(senttime[i], TIME_NOW);
                    request_time.Insert(time);
                    if(requests[i]->Type() == READ || requests[i]->Type() == SCAN){
                    	read_time.Insert(time);
                    	read_finished.fetch_add(1);
                    	total_read_latency.fetch_add((uint64_t)time);
                    }else if(requests[i]->Type() == UPDATE || requests[i]->Type() == INSERT){
                    	update_time.Insert(time);
                    	write_finished.fetch_add(1);
                    	total_write_latency.fetch_add((uint64_t)time);
                    }else{
                    	assert(0);
                    }

                    requests[i] = nullptr;
                    //delete status[i];
                    status[i].store(nullptr);
                    occupied[i] = false;
                    j++;
				}
				finished = false;
			}
		}
		//finish all requests
		if(UNLIKELY(finished && k == num))
			break;
	}
	assert(k==num);
	assert(j==num);

	if(is_warmup)
		return;
	mutex_.lock();
	request_time_->Join(&request_time);
	read_time_->Join(&read_time);
	update_time_->Join(&update_time);
	wal_time_ += rocksdb::get_perf_context()->write_wal_time/1000.0;
	wait_time_ += rocksdb::get_perf_context()->write_thread_wait_nanos/1000.0;
	complete_memtable_time_ += rocksdb::get_perf_context()->complete_parallel_memtable_time/1000.0;
	write_delay_time_ += rocksdb::get_perf_context()->write_delay_time/1000.0;
	block_read_time_ += rocksdb::get_perf_context()->block_read_time/1000.0;
	write_memtable_time_ += rocksdb::get_perf_context()->write_memtable_time/1000.0;
	//printf("%s\n\n",rocksdb::get_perf_context()->ToString().c_str());
	//printf("%s\n\n",rocksdb::get_iostats_context()->ToString().c_str());
	mutex_.unlock();
}

void RocksDBClient::RocksdDBLoader(uint64_t num, int coreid){
	rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableTimeExceptForMutex);
	rocksdb::get_perf_context()->Reset();
	rocksdb::get_iostats_context()->Reset();
	for(uint64_t i=0; i<num; i++){		
		std::string table;
		std::string key;
		std::vector<ycsbc::CoreWorkload::KVPair> values;
		workload_proxy_->LoadInsertArgs(table, key, values);
		assert(values.size() == 1);
		for (ycsbc::CoreWorkload::KVPair &field_pair : values) {
			std::string value = field_pair.second;
			ERR(db_->Put(write_options_, key, value));
		}
	}
	mutex_.lock();
	wal_time_ += rocksdb::get_perf_context()->write_wal_time/1000.0;
	wait_time_ += rocksdb::get_perf_context()->write_thread_wait_nanos/1000.0;
	complete_memtable_time_ += rocksdb::get_perf_context()->complete_parallel_memtable_time/1000.0;
	write_delay_time_ += rocksdb::get_perf_context()->write_delay_time/1000.0;
	write_memtable_time_ += rocksdb::get_perf_context()->write_memtable_time/1000.0;
	mutex_.unlock();
}

void RocksDBClient::Reset(){
	options_.statistics->Reset();
	options_.lo_env->ResetStat();
	db_->ResetStats();
	wait_time_ = wal_time_ = 0;
	complete_memtable_time_ = 0;
	block_read_time_ = 0;
	write_delay_time_ = 0;
	write_memtable_time_ = 0;
	submit_time_ = 0;
	total_finished_requests_.store(0);
}

void RocksDBClient::PrintArgs(){
	printf("-----------configuration------------\n");
	if(options_.ssdlogging_type != "spdk"){
		printf("Clients: %d\n", worker_threads_);
	}else{
		printf("Clients: %d * %d\n", worker_threads_, async_num_);
		printf("Loggers: %d (%d)\n", options_.logging_server_num, options_.ssdlogging_num);
		printf("Workers: %d\n", options_.spandb_worker_num);
	}
	printf("Auto configure: %s\n", options_.auto_config ? "true" : "false");
	printf("Dynamic moving: %s\n", options_.dynamic_moving ? "true" : "false");
	printf("Max read queue: %d\n", options_.max_read_que_length);
	printf("WAL: %d, fsync: %d\n", !(write_options_.disableWAL), write_options_.sync);
	printf("Max level: %d\n", options_.max_level);
	printf("Data: %s\n", data_dir_.c_str());
	if(write_options_.disableWAL){
		printf("WAL: nologging\n");
	}else if(options_.ssdlogging_type == "spdk"){
		printf("WAL: %s\n", options_.ssdlogging_path.c_str());
	}else{
		printf("WAL: %s\n", options_.wal_dir.c_str());
	}
	printf("Max_write_buffer_number: %d\n", options_.max_write_buffer_number);
	printf("Max_background_jobs: %d\n", options_.max_background_jobs);
	printf("High-priority backgd threds: %d\n", options_.env->GetBackgroundThreads(rocksdb::Env::HIGH));
	printf("Low-priority backgd threds: %d\n", options_.env->GetBackgroundThreads(rocksdb::Env::LOW));
	printf("Max_subcompactions: %d\n", options_.max_subcompactions);
	if(options_.write_buffer_size >= (1ull << 30)){
		printf("Write_buffer_size: %.3lf GB\n", options_.write_buffer_size * 1.0/(1ull << 30));
	}else{
		printf("Write_buffer_size: %.3lf MB\n", options_.write_buffer_size * 1.0/(1ull << 20));
	}
	printf("-------------------------------------\n");
	printf("write done by self: %ld\n", options_.statistics->getTickerCount(rocksdb::WRITE_DONE_BY_SELF));
	printf("WAL sync num: %ld\n", options_.statistics->getTickerCount(rocksdb::WAL_FILE_SYNCED));
	printf("WAL write: %.3lf GB\n", options_.statistics->getTickerCount(rocksdb::WAL_FILE_BYTES)*1.0/(1ull << 30));
	printf("compaction read: %.3lf GB, compaction write: %.3lf GB\n",
			options_.statistics->getTickerCount(rocksdb::COMPACT_READ_BYTES)*1.0/(1ull << 30),
			options_.statistics->getTickerCount(rocksdb::COMPACT_WRITE_BYTES)*1.0/(1ull << 30));
	printf("flush write: %.3lf GB\n",
			options_.statistics->getTickerCount(rocksdb::FLUSH_WRITE_BYTES)*1.0/(1ull << 30));
	printf("flush time: %.3lf s\n",
			options_.statistics->getTickerCount(rocksdb::FLUSH_TIME)*1.0/1000000);
	fflush(stdout);
}

}

