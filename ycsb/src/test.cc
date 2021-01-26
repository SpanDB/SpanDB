#include "rocksdb_client.h"
#include "iostream"
#include "cmath"
#include <sys/vfs.h>
#include "rocksdb/table.h"

void ParseCommandLine(int argc, const char *argv[], utils::Properties &props);
void PrintWorkload(const char* filename);

int main(const int argc, const char *argv[]){
	utils::Properties props;
	ParseCommandLine(argc, argv, props);

	ycsbc::CoreWorkload wl;
	wl.Init(props);
	ycsbc::WorkloadProxy wp(&wl);

	const int client_num = stoi(props.GetProperty("client_num"));
	const uint64_t load_num = stoull(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
	const uint64_t requests_num = stoull(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
	const std::string log_dir = props.GetProperty("log_dir");
	const std::string data_dir = props.GetProperty("data_dir");
	const int is_load = stoi(props.GetProperty("is_load"));
	const std::string dbname = props.GetProperty("dbname");
	const std::string db_bak = props.GetProperty("db_bak");

	//===================common-setting==========
	rocksdb::Options options;
	rocksdb::WriteOptions write_options;
	rocksdb::ReadOptions read_options;
	options.allow_concurrent_memtable_write = true;
	options.recycle_log_file_num = false;
	options.allow_2pc = false;
	options.compression = rocksdb::kNoCompression;
	options.max_open_files = 500000;
	options.wal_dir = log_dir;
	write_options.sync = true;
	write_options.disableWAL = false;
	if(is_load == 1){
		write_options.sync = false;
		write_options.disableWAL = true;
		options.error_if_exists = true;
		options.create_if_missing = true;
	}else{
		options.error_if_exists = false;
		options.create_if_missing = false;
	}
	options.statistics = rocksdb::CreateDBStatistics();
	options.max_total_wal_size =  1 * (1ull << 30); // wal size
	options.write_buffer_size = 1 * (1ull << 30);   // write buffer size
	auto env = rocksdb::Env::Default();
	options.env = env;
	options.auto_config = true;
	options.dynamic_moving = true;
	if(dbname == "spandb" && !options.auto_config){
		env->SetBgThreadCores(2, rocksdb::Env::HIGH);
		env->SetBgThreadCores(6, rocksdb::Env::LOW);
	}
	env->SetBackgroundThreads(2, rocksdb::Env::HIGH);
	env->SetBackgroundThreads(6, rocksdb::Env::LOW);
	options.max_background_jobs = 8;
	options.max_subcompactions = 4;
	options.max_write_buffer_number = 4;
	options.topfs_cache_size = 90; //20GB

	if(is_load == 0 || is_load == 1){
		printf("empty the existing data folder\n");
		system(("rm " + data_dir + "/*").c_str());
	}
	
	if(dbname == "rocksdb"){
		printf("empty the existing log folder\n");
		system(("rm " + log_dir + "/*").c_str());
	}
	if(is_load == 0){
		printf("loading database from %s to %s \n", db_bak.c_str(), data_dir.c_str());
		system(("cp " + db_bak + "/*" + " " + data_dir + "/").c_str());
		printf("loading finished\n");
	}

  	//===================DB=======================================
  	printf("dbname: %s\n", dbname.c_str());
  	const int async_num = 50;
	if(dbname == "rocksdb"){
		options.auto_config = false;
	}else if(dbname == "spandb"){
		int core_num = 40;
		if(core_num > sysconf(_SC_NPROCESSORS_ONLN))
			core_num = sysconf(_SC_NPROCESSORS_ONLN);
		std::string pcie_addr = "trtype:PCIe " + options.wal_dir;
		options.wal_dir = data_dir;
		options.enable_spdklogging = true;
		options.ssdlogging_type = "spdk";
		options.spdk_recovery = false;
		options.wal_dir = data_dir;
		options.lo_path = data_dir;
		options.max_level = 4;
		options.l0_queue_num = 20;
		options.max_compaction_bytes = 64ull<<20;
		options.ssdlogging_path = pcie_addr;
		options.max_read_que_length = 2;
		options.ssdlogging_num = 6;
		options.logging_server_num = 1;
		options.lo_env = rocksdb::NewSpdkEnv(rocksdb::Env::Default(), pcie_addr, options, is_load);
		options.spandb_worker_num = core_num - env->GetBgThreadCores(rocksdb::Env::HIGH) 
		 							   		 - env->GetBgThreadCores(rocksdb::Env::LOW)
		 							   		 - client_num;
	}else{
		printf("Please choose the correct db (rocksdb or spandb)\n");
		exit(0);
	}

	{
		ycsbc::RocksDBClient rocksdb_client(&wp, options, write_options, read_options, data_dir, client_num,
					  load_num, client_num, requests_num, async_num, is_load);
		system("sync;echo 3 > /proc/sys/vm/drop_caches");
		fflush(stdout);
		printf("--------------memory usage----------------\n");
		fflush(stdout);
		system("free -h");
		fflush(stdout);
		printf("------------------------------------------\n");
		fflush(stdout);
		if(is_load == 1){
			rocksdb_client.Load();
			std::this_thread::sleep_for(std::chrono::seconds(180));
		}else{
			rocksdb_client.Warmup();
			rocksdb_client.Work();
		}
	}
	if(dbname == "spandb"){
		delete options.lo_env;
	}
	fflush(stdout);
	return 0;
}

void ParseCommandLine(int argc, const char *argv[], utils::Properties &props) {
	if(argc != 8){
		printf("usage: <workload_file> <client_num> <data_dir> <log_dir> <is_load> <dbname> <db_bak>\n");
		exit(0);
	}
	// workload file
	std::ifstream input(argv[1]);
	try {
		props.Load(input);
	} catch (const std::string &message) {
		printf("%s\n", message.c_str());
		exit(0);
	}
	input.close();
	PrintWorkload(argv[1]);
	props.SetProperty("client_num", argv[2]);
	props.SetProperty("data_dir", argv[3]);
	props.SetProperty("log_dir", argv[4]);
	props.SetProperty("is_load", argv[5]);
	props.SetProperty("dbname", argv[6]);
	props.SetProperty("db_bak", argv[7]);
}

void PrintWorkload(const char* filename){
	FILE *file = fopen(filename, "r");
	char line[201];
	fgets(line,200,file);
	printf("==================Workload=================\n");
	printf("%s\n", filename);
	while(!feof(file)){
		std::string s = std::string(line);
		if(s.find("#") != 0 && s != "\n" && s!=""){
			printf("%s", s.c_str());
		}
		fgets(line,200,file);
	}
	fclose(file);
	printf("==========================================\n");
	fflush(stdout);
}