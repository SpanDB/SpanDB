#include "core_workload.h"

namespace ycsbc{

class WorkloadProxy{
	private:
		CoreWorkload *workload_;

	public:
		WorkloadProxy(CoreWorkload *wl)
			:workload_(wl){

		}

		~WorkloadProxy(){
			
		}

		void LoadInsertArgs(std::string &table, std::string &key, std::vector<ycsbc::CoreWorkload::KVPair> &values){
			table = workload_->NextTable();
			key = workload_->NextSequenceKey();
			workload_->BuildValues(values);
		}

		Operation GetNextOperation(){
			return workload_->NextOperation();
		}

		void GetReadArgs(std::string &table, std::string &key, std::vector<std::string> &fields){
			table = workload_->NextTable();
			key = workload_->NextTransactionKey();
			if (!workload_->read_all_fields()) {
				fields.push_back("field" + workload_->NextFieldName());
			}
		}

		void GetReadModifyWriteArgs(std::string &table, std::string &key, std::vector<std::string> &fields, 
									std::vector<ycsbc::CoreWorkload::KVPair> &values){
			table = workload_->NextTable();
			key = workload_->NextTransactionKey();
			if (!workload_->read_all_fields()) {
				fields.push_back("field" + workload_->NextFieldName());
			}
			if (workload_->write_all_fields()) {
				workload_->BuildValues(values);
			} else {
				workload_->BuildUpdate(values);
			}
		}

		void GetScanArgs(std::string &table, std::string &key, int &len, std::vector<std::string> &fields){
			table = workload_->NextTable();
			key = workload_->NextTransactionKey();
			len = workload_->NextScanLength();
			if (!workload_->read_all_fields()) {
				fields.push_back("field" + workload_->NextFieldName());
			}
		}

		void GetUpdateArgs(std::string &table, std::string &key, std::vector<ycsbc::CoreWorkload::KVPair> &values){
			table = workload_->NextTable();
			key = workload_->NextTransactionKey();
			 if (workload_->write_all_fields()) {
 				workload_->BuildValues(values);
			} else {
				workload_->BuildUpdate(values);
			}
		}

		void GetInsertArgs(std::string &table, std::string &key, std::vector<ycsbc::CoreWorkload::KVPair> &values){
			table = workload_->NextTable();
			key = workload_->NextTransactionKey();
			workload_->BuildValues(values);
		}
};
}