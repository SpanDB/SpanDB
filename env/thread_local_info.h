
#pragma once

namespace rocksdb{

  class ThreadLocalInfo{
    private:
      int id_;
      std::string name_;
      bool master_;
    
    public:
      ThreadLocalInfo():
        id_(-1),
        name_(""),
        master_(false){ }
      
      void SetID(int id){id_ = id;}

      void SetName(std::string name){name_ = name;}

      void SetMaster(bool master){master_ = master;}

      int GetID(){return id_;}

      std::string GetName(){return name_;}

      bool IsMaster(){return master_;}

      bool IsSpanDBCompactor(){return name_.rfind("spandb_compactor", 0) == 0;}

      bool IsRocksDBFlusher(){return name_.rfind("rocksdb_flusher", 0) == 0;}

      bool IsRocksDBCompactor(){return name_.rfind("rocksdb_compactor", 0) == 0;}
  };

  extern thread_local ThreadLocalInfo thread_local_info_;

}

