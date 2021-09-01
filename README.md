

# SpanDB: A Fast, Cost-Effective LSM-tree Based KV Store on Hybrid Storage



## 1. Install SpanDB

Upgrade your gcc to version at least 4.8 to get C++11 support


**Install SPDK**

Download SPDK

```
git clone https://github.com/spdk/spdk
cd spdk
git checkout v20.01.x
git submodule update --init
```

Compile SPDK

```
# install dependencies
sudo ./scripts/pkgdep.sh
# compile spdk
./configure --with-shared
make
sudo make install
```

Once installed, please add the include path for spdk to your `CPATH` environment variable and the lib path to `LIBRARY_PATH`. E.g.,

```
echo "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib:$PWD/dpdk/build/lib/" >> ~/.bashrc
```

Add dpdk environment,

```
echo "export DPDK_LIB=$PWD/dpdk/build/lib" >> ~/.bashrc
echo "export DPDK_INCLUDE=$PWD/dpdk/build/include" >> ~/.bashrc
source ~/.bashrc 
```


Before running an SPDK application, some hugepages must be allocated and any NVMe and I/OAT devices must be unbound from the native kernel drivers. SPDK includes a script to automate this process on both Linux and FreeBSD. This script should be run as root. It only needs to be run once on the system.

By default, the script allocates 2048MB of hugepages. To change this number, specify HUGEMEM (in MB) as follows:
```
sudo HUGEMEM=4096 scripts/setup.sh
```

In SpanDB, it relies on the hugepages to build it's cache. Please make sure the HUGEMEM is bigger than the size that you want to allocate for SpanDB's cache.


**Install other libraries**

* Upgrade your gcc to version at least 4.8 to get C++11 support.

* upgrade your cmake to version at least 3.5

* **Linux - Ubuntu**
    * Install gflags. First, try: `sudo apt-get install libgflags-dev`
      If this doesn't work and you're using Ubuntu, here's a nice tutorial:
      (http://askubuntu.com/questions/312173/installing-gflags-12-04)
    * Install snappy. This is usually as easy as:
      `sudo apt-get install libsnappy-dev`.
    * Install zlib. Try: `sudo apt-get install zlib1g-dev`.
    * Install bzip2: `sudo apt-get install libbz2-dev`.
    * Install lz4: `sudo apt-get install liblz4-dev`.
    * Install zstandard: `sudo apt-get install libzstd-dev`.
    * Install tbb: `sudo apt-get install libtbb-dev`


* **Linux - CentOS / RHEL**
    * Install gflags:

              git clone https://github.com/gflags/gflags.git
              cd gflags
              git checkout v2.0
              ./configure && make && sudo make install

      **Notice**: Once installed, please add the include path for gflags to your `CPATH` environment variable and the
      lib path to `LIBRARY_PATH`. If installed with default settings, the include path will be `/usr/local/include`
      and the lib path will be `/usr/local/lib`.

    * Install zstandard:

             wget https://github.com/facebook/zstd/archive/v1.1.3.tar.gz
             mv v1.1.3.tar.gz zstd-1.1.3.tar.gz
             tar zxvf zstd-1.1.3.tar.gz
             cd zstd-1.1.3
             make && sudo make install

    * Install others

            sudo yum install snappy snappy-devel zlib zlib-devel bzip2 bzip2-devel lz4-devel libasan tbb-devel


**Install SpanDB**

```
git clone https://github.com/SpanDB/SpanDB.git
cd SpanDB
mkdir build
cd build
cmake .. -DCMAKE_INSTALL_PREFIX:PATH=. -DWITH_TESTS=OFF -DCMAKE_BUILD_TYPE=Release -DUSE_RTTI=true -DFAIL_ON_WARNINGS=false
make -j$(nproc)
make install
```


## 2. How to Test SpanDB

**Compile YCSB**

```
cd SpanDB/ycsb
mkdir build
cd build
cmake ../
make
```

Before testing SpanDB or RocksDB, we need to generate a database that used for the test. During the YCSB test, we can first copy the database to the specific directory and run workloads on this database.

After compilation, you can get the execution file file `test`.

The `test` needs 7 parameters, `<workload_file> <client_num> <data_dir> <log_dir> <is_load> <dbname> <db_bak>`,
* **workload_file:** workload file, they are located in `ycsb/workloads`
* **client_num:** the number of clients
* **data_dir:** the directory that used for storing data
* **log_dir:** the directory that used for storing WAL. If the `dbname=spandb`, `log_dir` should be the pcie addr of the NVMe SSD that used for WAL, e.g., traddr:0000:c9:00.0
* **is_load:** `1` or `0`, `1` means it will generate a new database, `0` means it's used for running workloads.
* **dbname:** `rocksdb` or `spandb`, means it will run rocksdb or spandb 
* **db_bak:** where is the base DB that used for test. If `is_load=1`, this parameter will be ignored. If `is_load=0`, it will copy the database from `db_bak` to `data_dir` for the following testing.


**Generate DB**

```
./test $WORKLOAD_PATH 40 $DATA_PATH $LOG_PATH 1 rocksdb $BASE_DB_PATH
```

**Test**


RocksDB:

```
./test $WORKLOAD_PATH 40 $DATA_PATH $LOG_PATH 0 rocksdb $BASE_DB_PATH
```

SpanDB:

```
sudo su root
ulimit -n 100000
./test $WORKLOAD_PATH 8 $DATA_PATH $PCIE_ADDR 0 spandb $BASE_DB_PATH
```

Note: The TopFS also has a cache. Please make sure the hugepages size is bigger than the cache size.


## 3. Migrate RocksDB database to SpanDB

It's straightforward to migrate the existing RocksDB database to SpanDB by specifying a "fast device". When opening a current RocksDB database with SpanDB, it will automatically migrate the top-level data (as much as possible) to the "fast device".


## 4. Papers

Hao Chen, Chaoyi Ruan, Cheng Li, Xiaosong Ma, Yinlong Xu. *SpanDB: A Fast, Cost-Effective LSM-tree Based KV Store on Hybrid Storage*. In Proceedings of FAST'21: 19th USENIX Conference on File and Storage Technologies (FAST'2021). [[Paper](https://www.usenix.org/conference/fast21/presentation/chen-hao)]


