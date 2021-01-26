# Yahoo! Cloud System Benchmark
# Workload A: Update heavy workload
#   Application example: Session store recording recent actions
#                        
#   Read/update ratio: 50/50
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian

recordcount=512000000
operationcount=20000000
workload=com.yahoo.ycsb.workloads.CoreWorkload

readproportion=0.5
updateproportion=0.5
scanproportion=0
insertproportion=0

requestdistribution=uniform

fieldlength=1024

fieldcount=1

readallfields=true
