#!/bin/bash

current_time=$(date +"%Y%m%d%H")
last_time=$(date -d "-6 hours" +"%Y%m%d%H")

echo "create 'test_table_$current_time',{ NAME => 'cf', DATA_BLOCK_ENCODING => 'DIFF', BLOOMFILTER => 'ROWCOL', COMPRESSION => 'ZSTD'} , {SPLITS => (1..100).map{|i| \"user#{10000+i*(99999-10000)/100}\"}}" | hbase shell -n

nohup /data/ycsb-hbase20-binding-0.17.0/bin/ycsb load hbase20 -P /data/ycsb-hbase20-binding-0.17.0/workloads/workload_write -cp /usr/local/service/hbase/conf -p table="test_table_$current_time" -p columnfamily=cf -p recordcount=250000000 -p operationcount=250000000 -threads 10 -p clientbuffering=true -s >>workload_write.log 2>&1 &

nohup /data/ycsb-hbase20-binding-0.17.0/bin/ycsb run hbase20 -P /data/ycsb-hbase20-binding-0.17.0/workloads/workload_read -cp /usr/local/service/hbase/conf -p table="test_table_$current_time" -p columnfamily=cf -p recordcount=250000000 -p operationcount=250000000 -threads 20 -s >>workload_read.log 2>&1 &

# 查找进程ID
process_ids=$(pgrep -f "test_table_$last_time")

if [ -n "$process_ids" ]; then
  echo "找到以下进程 test_table_$last_time，进程ID为 $process_ids"
  # 逐个杀死进程
  for pid in $process_ids; do
    kill "$pid"
    echo "进程 $pid 已被杀死"
  done
else
  echo "未找到进程 test_table_$last_time"
fi
