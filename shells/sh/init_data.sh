#!/bin/bash
data_dir=($1)

# echo "************参数传递************"
# echo "*    文件执行名称$0     *"
# echo "*    执行目录:$*        *"
# echo "********************************"
# echo "** number of values:$#"
# echo "** process values:$*"
# 使用方法：./init_data.sh "data1 data2"

for i in "${data_dir[@]}";
do
  echo $i
  if [ -d "/$i/" ]
  then
        echo "check dir /$i ok!"
        echo "start repair /$i"
        mkdir -p /$i/emr/hive/logs /$i/emr/hive/tmp /$i/emr/hive/policycache /$i/emr/hive/pid
        mkdir -p /$i/emr/sqoop/logs /$i/emr/sqoop/data
        mkdir -p /$i/emr/hdfs/data /$i/emr/hdfs/logs /$i/emr/hdfs/tmp /$i/emr/hdfs/namenode /$i/emr/hdfs/pid /$i/emr/hdfs/journalnode /$i/emr/hdfs/policycache
        mkdir -p /$i/emr/yarn/data /$i/emr/yarn/logs /$i/emr/yarn/local /$i/emr/yarn/logs /$i/emr/yarn/pid /$i/emr/mr/pid /$i/emr/mr/logs /$i/emr/yarn/policycache
        mkdir -p /$i/emr/spark/logs
        mkdir -p /$i/emr/hbase/logs /$i/emr/hbase/tmp /$i/emr/hbase/pid /$i/emr/hbase/policycache
        mkdir -p /$i/emr/presto
        mkdir -p /$i/emr/tez/logs /$i/emr/tez/data
        mkdir -p /$i/emr/storm/data /$i/emr/storm/logs
        mkdir -p /$i/emr/flink/logs
        mkdir -p /$i/emr/ganglia/rrds
        mkdir -p /$i/qcloud/data/hdfs
        mkdir -p /data/emr/zookeeper/data
        mkdir -p /data/emr/zookeeper/log
        mkdir -p /data/emr/hdfs/journalnode/hadoop
        chown -HR hadoop:hadoop /$i/emr/hive/logs /$i/emr/hive/tmp /$i/emr/hive/policycache /$i/emr/hive/pid /$i/emr/sqoop/logs /$i/emr/sqoop/data /$i/emr/hdfs/data /$i/emr/hdfs/logs /$i/emr/hdfs/tmp /$i/emr/hdfs/namenode /$i/emr/hdfs/pid /$i/emr/hdfs/journalnode /$i/emr/hdfs/policycache /$i/emr/yarn/data /$i/emr/yarn/logs /$i/emr/yarn/local /$i/emr/yarn/logs /$i/emr/yarn/pid /$i/emr/mr/pid /$i/emr/mr/logs /$i/emr/yarn/policycache /$i/emr/spark/logs /$i/emr/hbase/logs /$i/emr/hbase/tmp /$i/emr/hbase/pid /$i/emr/hbase/policycache /$i/emr/presto /$i/emr/tez/logs /$i/emr/tez/data /$i/emr/storm/data /$i/emr/storm/logs /$i/emr/flink/logs /$i/emr/ganglia/rrds /$i/qcloud/data/hdfs /data/emr/hdfs/journalnode/hadoop
        #root
        mkdir -p /data/tmp
        mkdir -p /data/logs
        echo "/$i dict build success!"
  else
        echo "/$i is not exist"
  exit 0
  fi
done