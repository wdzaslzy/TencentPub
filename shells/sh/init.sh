#!/bin/bash
##更新目录的相关权限
update_path_permissions() {
  ### 01. 解决EMR执行sqoop/hive/spark等任务写本地临时文件没权限问题  所有core计算节点+router节点都要执行
  if [ -d /data/emr/hdfs/tmp ]; then
    chmod 777 -R /data/emr/hdfs/tmp
  fi

  ### 02.有ranger的情况下，spark-sql对hive 鉴权未生效。用来拉取鉴权策略的
  if [ -d /data/emr/spark ]; then
    mkdir -p /data/emr/spark/policycache
    if [ ! -f /data/emr/spark/policycache/sparkSql_hive.json ]; then
      touch /data/emr/spark/policycache/sparkSql_hive.json
    fi
    if [ ! -f /data/emr/spark/policycache/sparkSql_hive_roles.json ]; then
      touch /data/emr/spark/policycache/sparkSql_hive_roles.json
    fi
    chown -R hadoop:hadoop /data/emr/spark/policycache
    chmod 777 -R /data/emr/spark/policycache
  fi
}

update_path_permissions
