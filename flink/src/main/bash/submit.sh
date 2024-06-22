#!/bin/bash
SHELL_DIR=$(
  cd "$(dirname "$0")" || exit
  pwd
)
cd "${SHELL_DIR}" || exit

set -eu

export HADOOP_CLASSPATH=$(hadoop classpath)

./bin/flink run -m yarn-cluster -c com.tencent.flink.ry.CheckpointTimeoutExample /home/hadoop/flink-1.0-SNAPSHOT-jar-with-dependencies.jar


./bin/flink run -Djava. -m yarn-cluster -c com.tencent.flink.java.WritePublicKafkaExample /home/hadoop/flink-1.0-SNAPSHOT-jar-with-dependencies.jar

env.java.opts.all: "-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${FLINK_LOG_PREFIX}.hprof"

-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${FLINK_LOG_PREFIX}.hprof