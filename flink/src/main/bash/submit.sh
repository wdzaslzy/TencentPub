#!/bin/bash
SHELL_DIR=$(
  cd "$(dirname "$0")" || exit
  pwd
)
cd "${SHELL_DIR}" || exit

set -eu

export HADOOP_CLASSPATH=$(hadoop classpath)

/usr/local/service/flink-1.16.2/bin/flink run -d -m yarn-cluster \
   -c com.tencent.flink.ry \
  /home/hadoop/flink-1.0-SNAPSHOT-jar-with-dependencies.jar