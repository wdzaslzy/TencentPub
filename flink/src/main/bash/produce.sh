#!/bin/bash
SHELL_DIR=$(
  cd "$(dirname "$0")" || exit
  pwd
)
cd "${SHELL_DIR}" || exit

set -eu

# wget https://archive.apache.org/dist/kafka/1.1.1/kafka_2.11-1.1.1.tgz
# tar xf kafka_2.11-1.1.1.tgz
while [ true ]; do
  bash produce_one.sh
  sleep 60s
done