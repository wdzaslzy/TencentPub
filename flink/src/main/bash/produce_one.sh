#!/bin/bash
SHELL_DIR=$(
  cd "$(dirname "$0")" || exit
  pwd
)
cd "${SHELL_DIR}" || exit

set -eu

rm -f data1.txt
python gen.py data1.txt
kafka*/bin/kafka-console-producer.sh --topic test1 --broker-list 10.0.16.22:9092 < data1.txt
