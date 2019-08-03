#!/bin/bash
set -e

for service in kafka1:9091 kafka2:9092 kafka3:9093 kafka4:9094 kafka5:9095; do
    "$(dirname "$0")/wait-for-it.sh" -t 60 "$service"
done
