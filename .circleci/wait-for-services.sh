#!/bin/bash
set -e

for service in localhost:9091 localhost:9092 localhost:9093 localhost:9094 localhost:9095; do
    "$(dirname "$0")/wait-for-it.sh" -t 60 "$service"
done
