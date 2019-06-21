#!/bin/bash
set -e

for service in zookeeper:2181 kafka:9092; do
    "$(dirname "$0")/wait-for-it.sh" -t 60 "$service"
done
