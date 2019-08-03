#!/bin/bash
set -e

for service in kafka:9091 kafka:9092 kafka:9093 kafka:9094 kafka:9095; do
    "$(dirname "$0")/wait-for-it.sh" -t 60 "$service"
done
