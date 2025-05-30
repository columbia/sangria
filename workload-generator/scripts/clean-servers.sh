#!/bin/bash

services=("frontend" "rangeserver" "universe" "warden")

echo "Searching for matching processes..."

for service in "${services[@]}"; do
    echo "Checking for '$service'..."
    pgrep -af "$service"
done

for service in "${services[@]}"; do
    echo "Killing '$service'..."
    pkill -f "$service"
done
echo "All specified services killed."
