#!/bin/bash
set -e

echo "=== Testing Cascading Aborts ==="
echo ""

# Create a simple test config with abort_rate
cat > /tmp/test_abort_config.json <<'EOF'
{
  "num_queries": 10,
  "num_keys": 3,
  "zipf_exponent": 0.9,
  "namespace": "test",
  "name": "cascading-test",
  "max_concurrency": "2",
  "seed": 42,
  "background_runtime_core_ids": [],
  "fake_transactions": false,
  "workload_type": "custom"
}
EOF

# Backup original config
cp configs/config.json configs/config.json.bak

# Add artificial_abort_rate to config
python3 <<'PYTHON'
import json

with open('configs/config.json', 'r') as f:
    config = json.load(f)

# Set abort rate to 0.5 (50% of transactions will abort)
config['range_server']['artificial_abort_rate'] = 0.5

with open('configs/config.json', 'w') as f:
    json.dump(config, f, indent=2)

print("Updated config with artificial_abort_rate=0.5")
PYTHON

echo ""
echo "=== Running workload with abort_rate=0.5 ==="
echo ""

# Function to wait for a port to be listening
wait_for_port() {
    local port=$1
    local name=$2
    local max_attempts=30
    local attempt=0

    echo "Waiting for $name (port $port) to start..."
    while ! nc -z 127.0.0.1 $port 2>/dev/null; do
        attempt=$((attempt + 1))
        if [ $attempt -ge $max_attempts ]; then
            echo "ERROR: $name failed to start after $max_attempts seconds"
            return 1
        fi
        sleep 1
    done
    echo "$name is ready"
    return 0
}

# Kill any existing servers
pkill -f "target/release/frontend" || true
pkill -f "target/release/rangeserver" || true
pkill -f "target/release/resolver" || true
pkill -f "target/release/universe" || true
sleep 2

# Start servers in correct order with proper wait logic
echo "Starting universe..."
RUST_LOG=info target/release/universe --config configs/config.json > /tmp/universe.log 2>&1 &
wait_for_port 50056 "universe" || exit 1

echo "Starting resolver..."
RUST_LOG=info target/release/resolver --config configs/config.json > /tmp/resolver.log 2>&1 &
wait_for_port 50059 "resolver" || exit 1

echo "Starting rangeserver..."
RUST_LOG=info target/release/rangeserver --config configs/config.json > /tmp/rangeserver.log 2>&1 &
wait_for_port 50054 "rangeserver" || exit 1

echo "Starting frontend..."
RUST_LOG=info target/release/frontend --config configs/config.json > /tmp/frontend.log 2>&1 &
wait_for_port 50057 "frontend" || exit 1

echo "All servers started successfully!"

# Run workload
echo ""
echo "Running workload generator..."
RUST_LOG=info target/release/workload-generator \
  --config configs/config.json \
  --workload-config /tmp/test_abort_config.json \
  --create-keyspace \
  2>&1 | tee /tmp/workload.log

echo ""
echo "=== Checking for cascading abort evidence ==="
echo ""

# Check resolver logs for cascading abort messages
if grep -q "Cascading abort" /tmp/resolver.log; then
  echo "✅ SUCCESS: Found cascading abort messages in resolver.log"
  grep "Cascading abort" /tmp/resolver.log | head -10
else
  echo "⚠️  No cascading abort messages found in resolver.log"
fi

echo ""
if grep -q "Artificially aborting transaction" /tmp/rangeserver.log; then
  echo "✅ SUCCESS: Found artificial abort messages in rangeserver.log"
  grep "Artificially aborting transaction" /tmp/rangeserver.log | head -5
else
  echo "⚠️  No artificial abort messages found in rangeserver.log"
fi

echo ""
echo "=== Cleaning up ==="
pkill -f "target/release/frontend" || true
pkill -f "target/release/rangeserver" || true
pkill -f "target/release/resolver" || true
pkill -f "target/release/universe" || true

# Restore original config
mv configs/config.json.bak configs/config.json

echo ""
echo "=== Test Complete ==="
echo "Check /tmp/*.log for full logs"
