#!/bin/bash
# Diagnostic script to check if Ray experiment is hung or just slow

echo "=== Checking Ray Experiment Status ==="
echo ""

# Find the most recent Ray session
RAY_SESSION=$(ls -t /tmp/ray/session_* 2>/dev/null | head -1)

if [ -z "$RAY_SESSION" ]; then
    echo "❌ No Ray session found"
    exit 1
fi

echo "📂 Ray session: $RAY_SESSION"
echo ""

# Check if processes are running
echo "=== Process Status ==="
echo "Workload generators:"
ps aux | grep workload-generator | grep -v grep | wc -l
echo "Rangeservers:"
ps aux | grep rangeserver | grep -v grep | wc -l
echo ""

# Check for artificial aborts in logs
echo "=== Checking for Artificial Aborts (last 100 lines) ==="
ABORT_COUNT=$(tail -100 $RAY_SESSION/logs/worker-*.out 2>/dev/null | grep -c "Artificial abort triggered")
echo "Found $ABORT_COUNT artificial aborts in recent logs"

if [ $ABORT_COUNT -gt 0 ]; then
    echo "✅ Artificial aborts are happening"
    echo ""
    echo "Sample abort logs:"
    tail -100 $RAY_SESSION/logs/worker-*.out 2>/dev/null | grep -A 3 "Artificial abort" | head -20
else
    echo "⚠️  No artificial aborts found - might not be hitting abort code"
fi

echo ""

# Check for errors/panics
echo "=== Checking for Errors/Panics (last 100 lines) ==="
ERROR_COUNT=$(tail -100 $RAY_SESSION/logs/worker-*.out 2>/dev/null | grep -c -i -E "panic|error|deadlock")
echo "Found $ERROR_COUNT errors/panics in recent logs"

if [ $ERROR_COUNT -gt 0 ]; then
    echo "⚠️  Errors found:"
    tail -100 $RAY_SESSION/logs/worker-*.out 2>/dev/null | grep -i -E "panic|error|deadlock" | head -10
fi

echo ""

# Check transaction progress
echo "=== Checking Transaction Progress ==="
echo "Looking for 'Dependencies for transaction' messages (indicates successful prepare)..."
PREPARE_COUNT=$(tail -200 $RAY_SESSION/logs/worker-*.out 2>/dev/null | grep -c "Dependencies for transaction")
echo "Found $PREPARE_COUNT recent PREPARE operations"

if [ $PREPARE_COUNT -eq 0 ]; then
    echo "⚠️  No recent PREPARE operations - system might be stuck"
else
    echo "✅ Transactions are making progress"
fi

echo ""

# Check for specific hang indicators
echo "=== Checking for Hang Indicators ==="

# Check if workload generator is stuck
WG_RUNNING=$(ps aux | grep workload-generator | grep -v grep | wc -l)
if [ $WG_RUNNING -eq 0 ]; then
    echo "⚠️  No workload generators running - may have completed or crashed"
else
    echo "✅ Workload generators are running"

    # Check CPU usage
    WG_CPU=$(ps aux | grep workload-generator | grep -v grep | awk '{sum+=$3} END {print sum}')
    echo "   CPU usage: ${WG_CPU}%"

    if (( $(echo "$WG_CPU < 5" | bc -l) )); then
        echo "   ⚠️  Very low CPU - might be waiting/stuck"
    fi
fi

echo ""

# Estimate completion
echo "=== Experiment Progress Estimate ==="
START_TIME=$(stat -c %Y $RAY_SESSION/logs 2>/dev/null || stat -f %m $RAY_SESSION/logs 2>/dev/null)
CURRENT_TIME=$(date +%s)
ELAPSED=$((CURRENT_TIME - START_TIME))

echo "Elapsed time: ${ELAPSED} seconds ($((ELAPSED / 60)) minutes)"
echo ""

if [ $ELAPSED -lt 120 ]; then
    echo "✅ Under 2 minutes - still within normal range"
elif [ $ELAPSED -lt 300 ]; then
    echo "⚠️  Over 2 minutes - getting slow but might be normal with 10% abort rate"
else
    echo "❌ Over 5 minutes - likely indicates a hang or deadlock"
fi

echo ""
echo "=== Recommendations ==="
if [ $ABORT_COUNT -eq 0 ]; then
    echo "1. Artificial aborts not triggering - check config is loaded correctly"
fi

if [ $PREPARE_COUNT -eq 0 ]; then
    echo "2. No PREPARE operations - system is likely stuck. Check for deadlocks."
fi

if [ $ELAPSED -gt 300 ]; then
    echo "3. Experiment taking too long - consider killing and investigating"
    echo "   Kill command: pkill -f workload-generator"
fi

echo ""
echo "To monitor live: tail -f $RAY_SESSION/logs/worker-*.out | grep -i -E 'artificial|abort|error|dependencies'"
