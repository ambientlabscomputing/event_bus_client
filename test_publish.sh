#!/bin/bash
# Test script to verify publish/subscribe flow

echo "=== Testing Event Bus Client ==="
echo ""
echo "Step 1: Start subscriber in background..."
timeout 30 ./bin/eventbus_cli -c config.yaml subscribe test_flow_topic > /tmp/subscriber_output.txt 2>&1 &
SUB_PID=$!
echo "Subscriber started (PID: $SUB_PID)"
sleep 2

echo ""
echo "Step 2: Attempt to publish a message..."
timeout 10 ./bin/eventbus_cli -c config.yaml publish test_flow_topic "Test message at $(date)" 2>&1
PUBLISH_EXIT=$?

echo ""
echo "Step 3: Check subscriber output..."
sleep 1
kill $SUB_PID 2>/dev/null
cat /tmp/subscriber_output.txt

echo ""
echo "=== Test Results ==="
echo "Publish exit code: $PUBLISH_EXIT"
if [ $PUBLISH_EXIT -eq 0 ]; then
    echo "✓ Publish succeeded"
else
    echo "✗ Publish failed or timed out"
fi
