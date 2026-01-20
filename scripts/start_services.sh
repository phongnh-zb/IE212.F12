#!/bin/bash

# Hadoop takes a while to start, so we only start it if it's not running
if ! jps | grep -q "NameNode"; then
    echo ">>> [START] Starting Hadoop (HDFS)..."
    start-all.sh
else
    echo "✔ Hadoop (HDFS) is already running."
fi

# Similarly, only start if not running
if ! jps | grep -q "HMaster"; then
    echo ">>> [START] Starting HBase..."
    start-hbase.sh
else
    echo "✔ HBase Master is already running."
fi

# For Thrift, we always Kill and Restart to avoid "Broken Pipe" or "Timeout" errors
echo ">>> [RESET] Refreshing HBase Thrift Server..."

# Find and kill old Thrift process (if any)
# 'xargs -r' prevents errors if no pid is found
jps | grep ThriftServer | awk '{print $1}' | xargs -r kill -9 2>/dev/null

# Wait for OS to release the port (important)
echo "-> Waiting for port 9090 release..."
sleep 2

# Restart
echo ">>> [START] Starting new Thrift Server (Port 9090)..."
hbase thrift start -p 9090 --infoport 9095 > /dev/null 2>&1 &

echo "✔ Thrift start command sent."
