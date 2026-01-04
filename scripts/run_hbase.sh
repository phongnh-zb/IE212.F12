#!/bin/bash
# FILE: scripts/run_hbase.sh

# Load biến từ config
source "$(dirname "$0")/config.sh"

log_info ">>> BẮT ĐẦU QUY TRÌNH HBASE..."

# 1. Restart Thrift
log_info "Restarting HBase Thrift Server..."
hbase-daemon.sh stop thrift || true
sleep 2
hbase-daemon.sh start thrift

# 2. Re-create Table
log_info "Re-creating table 'movies'..."
echo "disable 'movies'; drop 'movies'; create 'movies', 'info', 'stats'" | hbase shell -n

# 3. Load Data
log_info "Running Python Load Scripts..."

python "$SRC_DIR/hbase/load_movies.py"
python "$SRC_DIR/hbase/load_stats.py"

log_info ">>> HBase Load hoàn tất!"