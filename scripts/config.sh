#!/bin/bash
# FILE: scripts/config.sh

# Lấy đường dẫn gốc của project
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Đường dẫn thư viện (nằm trong folder lib ở root)
HADOOP_STREAMING_JAR="$PROJECT_ROOT/lib/hadoop-streaming-3.2.3.jar"

# Đường dẫn HDFS
HDFS_ROOT="/user/ie212/movielens"

# Đường dẫn Source Code (để gọi file python)
SRC_DIR="$PROJECT_ROOT/src"

# Màu sắc cho log đẹp hơn
GREEN='\033[0;32m'
NC='\033[0m' # No Color

function log_info() {
    echo -e "${GREEN}[INFO] $1${NC}"
}