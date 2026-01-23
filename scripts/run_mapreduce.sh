#!/bin/bash
# FILE: scripts/run_mapreduce.sh

# Dừng ngay nếu có lỗi
set -e

# THIẾT LẬP MÔI TRƯỜNG & ĐỌC CONFIG

# Lấy đường dẫn gốc dự án (Đi ngược từ scripts/ ra root)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Hàm hỗ trợ: Đọc biến từ configs/config.py
# Lưu ý: Cần export PYTHONPATH để python tìm thấy module configs
export PYTHONPATH=$PROJECT_ROOT:$PYTHONPATH

function get_conf() {
    VAR_NAME=$1
    python3 -c "from configs import config; print(config.$VAR_NAME)"
}

log_info() { 
    echo -e "\033[0;32m[INFO] $1\033[0m"; 
}
log_error() { 
    echo -e "\033[0;31m[ERROR] $1\033[0m"; 
}

log_info ">>> ĐANG ĐỌC CẤU HÌNH TỪ: configs/config.py"

# --- LẤY GIÁ TRỊ TỪ PYTHON (Cập nhật theo tên biến mới của bạn) ---

# 1. HDFS Paths
HDFS_ROOT=$(get_conf "HDFS_BASE_PATH")

# 2. File Names (Đã sửa khớp với config của bạn: RATINGS_FILE, MOVIES_FILE)
FILE_RATINGS_NAME=$(get_conf "RATINGS_FILE")
FILE_MOVIES_NAME=$(get_conf "MOVIES_FILE")

# 3. Local Data Path
LOCAL_DATA_DIR="$PROJECT_ROOT/data"

# 4. Tìm Hadoop Jar
HADOOP_STREAMING_JAR=$(find "$PROJECT_ROOT/lib" -name "hadoop-streaming-*.jar" | head -n 1)

if [ -z "$HADOOP_STREAMING_JAR" ]; then
    log_error "Không tìm thấy hadoop-streaming.jar trong thư mục lib/"
    exit 1
fi

log_info "HDFS ROOT:   $HDFS_ROOT"
log_info "RATINGS FILE: $FILE_RATINGS_NAME"
log_info "MOVIES FILE:  $FILE_MOVIES_NAME"

# KIỂM TRA & TỰ ĐỘNG TẠO PATH

log_info "--- [Step 0] Kiểm tra môi trường HDFS ---"

# Kiểm tra thư mục gốc
if hdfs dfs -test -d "$HDFS_ROOT"; then
    log_info "-> Thư mục gốc ($HDFS_ROOT) đã tồn tại."
else
    log_info "-> Thư mục gốc chưa có. Đang tạo mới..."
    hdfs dfs -mkdir -p "$HDFS_ROOT"
    log_info "Đã tạo xong!"
fi

# Hàm upload file an toàn
function ensure_hdfs_file() {
    local F_NAME=$1
    local HDFS_PATH="$HDFS_ROOT/$F_NAME"
    local LOCAL_PATH="$LOCAL_DATA_DIR/$F_NAME"

    if hdfs dfs -test -e "$HDFS_PATH"; then
        log_info "-> File '$F_NAME' đã có trên HDFS. Ready."
    else
        log_info "-> File '$F_NAME' thiếu. Đang upload từ Local..."
        if [ -f "$LOCAL_PATH" ]; then
            hdfs dfs -put "$LOCAL_PATH" "$HDFS_PATH"
            log_info "Upload thành công!"
        else
            log_error "LỖI: Không tìm thấy file gốc tại $LOCAL_PATH"
            exit 1
        fi
    fi
}

# Kiểm tra upload
ensure_hdfs_file "$FILE_RATINGS_NAME"
ensure_hdfs_file "$FILE_MOVIES_NAME"

# CHẠY MAPREDUCE JOBS

SRC_DIR="$PROJECT_ROOT/src"

function run_job() {
    JOB_NAME=$1
    INPUT_PATH=$2
    OUTPUT_DIR=$3
    MAPPER=$4
    REDUCER=$5

    log_info "------------------------------------------------"
    log_info "RUNNING JOB: $JOB_NAME"
    
    hdfs dfs -rm -r -f "$OUTPUT_DIR" || true
    
    hadoop jar "$HADOOP_STREAMING_JAR" \
        -D mapreduce.framework.name=local \
        -files "$MAPPER","$REDUCER" \
        -input "$INPUT_PATH" \
        -output "$OUTPUT_DIR" \
        -mapper "$(basename "$MAPPER")" \
        -reducer "$(basename "$REDUCER")"
}

# Job 1: Count Ratings
OUT_1=$(get_conf "HDFS_OUTPUT_RATINGS")
run_job "Count Ratings" \
    "$HDFS_ROOT/$FILE_RATINGS_NAME" \
    "$OUT_1" \
    "$SRC_DIR/mapreduce/count_movie_ratings/mapper.py" \
    "$SRC_DIR/mapreduce/count_movie_ratings/reducer.py"

# Job 2: Average Ratings
OUT_2=$(get_conf "HDFS_OUTPUT_AVG")
run_job "Average Ratings" \
    "$HDFS_ROOT/$FILE_RATINGS_NAME" \
    "$OUT_2" \
    "$SRC_DIR/mapreduce/calc_average_ratings/mapper.py" \
    "$SRC_DIR/mapreduce/calc_average_ratings/reducer.py"

# Job 3: Count Genres
OUT_3=$(get_conf "HDFS_OUTPUT_GENRES")
run_job "Count Genres" \
    "$HDFS_ROOT/$FILE_MOVIES_NAME" \
    "$OUT_3" \
    "$SRC_DIR/mapreduce/count_genres/mapper.py" \
    "$SRC_DIR/mapreduce/count_genres/reducer.py"

# Job 4: NẠP KẾT QUẢ TỪ HDFS VÀO HBASE
log_info "------------------------------------------------"
log_info "ĐANG NẠP KẾT QUẢ THỐNG KÊ VÀO HBASE..."

# Kiểm tra file loader có tồn tại không
if [ -f "$SRC_DIR/hbase/load_stats.py" ]; then
    python3 "$SRC_DIR/hbase/load_stats.py"
    log_info "-> Đã cập nhật 'Điểm Cộng Đồng' vào bảng Movies."
else
    log_error "LỖI: Không tìm thấy file src/hbase/load_stats.py"
fi

log_info ">>> MAPREDUCE PIPELINE HOÀN TẤT!"