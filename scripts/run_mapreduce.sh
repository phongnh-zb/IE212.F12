#!/bin/bash
# FILE: scripts/run_mapreduce.sh

# Dừng ngay nếu có lỗi
set -e

# ============================================================
# 1. THIẾT LẬP MÔI TRƯỜNG & ĐỌC CONFIG
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
export PYTHONPATH=$PROJECT_ROOT:$PYTHONPATH

function get_conf() {
    VAR_NAME=$1
    python3 -c "from configs import config; print(config.$VAR_NAME)"
}

log_info() { echo -e "\033[0;32m[INFO] $1\033[0m"; }
log_error() { echo -e "\033[0;31m[ERROR] $1\033[0m"; }

log_info ">>> ĐANG ĐỌC CẤU HÌNH TỪ: configs/config.py"

HDFS_ROOT=$(get_conf "HDFS_BASE_PATH")
FILE_RATINGS_NAME=$(get_conf "RATINGS_FILE")
FILE_MOVIES_NAME=$(get_conf "MOVIES_FILE")
LOCAL_DATA_DIR="$PROJECT_ROOT/data"
HADOOP_STREAMING_JAR=$(find "$PROJECT_ROOT/lib" -name "hadoop-streaming-*.jar" | head -n 1)

if [ -z "$HADOOP_STREAMING_JAR" ]; then
    log_error "Không tìm thấy hadoop-streaming.jar trong thư mục lib/"
    exit 1
fi

# ============================================================
# 2. LÀM SẠCH VÀ CẬP NHẬT DỮ LIỆU HDFS (THEO YÊU CẦU)
# ============================================================

log_info "--- [Step 0] Chuẩn bị dữ liệu trên HDFS ---"

# Đảm bảo thư mục gốc tồn tại
hdfs dfs -mkdir -p "$HDFS_ROOT"

# 2.1. Xóa thư mục output cũ (Để MapReduce chạy sạch sẽ)
OUT_AVG=$(get_conf "HDFS_OUTPUT_AVG")
log_info "-> Đang xóa output cũ trên HDFS: $OUT_AVG"
hdfs dfs -rm -r -f "$OUT_AVG" || true
# Xóa luôn các output khác cho chắc
hdfs dfs -rm -r -f "$(get_conf "HDFS_OUTPUT_RATINGS")" || true
hdfs dfs -rm -r -f "$(get_conf "HDFS_OUTPUT_GENRES")" || true

# 2.2. FORCE UPLOAD dữ liệu mới nhất từ Local lên HDFS
# Dùng flag -f để ghi đè file cũ
log_info "-> Đang FORCE UPLOAD dữ liệu mới lên HDFS..."

HDFS_RATINGS_PATH="$HDFS_ROOT/$FILE_RATINGS_NAME"
LOCAL_RATINGS_PATH="$LOCAL_DATA_DIR/$FILE_RATINGS_NAME"
if [ -f "$LOCAL_RATINGS_PATH" ]; then
    hdfs dfs -put -f "$LOCAL_RATINGS_PATH" "$HDFS_RATINGS_PATH"
    log_info "   ✅ Đã cập nhật $FILE_RATINGS_NAME"
else
    log_error "   ❌ Không tìm thấy file local: $LOCAL_RATINGS_PATH"
    exit 1
fi

HDFS_MOVIES_PATH="$HDFS_ROOT/$FILE_MOVIES_NAME"
LOCAL_MOVIES_PATH="$LOCAL_DATA_DIR/$FILE_MOVIES_NAME"
if [ -f "$LOCAL_MOVIES_PATH" ]; then
    hdfs dfs -put -f "$LOCAL_MOVIES_PATH" "$HDFS_MOVIES_PATH"
    log_info "   ✅ Đã cập nhật $FILE_MOVIES_NAME"
else
    log_error "   ❌ Không tìm thấy file local: $LOCAL_MOVIES_PATH"
    exit 1
fi

# ============================================================
# 3. CHẠY MAPREDUCE JOBS
# ============================================================

SRC_DIR="$PROJECT_ROOT/src"

function run_job() {
    JOB_NAME=$1
    INPUT_PATH=$2
    OUTPUT_DIR=$3
    MAPPER=$4
    REDUCER=$5

    log_info "------------------------------------------------"
    log_info "RUNNING JOB: $JOB_NAME"
    
    # Xóa lần nữa cho chắc chắn (Idempotency)
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

# ============================================================
# 4. NẠP KẾT QUẢ TỪ HDFS VÀO HBASE
# ============================================================
log_info "------------------------------------------------"
log_info "ĐANG NẠP KẾT QUẢ THỐNG KÊ VÀO HBASE..."

if [ -f "$SRC_DIR/hbase/load_stats.py" ]; then
    # Gọi script load_stats (Script này đã được sửa batch_size=500 ở các bước trước)
    python3 "$SRC_DIR/hbase/load_stats.py"
    log_info "-> Đã cập nhật 'Điểm Cộng Đồng' vào bảng Movies."
else
    log_error "LỖI: Không tìm thấy file src/hbase/load_stats.py"
fi

log_info ">>> MAPREDUCE PIPELINE HOÀN TẤT!"