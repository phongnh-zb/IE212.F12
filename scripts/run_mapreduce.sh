#!/bin/bash
# FILE: scripts/run_mapreduce.sh

# Load biến từ config
source "$(dirname "$0")/config.sh"

log_info ">>> BẮT ĐẦU QUY TRÌNH MAPREDUCE (Local Mode)..."

# Hàm chạy 1 Job cho gọn code
function run_job() {
    JOB_NAME=$1
    INPUT_FILE=$2
    OUTPUT_DIR=$3
    MAPPER_PATH=$4
    REDUCER_PATH=$5

    log_info "Dang chay Job: $JOB_NAME..."
    
    # Xóa output cũ
    hdfs dfs -rm -r -f $OUTPUT_DIR || true
    
    # Chạy Hadoop
    hadoop jar "$HADOOP_STREAMING_JAR" \
        -D mapreduce.framework.name=local \
        -files "$MAPPER_PATH","$REDUCER_PATH" \
        -input "$INPUT_FILE" \
        -output "$OUTPUT_DIR" \
        -mapper "$(basename "$MAPPER_PATH")" \
        -reducer "$(basename "$REDUCER_PATH")"
}

# --- THỰC THI CÁC JOB ---

# Job 1
run_job "Count Ratings" \
    "$HDFS_ROOT/ratings.csv" \
    "$HDFS_ROOT/output_rating_counts" \
    "$SRC_DIR/mapreduce/count_movie_ratings/mapper.py" \
    "$SRC_DIR/mapreduce/count_movie_ratings/reducer.py"

# Job 2
run_job "Average Ratings" \
    "$HDFS_ROOT/ratings.csv" \
    "$HDFS_ROOT/output_average_ratings" \
    "$SRC_DIR/mapreduce/calc_average_ratings/mapper.py" \
    "$SRC_DIR/mapreduce/calc_average_ratings/reducer.py"

# Job 3
run_job "Count Genres" \
    "$HDFS_ROOT/movies.csv" \
    "$HDFS_ROOT/output_genre_counts" \
    "$SRC_DIR/mapreduce/count_genres/mapper.py" \
    "$SRC_DIR/mapreduce/count_genres/reducer.py"

log_info ">>> MapReduce hoàn tất!"