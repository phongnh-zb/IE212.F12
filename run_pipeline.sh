#!/bin/bash

# ============================================================
# MOVIELENS BIG DATA PIPELINE - MASTER SCRIPT
# ============================================================

set -e # Dừng ngay nếu có lỗi

# Màu sắc
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

PROJECT_ROOT=$(pwd)
export PYTHONPATH=$PROJECT_ROOT
MODEL_TYPE=${1:-all}

print_header() {
    echo -e "\n${GREEN}========================================================${NC}"
    echo -e "${GREEN}>>> [PIPELINE] $1${NC}"
    echo -e "${GREEN}========================================================${NC}\n"
}

# --- BƯỚC 0: KHỞI ĐỘNG SERVICES ---
print_header "BƯỚC 0: KIỂM TRA SERVICES"

# Gọi script đã được nâng cấp ở trên
if [ -f "./scripts/start_services.sh" ]; then
    chmod +x ./scripts/start_services.sh
    ./scripts/start_services.sh
else
    echo -e "${RED}❌ Không tìm thấy start_services.sh${NC}"
    exit 1
fi

# --- BƯỚC 1: KHỞI TẠO BẢNG (QUAN TRỌNG NHẤT) ---
# Phải chạy cái này trước tiên để tránh lỗi TableNotFound
print_header "BƯỚC 1: KHỞI TẠO SCHEMA HBASE"
if [ -f "src/hbase/init_tables.py" ]; then
    echo "-> Đang tạo/làm sạch bảng..."
    python3 src/hbase/init_tables.py
else
    echo -e "${RED}❌ Thiếu file src/hbase/init_tables.py${NC}"
    exit 1
fi

# --- BƯỚC 2: ETL - NẠP DỮ LIỆU CƠ BẢN ---
print_header "BƯỚC 2: NẠP DỮ LIỆU GỐC (MOVIES, RATINGS, TAGS)"
# Lưu ý: Trong load_movies.py và load_ratings.py nên set BATCH_SIZE=500 để tránh timeout
echo "-> Nạp Movies..."
python3 src/hbase/load_movies.py
echo "-> Nạp Ratings..."
python3 src/hbase/load_ratings.py
echo "-> Nạp Tags..."
python3 src/hbase/load_tags.py

# --- BƯỚC 3: MAPREDUCE - TÍNH TOÁN THỐNG KÊ ---
# Bước này sẽ tính Avg Rating và nạp ngược lại vào HBase
print_header "BƯỚC 3: CHẠY MAPREDUCE & NẠP STATS"
if [ -f "scripts/run_mapreduce.sh" ]; then
    chmod +x scripts/run_mapreduce.sh
    ./scripts/run_mapreduce.sh
else
    echo -e "${RED}❌ Thiếu script MapReduce${NC}"
    exit 1
fi

# --- BƯỚC 4: SPARK - HUẤN LUYỆN MODEL ---
print_header "BƯỚC 4: HUẤN LUYỆN MODEL (Mode: $MODEL_TYPE)"
python3 src/run_training.py --model "$MODEL_TYPE"

# --- KẾT THÚC ---
print_header "✅ PIPELINE HOÀN THÀNH!"
echo -e "👉 Chạy Web App: ${YELLOW}streamlit run src/app.py${NC}"