#!/bin/bash
set -e
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# [UPDATE] Mặc định chạy 'all' nếu không nhập tham số
MODEL_TYPE=${1:-all}

print_header() {
    echo -e "\n${GREEN}========================================================${NC}"
    echo -e "${GREEN}>>> [PIPELINE] $1${NC}"
    echo -e "${GREEN}========================================================${NC}\n"
}

check_service() {
    if ! jps | grep -q "$1"; then
        echo -e "${YELLOW}⚠️ Cảnh báo: Không thấy service '$1'. Đang thử khởi động lại...${NC}"
        # Tự động fix nếu thiếu service (Optional)
    fi
}

# BƯỚC 0
print_header "BƯỚC 0: CHECK SERVICES"
check_service "ThriftServer"

# BƯỚC 1
print_header "BƯỚC 1: MAPREDUCE (ETL)"
chmod +x scripts/run_mapreduce.sh
./scripts/run_mapreduce.sh

# BƯỚC 2
print_header "BƯỚC 2: HBASE LOAD"
chmod +x scripts/run_hbase.sh
./scripts/run_hbase.sh
sleep 5

# BƯỚC 3
print_header "BƯỚC 3: TRAINING (Mode: $MODEL_TYPE)"
echo "Hệ thống sẽ chạy lần lượt các model được yêu cầu..."

# Chạy code Python mới
python3 src/run_training.py --model "$MODEL_TYPE"

print_header "PIPELINE HOÀN TẤT!"
echo -e "Web App: ${YELLOW}streamlit run src/app.py${NC}"