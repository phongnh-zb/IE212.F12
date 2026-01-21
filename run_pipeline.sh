#!/bin/bash

# --- CẤU HÌNH ---
set -e
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Model mặc định
MODEL_TYPE=${1:-all}

print_header() {
    echo -e "\n${GREEN}========================================================${NC}"
    echo -e "${GREEN}>>> [PIPELINE] $1${NC}"
    echo -e "${GREEN}========================================================${NC}\n"
}

# --- HÀM KIỂM TRA & TỰ KHỞI ĐỘNG ---
ensure_services_running() {
    echo "Running system check..."
    
    # Kiểm tra xem có thiếu service quan trọng nào không
    # Check NameNode (Hadoop), HMaster (HBase), ThriftServer
    if ! jps | grep -q "NameNode" || ! jps | grep -q "HMaster" || ! jps | grep -q "ThriftServer"; then
        echo -e "${YELLOW}⚠️  Phát hiện services chưa chạy đầy đủ.${NC}"
        echo -e "${YELLOW}🚀 Đang gọi 'start_services.sh' để khởi động hệ thống...${NC}"
        
        if [ -f "./scripts/start_services.sh" ]; then
            ./scripts/start_services.sh
            
            echo -e "${YELLOW}⏳ Đang đợi 45 giây để HBase Master khởi tạo (Tránh lỗi 'Initializing')...${NC}"
            # Time wait này cực kỳ quan trọng! HBase cần thời gian để "tỉnh ngủ"
            for i in {45..1}; do echo -ne "$i... " && sleep 1; done; echo ""
        else
            echo -e "${RED}❌ Lỗi: Không tìm thấy file scripts/start_services.sh${NC}"
            exit 1
        fi
    else
        echo -e "${GREEN}✔ Tất cả Services (Hadoop, HBase, Thrift) đang chạy tốt.${NC}"
    fi
}

# ========================================================
# BƯỚC 0: AUTO-START SERVICES
# ========================================================
print_header "BƯỚC 0: SYSTEM HEALTH CHECK"
ensure_services_running

# ========================================================
# BƯỚC 1: MAPREDUCE (ETL)
# ========================================================
print_header "BƯỚC 1: CHẠY MAPREDUCE (ETL)"
chmod +x scripts/run_mapreduce.sh
./scripts/run_mapreduce.sh

# ========================================================
# BƯỚC 2: HBASE LOAD
# ========================================================
print_header "BƯỚC 2: HBASE INIT & LOAD DATA"
chmod +x scripts/run_hbase.sh
./scripts/run_hbase.sh

# Sleep nhẹ để giảm tải sau khi load data
echo -e "${YELLOW}⏳ Đợi 10s để HBase ổn định dữ liệu...${NC}"
sleep 10

# ========================================================
# BƯỚC 3: TRAINING
# ========================================================
print_header "BƯỚC 3: SPARK TRAINING & SAVING (Mode: $MODEL_TYPE)"
python3 src/run_training.py --model "$MODEL_TYPE"

# ========================================================
# KẾT THÚC
# ========================================================
print_header "PIPELINE HOÀN THÀNH!"
echo -e "Web App: ${YELLOW}streamlit run src/app.py${NC}"