#!/bin/bash

# ============================================================
# MOVIELENS BIG DATA PIPELINE - SCRIPT TỔNG HỢP
# ============================================================

# Dừng script ngay lập tức nếu có lệnh bị lỗi
set -e

# Định nghĩa màu sắc
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Lấy đường dẫn gốc của dự án
PROJECT_ROOT=$(pwd)
export PYTHONPATH=$PROJECT_ROOT

# Model mặc định (nếu không truyền tham số thì mặc định là 'all')
MODEL_TYPE=${1:-all}

# Hàm in tiêu đề
print_header() {
    echo -e "\n${GREEN}========================================================${NC}"
    echo -e "${GREEN}>>> [PIPELINE] $1${NC}"
    echo -e "${GREEN}========================================================${NC}\n"
}

# --- HÀM KIỂM TRA & TỰ KHỞI ĐỘNG SERVICES ---
ensure_services_running() {
    echo "Đang kiểm tra trạng thái hệ thống..."
    
    # Kiểm tra NameNode, HMaster, ThriftServer
    if ! jps | grep -q "NameNode" || ! jps | grep -q "HMaster" || ! jps | grep -q "ThriftServer"; then
        echo -e "${YELLOW}⚠️  Phát hiện services chưa chạy đầy đủ.${NC}"
        echo -e "${YELLOW}🚀 Đang gọi 'scripts/start_services.sh'...${NC}"
        
        if [ -f "./scripts/start_services.sh" ]; then
            chmod +x ./scripts/start_services.sh
            ./scripts/start_services.sh
            
            echo -e "${YELLOW}⏳ Đang đợi 45 giây để HBase Master khởi tạo...${NC}"
            for i in {45..1}; do echo -ne "$i... " && sleep 1; done; echo ""
        else
            echo -e "${RED}❌ Lỗi: Không tìm thấy file scripts/start_services.sh${NC}"
            exit 1
        fi
    else
        echo -e "${GREEN}✔ Tất cả Services đang hoạt động tốt.${NC}"
    fi
}

# ========================================================
# BƯỚC 0: KIỂM TRA HỆ THỐNG
# ========================================================
print_header "BƯỚC 0: SYSTEM HEALTH CHECK"
ensure_services_running

# ========================================================
# BƯỚC 1: NẠP DỮ LIỆU THÔ (ETL)
# ========================================================
print_header "BƯỚC 1: NẠP DỮ LIỆU VÀO HBASE (ETL)"

# Sử dụng script riêng run_hbase.sh để quản lý gọn gàng
if [ -f "scripts/run_hbase.sh" ]; then
    chmod +x scripts/run_hbase.sh
    ./scripts/run_hbase.sh
else
    echo -e "${RED}❌ Lỗi: Không tìm thấy file scripts/run_hbase.sh${NC}"
    exit 1
fi

# ========================================================
# BƯỚC 2: TÍNH TOÁN THỐNG KÊ (MAPREDUCE)
# ========================================================
print_header "BƯỚC 2: CHẠY MAPREDUCE (THỐNG KÊ)"

if [ -f "scripts/run_mapreduce.sh" ]; then
    chmod +x scripts/run_mapreduce.sh
    ./scripts/run_mapreduce.sh
else
    echo -e "${YELLOW}⚠️  Không tìm thấy script MapReduce. Bỏ qua.${NC}"
fi

# ========================================================
# BƯỚC 3: HUẤN LUYỆN MODEL & GỢI Ý (SPARK)
# ========================================================
print_header "BƯỚC 3: HUẤN LUYỆN MODEL SPARK (Mode: $MODEL_TYPE)"

# Sử dụng spark-submit để tối ưu bộ nhớ thay vì python thuần
python3 src/run_training.py --model "$MODEL_TYPE"

# ========================================================
# KẾT THÚC
# ========================================================
print_header "✅ PIPELINE HOÀN THÀNH!"
echo -e "👉 Mở Web App: ${YELLOW}streamlit run src/app.py${NC}"
echo "========================================================"