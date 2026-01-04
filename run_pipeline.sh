#!/bin/bash
# FILE: run_pipeline.sh

# Cấp quyền thực thi cho các files
chmod +x scripts/*.sh

echo "========================================"
echo "XÂY DỰNG HỆ THỐNG GỢI Ý PHIM THÔNG MINH SỬ DỤNG BIG DATA"
echo "========================================"
echo "1. Chạy TOÀN BỘ (Full Pipeline)"
echo "2. Chạy MapReduce"
echo "3. Chạy HBase Setup & Load"
echo "4. Thoát"
echo "========================================"
read -p "Chọn chế độ (1/2/3/4): " choice

case $choice in
    1)
        ./scripts/run_mapreduce.sh
        ./scripts/run_hbase.sh
        ;;
    2)
        ./scripts/run_mapreduce.sh
        ;;
    3)
        ./scripts/run_hbase.sh
        ;;
	4)
        exit 1
        ;;
    *)
        echo "Lựa chọn không hợp lệ."
        exit 1
        ;;
esac

echo "================ DONE ================"