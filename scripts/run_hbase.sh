#!/bin/bash

# ============================================================
# SCRIPT NẠP DỮ LIỆU VÀO HBASE (ETL)
# ============================================================

GREEN='\033[0;32m'
NC='\033[0m' # No Color

echo -e "${GREEN}>>> [HBASE] BẮT ĐẦU QUÁ TRÌNH NẠP DỮ LIỆU...${NC}"

# 1. Nạp Movies
echo "-----------------------------------"
echo "1. Đang load dữ liệu 'movies'..."
python3 src/hbase/load_movies.py

# 2. Nạp Ratings
echo "-----------------------------------"
echo "2. Đang load dữ liệu 'ratings'..."
python3 src/hbase/load_ratings.py

# 3. Nạp Tags (Nếu có file)
echo "-----------------------------------"
echo "3. Đang load dữ liệu 'tags'..."
if [ -f "src/hbase/load_tags.py" ]; then
    python3 src/hbase/load_tags.py
else
    echo "⚠️  Không tìm thấy src/hbase/load_tags.py -> Bỏ qua."
fi

echo -e "\n${GREEN}>>> [HBASE] HOÀN TẤT QUÁ TRÌNH ETL!${NC}"