#!/bin/bash

# ============================================================
# SCRIPT NẠP DỮ LIỆU VÀO HBASE (ETL)
# ============================================================

GREEN='\033[0;32m'
NC='\033[0m' # No Color

echo -e "${GREEN}>>> [HBASE] BẮT ĐẦU QUÁ TRÌNH NẠP DỮ LIỆU...${NC}"

# 1. Nạp Movies (Danh sách phim)
echo "-----------------------------------"
echo "1. Đang nạp bảng 'movies'..."
python3 src/hbase/load_movies.py

# 2. Nạp Ratings (Lịch sử đánh giá) - Đã fix lỗi tạo bảng
echo "-----------------------------------"
echo "2. Đang nạp bảng 'ratings'..."
python3 src/hbase/load_ratings.py

# 3. Nạp Tags (Từ khóa do user gắn)
echo "-----------------------------------"
echo "3. Đang nạp bảng 'tags'..."
if [ -f "src/hbase/load_tags.py" ]; then
    python3 src/hbase/load_tags.py
else
    echo "⚠️  Không tìm thấy src/hbase/load_tags.py -> Bỏ qua."
fi

# 4. Nạp Links (Liên kết IMDb/TMDB)
echo "-----------------------------------"
echo "4. Đang nạp bảng 'links'..."
if [ -f "src/hbase/load_links.py" ]; then
    python3 src/hbase/load_links.py
else
    echo "⚠️  Không tìm thấy src/hbase/load_links.py -> Bỏ qua."
fi

echo -e "\n${GREEN}>>> [HBASE] HOÀN TẤT NẠP DỮ LIỆU!${NC}"