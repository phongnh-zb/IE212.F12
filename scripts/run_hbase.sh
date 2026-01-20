#!/bin/bash
# FILE: scripts/run_hbase.sh

# Load biến từ config
source "$(dirname "$0")/config.sh"

log_info ">>> BẮT ĐẦU QUY TRÌNH HBASE..."

# 2. Re-create Tables (Sử dụng Ruby begin/rescue để tránh lỗi)
# Cơ chế: Cố gắng disable và drop. Nếu bảng chưa có (gặp lỗi) -> bỏ qua (rescue) và chạy tiếp lệnh create.

log_info "Re-creating table 'movies'..."
hbase shell -n <<EOF
  begin
    disable 'movies'
    drop 'movies'
  rescue
  end
  create 'movies', 'info', 'stats'
EOF

log_info "Re-creating table 'ratings'..."
hbase shell -n <<EOF
  begin
    disable 'ratings'
    drop 'ratings'
  rescue
  end
  create 'ratings', 'info'
EOF

log_info "Re-creating table 'tags'..."
hbase shell -n <<EOF
  begin
    disable 'tags'
    drop 'tags'
  rescue
  end
  create 'tags', 'info'
EOF

log_info "Re-creating table 'recommendations'..."
hbase shell -n <<EOF
  begin
    disable 'recommendations'
    drop 'recommendations'
  rescue
  end
  create 'recommendations', 'info'
EOF

# 3. Load Data
log_info "Running Python Load Scripts..."

# Dữ liệu Movies (Core + Stats + Links)
log_info "Loading Movies data..."
python "$SRC_DIR/hbase/load_movies.py"
python "$SRC_DIR/hbase/load_stats.py"
python "$SRC_DIR/hbase/load_links.py"

# Dữ liệu Ratings
log_info "Loading Ratings data..."
python "$SRC_DIR/hbase/load_ratings.py"

# Dữ liệu Tags
log_info "Loading Tags data..."
python "$SRC_DIR/hbase/load_tags.py"

log_info ">>> HBase Load hoàn tất!"