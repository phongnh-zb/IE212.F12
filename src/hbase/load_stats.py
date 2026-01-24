import os
import subprocess
import sys

import happybase

# --- SETUP PATH ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.append(project_root)

from configs import config


def read_hdfs_output(hdfs_folder):
    """Đọc output từ HDFS"""
    path = f"{hdfs_folder}/*"
    print(f"📡 Đang đọc stream từ HDFS: {path}")
    process = subprocess.Popen(
        ['hdfs', 'dfs', '-cat', path],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    for line in process.stdout:
        yield line.decode('utf-8').strip()

def load_generic(connection, table_name, family, col_name, hdfs_path, desc):
    """Hàm nạp dữ liệu chung"""
    print(f"📥 Bắt đầu nạp {desc} vào bảng '{table_name}'...")
    try:
        table = connection.table(table_name)
        batch = table.batch(batch_size=1000)
        count = 0
        
        for line in read_hdfs_output(hdfs_path):
            if not line: continue
            try:
                # MapReduce output format: Key \t Value
                parts = line.split('\t')
                if len(parts) != 2: continue
                
                key = parts[0]
                val = parts[1]
                
                # Ghi vào HBase
                full_col = f"{family}:{col_name}".encode('utf-8')
                batch.put(key.encode('utf-8'), {full_col: val.encode('utf-8')})
                count += 1
            except: continue
        
        batch.send()
        print(f"✅ Đã nạp {count} dòng cho {desc}.")
    except Exception as e:
        print(f"❌ Lỗi nạp {desc}: {e}")

def main():
    print(f"🔌 Kết nối HBase tại {config.HBASE_HOST}...")
    try:
        conn = happybase.Connection(config.HBASE_HOST, timeout=60000, autoconnect=True)
        
        # 1. NẠP CHO BẢNG MOVIES (Dữ liệu thống kê)
        # Giả sử bảng 'movies' và family 'stats' đã được tạo bởi init_tables.py
        load_generic(conn, config.HBASE_TABLE_MOVIES, 'stats', 'avg_rating', config.HDFS_OUTPUT_AVG, "Điểm TB")
        load_generic(conn, config.HBASE_TABLE_MOVIES, 'stats', 'rating_count', config.HDFS_OUTPUT_RATINGS, "Lượt Chấm")
        
        # 2. NẠP CHO BẢNG GENRE_STATS
        # Giả sử bảng 'genre_stats' đã được tạo bởi init_tables.py
        load_generic(conn, config.HBASE_TABLE_GENRE_STATS, 'info', 'count', config.HDFS_OUTPUT_GENRES, "Thống Kê Thể Loại")
        
        conn.close()
        print("\n🎉 HOÀN TẤT CẬP NHẬT STATS!")
        
    except Exception as e:
        print(f"❌ Critical Error: {e}")

if __name__ == "__main__":
    main()