import os
import subprocess
import sys

import happybase

# Setup đường dẫn
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(project_root)

from configs import config


def get_hbase_connection():
    try:
        connection = happybase.Connection(config.HBASE_HOST, timeout=30000, autoconnect=True)
        return connection
    except Exception as e:
        print(f"❌ Lỗi kết nối HBase: {e}")
        return None

def load_from_hdfs(connection, hdfs_path, column_family, column_name, description):
    print(f"\n🚀 Bắt đầu nạp '{description}' từ: {hdfs_path}")
    
    table = connection.table(config.HBASE_TABLE_MOVIES)
    
    # Lệnh đọc file từ HDFS
    hdfs_cmd = f"hdfs dfs -cat {hdfs_path}/part-*"
    
    try:
        process = subprocess.Popen(hdfs_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        batch_size = 500
        batch = table.batch(batch_size=batch_size)
        count = 0
        
        for line in process.stdout:
            try:
                line_str = line.decode('utf-8').strip()
                if not line_str: continue

                # Tách bằng dấu TAB (\t) do MapReduce xuất ra
                parts = line_str.split('\t')
                
                # Fallback dấu phẩy
                if len(parts) < 2:
                    parts = line_str.split(',')
                
                if len(parts) >= 2:
                    movie_id = parts[0].strip()
                    value = parts[1].strip()
                    
                    # Ghi vào HBase
                    # column_family: b'stats'
                    # column_name: b'avg_rating' hoặc b'rating_count'
                    col_key = f"{column_family}:{column_name}".encode()
                    
                    batch.put(movie_id.encode(), {
                        col_key: value.encode()
                    })
                    count += 1
                    
                    if count % 2000 == 0:
                        print(f"   -> Đã nạp {count} dòng...", end='\r')
                        
            except Exception as e:
                continue

        batch.send()
        print(f"✅ HOÀN TẤT '{description}'! Tổng cộng: {count} dòng.")
        
    except Exception as e:
        print(f"❌ Lỗi khi đọc HDFS: {e}")

def main():
    conn = get_hbase_connection()
    if not conn: return

    try:
        # 1. NẠP AVG RATING (Điểm cộng đồng)
        # MapReduce Job: Average Ratings
        load_from_hdfs(
            conn, 
            config.HDFS_OUTPUT_AVG, 
            "stats", 
            "avg_rating", 
            "Điểm Cộng Đồng"
        )

        # 2. NẠP RATING COUNT (Số lượt đánh giá)
        # MapReduce Job: Count Ratings
        load_from_hdfs(
            conn, 
            config.HDFS_OUTPUT_RATINGS, 
            "stats", 
            "rating_count", 
            "Số Lượt Đánh Giá"
        )

    finally:
        conn.close()

if __name__ == "__main__":
    main()