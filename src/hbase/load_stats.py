import os
import subprocess
import sys

import happybase
from common import config


def read_hdfs_stream():
    # Đường dẫn output của MapReduce
    hdfs_path = f"{config.HDFS_OUTPUT_AVG}/*"
    print(f"Dang doc stream tu HDFS: {hdfs_path}")
    
    process = subprocess.Popen(
        ['hdfs', 'dfs', '-cat', hdfs_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    for line in process.stdout:
        yield line.decode('utf-8').strip()

def main():
    print(f"Dang ket noi HBase voi {config.HBASE_HOST}...")
    # Thêm timeout và autoconnect để kết nối ổn định hơn
    connection = happybase.Connection(config.HBASE_HOST, timeout=60000, autoconnect=True)
    table = connection.table(config.HBASE_TABLE_MOVIES)
    
    print(f"Bat dau cap nhat Rating vao bang '{config.HBASE_TABLE_MOVIES}'...")
    
    count = 0
    try:
        with table.batch(batch_size=1000) as batch:
            for line in read_hdfs_stream():
                if not line: continue
                
                try:
                    # Parse dữ liệu: movieID \t avg_rating
                    parts = line.split('\t')
                    if len(parts) != 2: continue
                    
                    movie_id = parts[0]
                    avg_rating = parts[1]
                    
                    # Tạo tên cột: stats:avg_rating
                    col_name = b'stats' + b':avg_rating'
                    
                    batch.put(movie_id.encode('utf-8'), {
                        col_name: avg_rating.encode('utf-8')
                    })
                    
                    count += 1
                    if count % 1000 == 0:
                        print(f"Da cap nhat {count} phim...")
                except Exception as e:
                    print(f"Loi xu ly dong: {line} - {e}")
    except Exception as e:
        print(f"!!! Lỗi khi ghi vào HBase: {e}")
    finally:
        connection.close()

    print(f"HOAN TAT! Da cap nhat diem rating cho {count} phim.")

if __name__ == "__main__":
    main()