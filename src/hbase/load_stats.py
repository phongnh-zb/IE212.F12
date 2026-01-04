import os
import subprocess
import sys

import happybase
from common import config


def read_hdfs_stream():
    hdfs_path = f"{config.HDFS_OUTPUT_AVG}/*"
    
    print(f"Dang doc stream tu HDFS: {hdfs_path}")
    
    # Gọi lệnh shell: hdfs dfs -cat ...
    cat_process = subprocess.Popen(
        ['hdfs', 'dfs', '-cat', hdfs_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    # Đọc stream output
    for line in cat_process.stdout:
        yield line.decode('utf-8').strip()

def main():
    print(f"Dang ket noi HBase voi {config.HBASE_HOST}...")
    connection = happybase.Connection(config.HBASE_HOST)
    
    table_name = config.HBASE_TABLE_MOVIES
    table = connection.table(table_name)
    
    batch = table.batch()
    count = 0
    
    print(f"Bat dau cap nhat Rating vao bang '{table_name}'...")
    
    cf_stats = config.HBASE_CF_STATS
    
    for line in read_hdfs_stream():
        if not line: continue
        
        try:
            parts = line.split('\t')
            if len(parts) != 2: continue
            
            movie_id = parts[0]
            avg_rating = parts[1]
            
            col_name = cf_stats + b':avg_rating'
            
            # Update vào HBase
            batch.put(movie_id.encode('utf-8'), {
                col_name: avg_rating.encode('utf-8')
            })
            
            count += 1
            if count % 1000 == 0:
                print(f"Da cap nhat {count} phim...")
                
        except Exception as e:
            print(f"Loi dong: {line} - {e}")

    batch.send()
    connection.close()
    print(f"THANH CONG! Da cap nhat diem rating cho {count} phim.")

if __name__ == "__main__":
    main()