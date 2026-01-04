import subprocess
import sys

import happybase

HDFS_OUTPUT_PATH = '/user/ie212/movielens/output_average_ratings/*'

def read_hdfs_stream():
    cat_process = subprocess.Popen(
        ['hdfs', 'dfs', '-cat', HDFS_OUTPUT_PATH],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    for line in cat_process.stdout:
        yield line.decode('utf-8').strip()

def main():
    connection = happybase.Connection('localhost')
    table = connection.table('movies')
    print(f"Dang ket noi HBase va doc du lieu tu: {HDFS_OUTPUT_PATH}")

    batch = table.batch()
    count = 0
    
    print("Bat dau cap nhat Rating vao HBase...")
    
    for line in read_hdfs_stream():
        if not line: continue
        
        try:
            parts = line.split('\t')
            if len(parts) != 2: continue
            
            movie_id = parts[0]
            avg_rating = parts[1]
            
            batch.put(movie_id.encode('utf-8'), {
                b'stats:avg_rating': avg_rating.encode('utf-8')
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