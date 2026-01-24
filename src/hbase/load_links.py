import csv
import os
import sys

import happybase

# --- SETUP PATH ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.append(project_root)

from configs import config


def main():
    print(f"🔌 Kết nối HBase tại {config.HBASE_HOST}...")
    try:
        connection = happybase.Connection(config.HBASE_HOST, timeout=60000, autoconnect=True)
        table = connection.table(config.HBASE_TABLE_MOVIES)

        csv_path = os.path.join(config.DATA_DIR_LOCAL, config.LINKS_FILE)
        print(f"📂 Đang đọc file: {csv_path}")
        
        if not os.path.exists(csv_path):
             print(f"⚠️ Không tìm thấy file links: {csv_path}")
             return

        # Dùng Context Manager
        with table.batch(batch_size=1000) as batch:
            count = 0
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader) 
                
                for row in reader:
                    if len(row) < 3: continue
                    movie_id = row[0]
                    imdb_id = row[1]
                    tmdb_id = row[2]

                    batch.put(movie_id.encode('utf-8'), {
                        b'info:imdbId': imdb_id.encode('utf-8'),
                        b'info:tmdbId': tmdb_id.encode('utf-8')
                    })
                    count += 1
                    if count % 10000 == 0:
                        print(f"   -> Đã load {count} links...")
                
        connection.close()
        print(f"✅ HOÀN TẤT! Đã load {count} links.")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()