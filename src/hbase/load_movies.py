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

        csv_file_path = os.path.join(config.DATA_DIR_LOCAL, config.MOVIES_FILE)
        print(f"📂 Đang đọc file: {csv_file_path}")

        col_title = b'info:title'
        col_genres = b'info:genres'

        with table.batch(batch_size=500) as batch:
            with open(csv_file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader) 
                
                count = 0
                for row in reader:
                    if len(row) < 3: continue
                    movie_id = row[0]
                    title = row[1]
                    genres = row[2]
                    
                    batch.put(movie_id.encode('utf-8'), {
                        col_title: title.encode('utf-8'),
                        col_genres: genres.encode('utf-8')
                    })
                    count += 1
                    if count % 10000 == 0:
                        print(f"   -> Đã load {count} movies...")

        connection.close()
        print(f"✅ HOÀN TẤT! Đã load {count} movies.")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()