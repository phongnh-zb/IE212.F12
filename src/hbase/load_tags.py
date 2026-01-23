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
        
        csv_file = os.path.join(config.DATA_DIR_LOCAL, 'tags.csv')
        
        if not os.path.exists(csv_file):
            print(f"⚠️  Không tìm thấy file tags: {csv_file}")
            return

        print("🔄 Đang gom nhóm Tags...")
        movie_tags = {}
        
        with open(csv_file, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                mid = row['movieId']
                tag = row['tag'].strip()
                if mid not in movie_tags:
                    movie_tags[mid] = set()
                movie_tags[mid].add(tag)

        print(f"📦 Đang ghi tags cho {len(movie_tags)} phim...")

        batch = table.batch(batch_size=1000)
        count = 0
        for mid, tags_set in movie_tags.items():
            top_tags = list(tags_set)[:7] 
            tags_str = ", ".join(top_tags)
            
            batch.put(str(mid).encode(), {
                b'info:tags': tags_str.encode()
            })
            count += 1
            
        batch.send()
        connection.close()
        print(f"✅ HOÀN TẤT! Đã cập nhật tags.")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()