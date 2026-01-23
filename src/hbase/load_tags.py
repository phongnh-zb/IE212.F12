import csv
import os
import sys

import happybase

# --- SETUP PATH ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(project_root)

from configs import config


def main():
    print(f"🔌 Connecting to HBase at {config.HBASE_HOST}...")
    try:
        connection = happybase.Connection(config.HBASE_HOST, timeout=60000, autoconnect=True)
        table = connection.table(config.HBASE_TABLE_MOVIES)
        
        # Đường dẫn file tags.csv
        # Lưu ý: tags.csv có cột: userId,movieId,tag,timestamp
        csv_file = os.path.join(config.DATA_DIR_LOCAL, 'tags.csv')
        
        if not os.path.exists(csv_file):
            print(f"❌ Không tìm thấy file: {csv_file}")
            return

        print("🔄 Đang gom nhóm Tags theo Movie ID (Việc này có thể mất chút thời gian)...")
        movie_tags = {}
        
        # 1. Đọc và gom nhóm Tags trong bộ nhớ (In-memory aggregation)
        with open(csv_file, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                mid = row['movieId']
                tag = row['tag'].strip()
                if mid not in movie_tags:
                    movie_tags[mid] = set() # Dùng set để loại bỏ tag trùng lặp
                movie_tags[mid].add(tag)

        print(f"📦 Đã gom nhóm xong tags cho {len(movie_tags)} phim. Bắt đầu ghi vào HBase...")

        # 2. Ghi vào HBase
        batch = table.batch(batch_size=1000)
        count = 0
        for mid, tags_set in movie_tags.items():
            # Nối các tag thành chuỗi: "funny, pixar, classic"
            # Giới hạn lấy khoảng 5-7 tag đầu tiên để không quá dài
            top_tags = list(tags_set)[:7] 
            tags_str = ", ".join(top_tags)
            
            batch.put(str(mid).encode(), {
                b'info:tags': tags_str.encode()
            })
            count += 1
            
        batch.send()
        print(f"✅ HOÀN TẤT! Đã cập nhật tags cho {count} phim.")
        connection.close()

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()