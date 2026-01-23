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
        # CHỈ KẾT NỐI, KHÔNG TẠO BẢNG
        table = connection.table(config.HBASE_TABLE_RATINGS)

        csv_file_path = os.path.join(config.DATA_DIR_LOCAL, config.RATINGS_FILE)
        print(f"📂 Đang đọc file: {csv_file_path}")

        if not os.path.exists(csv_file_path):
            print(f"❌ Không tìm thấy file: {csv_file_path}")
            return

        with open(csv_file_path, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            batch = table.batch(batch_size=1000)
            count = 0
            
            print("🚀 Bắt đầu nạp Rating...")
            for row in reader:
                try:
                    user_id = str(int(row["userId"]))
                    movie_id = str(int(row["movieId"]))
                    rating = str(float(row["rating"]))
                    timestamp = str(int(row["timestamp"]))

                    # RowKey: UserID
                    # r: rating, t: timestamp
                    batch.put(user_id.encode(), {
                        f"r:{movie_id}".encode(): rating.encode(),
                        f"t:{movie_id}".encode(): timestamp.encode()
                    })
                    
                    count += 1
                    if count % 10000 == 0:
                        print(f"   -> Đã load {count} dòng...")
                        
                except ValueError: continue
            
            batch.send()
            print(f"✅ HOÀN TẤT! Đã nạp {count} ratings.")
            
        connection.close()

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()