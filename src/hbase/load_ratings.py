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
        connection = happybase.Connection(config.HBASE_HOST, timeout=10000)
        table_name = config.HBASE_TABLE_RATINGS
        
        # 1. TẠO LẠI BẢNG VỚI 2 FAMILY: 'r' (rating) VÀ 't' (time)
        existing_tables = [t.decode('utf-8') for t in connection.tables()]
        if table_name in existing_tables:
            print(f"⚠️  Đang xóa bảng '{table_name}' cũ để nạp lại dữ liệu có Timestamp...")
            connection.disable_table(table_name)
            connection.delete_table(table_name)
        
        print(f"🛠  Đang tạo bảng '{table_name}' với families: 'r' (rating), 't' (time)...")
        connection.create_table(table_name, {'r': dict(), 't': dict()})
        
        table = connection.table(table_name)

        # 2. ĐỌC FILE VÀ NẠP DỮ LIỆU
        csv_file_path = os.path.join(config.DATA_DIR_LOCAL, config.RATINGS_FILE)
        print(f"📂 Reading file: {csv_file_path}")

        if not os.path.exists(csv_file_path):
            print(f"❌ Error: File not found at {csv_file_path}")
            return

        with open(csv_file_path, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            batch = table.batch(batch_size=1000)
            count = 0
            
            print("🚀 Bắt đầu nạp Rating + Timestamp vào HBase...")
            for row in reader:
                try:
                    user_id = str(int(row["userId"]))
                    movie_id = str(int(row["movieId"]))
                    rating = str(float(row["rating"]))
                    timestamp = str(int(row["timestamp"])) # Lấy cột timestamp

                    # RowKey: UserID
                    # Lưu Rating vào 'r:MovieID'
                    # Lưu Timestamp vào 't:MovieID'
                    batch.put(user_id.encode(), {
                        f"r:{movie_id}".encode(): rating.encode(),
                        f"t:{movie_id}".encode(): timestamp.encode()
                    })
                    
                    count += 1
                    if count % 10000 == 0:
                        print(f"Da load {count} ratings...")
                        
                except ValueError: continue
            
            batch.send()
            print(f"THANH CONG! Da load tong cong {count} ratings.")
            
        connection.close()

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()