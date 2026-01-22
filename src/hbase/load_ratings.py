import csv
import os
import sys

import happybase

# --- SETUP PATH ĐỂ IMPORT CONFIG ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(project_root)

from configs import config


def main():
    try:
        connection = happybase.Connection(config.HBASE_HOST, timeout=10000)
        table_name = config.HBASE_TABLE_RATINGS
        
        existing_tables = [t.decode('utf-8') for t in connection.tables()]
        
        if table_name in existing_tables:
            if connection.is_table_enabled(table_name):
                connection.disable_table(table_name)
            connection.delete_table(table_name)
        
        connection.create_table(table_name, {'r': dict()})
        
        table = connection.table(table_name)

        # 2. ĐƯỜNG DẪN FILE
        csv_file_path = os.path.join(config.DATA_DIR_LOCAL, config.RATINGS_FILE)
        print(f"Dang doc file tu: {csv_file_path}")

        # 3. ĐỌC FILE VÀ NẠP DỮ LIỆU
        # Sử dụng csv.DictReader để đọc file thành Dictionary (Tránh lỗi TypeError index)
        with open(csv_file_path, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile) 
            
            batch = table.batch(batch_size=1000)
            count = 0
            
            for row in reader:
                try:
                    # Lấy data an toàn từ DictReader
                    user_id = str(int(row["userId"]))
                    movie_id = str(int(row["movieId"]))
                    rating = str(float(row["rating"]))

                    # RowKey: UserID | Column: r:MovieID | Value: Rating
                    batch.put(user_id.encode(), {f"r:{movie_id}".encode(): rating.encode()})
                    
                    count += 1
                    if count % 10000 == 0:
                        print(f"Da load {count} ratings...")
                        
                except ValueError as e:
                    # Bỏ qua dòng lỗi data (nếu có)
                    continue
                except KeyError as e:
                    print(f"❌ Lỗi Header CSV: Không tìm thấy cột {e}. Kiểm tra file CSV của bạn.")
                    return
            
            batch.send()
            print(f"THANH CONG! Da load tong cong {count} ratings.")
            
        connection.close()

    except Exception as e:
        print(f"❌ Critical Error: {e}")

if __name__ == "__main__":
    main()