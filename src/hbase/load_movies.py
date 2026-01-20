import csv
import os

import happybase
from common import config


def main():
    print(f"Dang ket noi HBase voi {config.HBASE_HOST}...")
    connection = happybase.Connection(config.HBASE_HOST)
    table = connection.table(config.HBASE_TABLE_MOVIES)

    csv_file_path = os.path.join(config.DATA_DIR_LOCAL, config.MOVIES_FILE)

    print(f"Dang doc file tu: {csv_file_path}")

    col_title = b'info' + b':title'
    col_genres = b'info' + b':genres'

    batch = table.batch(batch_size=1000) # [Tip] Thêm batch_size để tự gửi
    
    with open(csv_file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader) # Bỏ qua header
        
        count = 0
        for row in reader:
            movie_id = row[0]
            title = row[1]
            genres = row[2]
            
            # Put dữ liệu (đã encode sang bytes)
            batch.put(movie_id.encode('utf-8'), {
                col_title: title.encode('utf-8'),
                col_genres: genres.encode('utf-8')
            })
            
            count += 1
            if count % 1000 == 0:
                print(f"Da load {count} phim...")

    batch.send()
    connection.close()
    print(f"Hoan tat! Tong cong {count} phim da duoc load vao HBase.")

if __name__ == "__main__":
    main()