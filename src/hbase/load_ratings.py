import csv
import os

import happybase
from common import config


def main():
    print(f"Connecting to HBase at {config.HBASE_HOST}...")
    connection = happybase.Connection(config.HBASE_HOST)
    table = connection.table(config.HBASE_TABLE_RATINGS)

    csv_path = os.path.join(config.DATA_DIR_LOCAL, config.RATINGS_FILE)
    print(f"Reading file: {csv_path}")

    batch = table.batch(batch_size=2000) # Tăng batch size vì file này rất lớn
    count = 0
    
    # Format ratings.csv: userId, movieId, rating, timestamp
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader) 
        
        for row in reader:
            user_id = row[0]
            movie_id = row[1]
            rating = row[2]
            timestamp = row[3]

            # RowKey: userId_movieId (để dễ dàng tìm kiếm)
            row_key = f"{user_id}_{movie_id}"

            batch.put(row_key.encode('utf-8'), {
                b'info:userId': user_id.encode('utf-8'),
                b'info:movieId': movie_id.encode('utf-8'),
                b'info:rating': rating.encode('utf-8'),
                b'info:timestamp': timestamp.encode('utf-8')
            })
            
            count += 1
            if count % 10000 == 0:
                print(f"Da load {count} ratings...")

    batch.send()
    connection.close()
    print(f"THANH CONG! Da load tong cong {count} ratings.")

if __name__ == "__main__":
    main()