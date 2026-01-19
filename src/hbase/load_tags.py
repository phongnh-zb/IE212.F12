import csv
import os

import happybase
from common import config


def main():
    print(f"Connecting to HBase at {config.HBASE_HOST}...")
    connection = happybase.Connection(config.HBASE_HOST)
    table = connection.table(config.HBASE_TABLE_TAGS)

    csv_path = os.path.join(config.DATA_DIR_LOCAL, config.TAGS_FILE)
    print(f"Reading file: {csv_path}")

    batch = table.batch()
    count = 0
    
    # Format tags.csv: userId, movieId, tag, timestamp
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader) 
        
        for row in reader:
            if len(row) < 4: continue

            user_id = row[0]
            movie_id = row[1]
            tag = row[2]
            timestamp = row[3]

            # RowKey: userId_movieId_timestamp (đảm bảo duy nhất)
            row_key = f"{user_id}_{movie_id}_{timestamp}"

            batch.put(row_key.encode('utf-8'), {
                b'info:userId': user_id.encode('utf-8'),
                b'info:movieId': movie_id.encode('utf-8'),
                b'info:tag': tag.encode('utf-8'),
                b'info:timestamp': timestamp.encode('utf-8')
            })
            
            count += 1
            if count % 1000 == 0:
                print(f"Da load {count} tags...")

    batch.send()
    connection.close()
    print(f"THANH CONG! Da load tong cong {count} tags.")

if __name__ == "__main__":
    main()