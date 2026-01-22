import csv
import os

import happybase
from common import config


def main():
    connection = happybase.Connection(config.HBASE_HOST)
    table = connection.table(config.HBASE_TABLE_MOVIES)

    csv_path = os.path.join(config.DATA_DIR_LOCAL, config.LINKS_FILE)
    print(f"Dang doc file tu: {csv_path}")

    batch = table.batch()
    count = 0
    
    # Format links.csv: movieId, imdbId, tmdbId
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader) # Skip header
        
        for row in reader:
            if len(row) < 3: continue
            
            movie_id = row[0]
            imdb_id = row[1]
            tmdb_id = row[2]

            # Update vào bảng movies, Column Family 'info'
            batch.put(movie_id.encode('utf-8'), {
                b'info:imdbId': imdb_id.encode('utf-8'),
                b'info:tmdbId': tmdb_id.encode('utf-8')
            })
            
            count += 1
            if count % 5000 == 0:
                print(f"Da link {count} movies...")

    batch.send()
    connection.close()
    print(f"THANH CONG! Da cap nhat links cho {count} movies.")

if __name__ == "__main__":
    main()