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

def create_table_if_not_exists(connection, table_name):
    tables = [t.decode('utf-8') for t in connection.tables()]
    if table_name not in tables:
        print(f"tao table: {table_name}")
        connection.create_table(table_name, {'info': dict()})
        print(f"da tao table: {table_name}")
    else:
        print(f"da ton tai table: {table_name}")

def main():
    print(f"🔌 Kết nối HBase tại {config.HBASE_HOST}...")
    try:
        connection = happybase.Connection(config.HBASE_HOST, timeout=60000, autoconnect=True)
        create_table_if_not_exists(connection, config.HBASE_TABLE_TAGS)

        table = connection.table(config.HBASE_TABLE_TAGS)
        csv_file_path = os.path.join(config.DATA_DIR_LOCAL, config.TAGS_FILE)
        print(f"Dang doc file: {csv_file_path}")
        if not os.path.exists(csv_file_path):
            print(f"Khong tim thay file: {csv_file_path}")
            return

        col_userId = b'info:userId'
        col_movieId = b'info:movieId'
        col_tag = b'info:tag'
        col_timestamp = b'info:timestamp'
        with table.batch(batch_size=1000) as batch:
            with open(csv_file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader)

                count = 0
                for row in reader:
                    if len(row) < 4:
                        continue
                    user_id = row[0]
                    movie_id = row[1]
                    tag = row[2]
                    timestamp = row[3]
                    if not tag.strip():
                        continue
                    row_key = str(count).zfill(10).encode("utf-8")
                    batch.put(row_key, {
                        col_userId: user_id.encode("utf-8"),
                        col_movieId: movie_id.encode("utf-8"),
                        col_tag: tag.encode("utf-8"),
                        col_timestamp: timestamp.encode("utf-8")
                    })

                    count += 1
                    if count % 100000 == 0:
                        print(f"    -> Da Load {count:,} tags...")
        connection.close()
        print(f"✅ HOÀN TẤT! Đã load {count:,} tags")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()