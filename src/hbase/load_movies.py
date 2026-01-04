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

	batch = table.batch()
	with open(csv_file_path, 'r', encoding='utf-8') as f:
		reader = csv.reader(f)
		next(reader)
		
		count = 0
		for row in reader:
			movie_id = row[0]
			
			batch.put(movie_id.encode('utf-8'), {
				f'{config.HBASE_CF_INFO.decode()}:title'.encode(): row[1].encode(),
				f'{config.HBASE_CF_INFO.decode()}:genres'.encode(): row[2].encode()
			})
			
			count += 1
			if count % 1000 == 0:
				print(f"Da load {count} phim...")

	batch.send()
	connection.close()
	print(f"Hoan tat! Tong cong {count} phim da duoc load vao HBase.")

if __name__ == "__main__":
    main()