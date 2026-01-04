import csv
import os

import happybase

script_dir = os.path.dirname(os.path.abspath(__file__))
csv_file_path = os.path.join(script_dir, '../../data/movies.csv')

connection = happybase.Connection('localhost')
table = connection.table('movies')

print(f"Dang doc file tu: {os.path.abspath(csv_file_path)}")
print("Dang ket noi HBase va batch load du lieu...")

batch = table.batch()

with open(csv_file_path, 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    next(reader) # Bỏ qua header
    
    count = 0
    for row in reader:
        if len(row) < 3: continue # Bỏ qua dòng lỗi
        
        movie_id = row[0]
        title = row[1]
        genres = row[2]
        
        batch.put(movie_id.encode('utf-8'), {
            b'info:title': title.encode('utf-8'),
            b'info:genres': genres.encode('utf-8')
        })
        
        count += 1
        if count % 1000 == 0:
            print(f"Da load {count} phim...")

batch.send()
connection.close()
print(f"Hoan tat! Tong cong {count} phim da duoc load vao HBase.")