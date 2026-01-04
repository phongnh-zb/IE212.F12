#!/usr/bin/env python3
import csv
import sys

# Input: movies.csv
# Định dạng: movieId,title,genres

# Sử dụng csv.reader để xử lý trường hợp tên phim có dấu phẩy
reader = csv.reader(sys.stdin)

for row in reader:
    # Bỏ qua dòng header hoặc dòng lỗi
    if not row or row[0] == 'movieId':
        continue
    
    # Cột genres thường là cột cuối cùng (index 2)
    if len(row) >= 3:
        genres_string = row[2]
        genres = genres_string.split('|')
        
        for genre in genres:
            # Output: Genre <tab> 1
            print(f"{genre}\t1")