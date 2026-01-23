#!/usr/bin/env python3
import csv
import sys


def mapper():
    # Đọc từ stdin (file movies.csv)
    reader = csv.reader(sys.stdin)
    
    for row in reader:
        # Bỏ qua dòng tiêu đề hoặc dòng lỗi
        if len(row) < 3:
            continue
        if row[0] == 'movieId': # Skip header
            continue
            
        # Cột genres là cột thứ 3 (index 2)
        genres_str = row[2]
        
        # Một phim có thể không có genre
        if not genres_str or genres_str == '(no genres listed)':
            continue
            
        # Tách các genre bằng dấu gạch đứng '|'
        # Ví dụ: "Action|Adventure" -> ["Action", "Adventure"]
        genres = genres_str.split('|')
        
        for genre in genres:
            # Output: Genre \t 1
            print(f"{genre}\t1")

if __name__ == "__main__":
    mapper()