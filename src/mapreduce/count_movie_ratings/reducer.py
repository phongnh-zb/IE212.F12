#!/usr/bin/env python3
import sys

current_movie = None
current_count = 0
movie_id = None

for line in sys.stdin:
    line = line.strip()
    
    try:
        movie_id, count = line.split('\t', 1)
        count = int(count)
    except ValueError:
        continue

    if current_movie == movie_id:
        current_count += count
    else:
        if current_movie:
            print(f"{current_movie}\t{current_count}")
        current_count = count
        current_movie = movie_id

if current_movie == movie_id:
    print(f"{current_movie}\t{current_count}")