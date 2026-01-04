#!/usr/bin/env python3
import sys

current_movie, current_sum, current_count = None, 0.0, 0

for line in sys.stdin:
    try:
        movie_id, rating = line.strip().split('\t', 1)
        rating = float(rating)
    except ValueError: continue

    if current_movie == movie_id:
        current_sum += rating
        current_count += 1
    else:
        if current_movie:
            print(f"{current_movie}\t{current_sum/current_count:.2f}")
        current_movie, current_sum, current_count = movie_id, rating, 1

if current_movie: print(f"{current_movie}\t{current_sum/current_count:.2f}")