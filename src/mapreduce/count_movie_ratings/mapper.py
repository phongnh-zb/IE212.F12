#!/usr/bin/env python3
import sys

# Input: userId,movieId,rating,timestamp

for line in sys.stdin:
    line = line.strip()
    # Bỏ qua header
    if line.startswith('userId') or not line:
        continue
    
    parts = line.split(',')
    if len(parts) >= 2:
        movie_id = parts[1]
        # Output: movieId <tab> 1
        print(f"{movie_id}\t1")