#!/usr/bin/env python3
import sys

current_genre = None
current_count = 0
genre = None

for line in sys.stdin:
    line = line.strip()
    
    try:
        genre, count = line.split('\t', 1)
        count = int(count)
    except ValueError:
        continue

    if current_genre == genre:
        current_count += count
    else:
        if current_genre:
            print(f"{current_genre}\t{current_count}")
        current_count = count
        current_genre = genre

if current_genre == genre:
    print(f"{current_genre}\t{current_count}")