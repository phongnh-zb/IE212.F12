#!/usr/bin/env python3
import sys


def reducer():
    current_genre = None
    current_count = 0
    
    for line in sys.stdin:
        try:
            line = line.strip()
            if not line: continue
            
            genre, count = line.split('\t', 1)
            count = int(count)
            
            if current_genre == genre:
                current_count += count
            else:
                if current_genre:
                    print(f"{current_genre}\t{current_count}")
                current_genre = genre
                current_count = count
        except ValueError:
            continue

    if current_genre:
        print(f"{current_genre}\t{current_count}")

if __name__ == "__main__":
    reducer()