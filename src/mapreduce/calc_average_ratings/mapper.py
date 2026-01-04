#!/usr/bin/env python3
import sys

# Input: userId,movieId,rating,timestamp
for line in sys.stdin:
    line = line.strip()
    if line.startswith('userId') or not line: continue
    parts = line.split(',')
    if len(parts) >= 3:
        print(f"{parts[1]}\t{parts[2]}") # movieId <tab> rating