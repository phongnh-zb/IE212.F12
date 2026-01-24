# FILE: src/utils/hbase_utils.py
import os
import sys
from datetime import datetime

import happybase

# --- SETUP PATH ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(project_root)

from configs import config


class HBaseProvider:
    def __init__(self):
        self.host = config.HBASE_HOST
        self.pool = None
    
    def connect(self):
        # Chỉ tạo pool nếu chưa có hoặc đã bị reset
        if not self.pool:
            print(f"🔌 [HBase] Connecting to {self.host}...")
            # Autoconnect=True giúp quản lý socket tốt hơn
            self.pool = happybase.ConnectionPool(size=3, host=self.host, timeout=30000, autoconnect=True)

    def get_recommendations(self, user_id):
        self.connect()
        results = []
        try:
            with self.pool.connection() as connection:
                rec_table = connection.table(config.HBASE_TABLE_RECS)
                row = rec_table.row(str(user_id).encode('utf-8'))
                if not row or b'info:movieIds' not in row: return []
                
                raw_string = row[b'info:movieIds'].decode('utf-8')
                rec_items = []
                movie_ids = []
                for item in raw_string.split(','):
                    try:
                        mid, pred_score = item.split(':')
                        rec_items.append((mid, pred_score))
                        movie_ids.append(mid)
                    except ValueError: continue
                
                if not movie_ids: return []

                movie_table = connection.table(config.HBASE_TABLE_MOVIES)
                rows = movie_table.rows([mid.encode('utf-8') for mid in movie_ids])
                movies_info = {k.decode(): v for k, v in rows}

                for mid, pred_score in rec_items:
                    data = movies_info.get(mid)
                    if data:
                        # Xử lý Avg Rating
                        avg_rating_bytes = data.get(b'stats:avg_rating')
                        avg_rating = float(avg_rating_bytes.decode('utf-8')) if avg_rating_bytes else 0.0
                        
                        results.append({
                            "movieId": mid,
                            "title": data.get(b'info:title', b'Unknown').decode('utf-8'),
                            "genres": data.get(b'info:genres', b'Unknown').decode('utf-8'),
                            "avg_rating": avg_rating,
                            "pred_rating": float(pred_score)
                        })
            return results
        except Exception as e:
            print(f"!!! [HBase Error - get_recommendations] {e}")
            self.pool = None # Reset pool để kết nối lại lần sau
            return []

    def get_movie_details(self, movie_id):
        self.connect()
        try:
            with self.pool.connection() as connection:
                table = connection.table(config.HBASE_TABLE_MOVIES)
                row = table.row(str(movie_id).encode('utf-8'))
                
                if not row: return None
                
                avg_rating_bytes = row.get(b'stats:avg_rating')
                rating_count_bytes = row.get(b'stats:rating_count')
                
                return {
                    'movieId': movie_id,
                    'title': row.get(b'info:title', b'Unknown').decode('utf-8'),
                    'genres': row.get(b'info:genres', b'--').decode('utf-8'),
                    'avg_rating': float(avg_rating_bytes.decode('utf-8')) if avg_rating_bytes else 0.0,
                    'rating_count': int(rating_count_bytes.decode('utf-8')) if rating_count_bytes else 0,
                    "tags": row.get(b'info:tags', b'').decode('utf-8')
                }
        except Exception as e:
            print(f"!!! [HBase Error - get_movie_details] {e}")
            self.pool = None # Reset pool
            return None

    # Hàm lấy danh sách rating của user (dạng dict)
    def get_user_ratings(self, user_id):
        self.connect()
        user_ratings = {}
        try:
            with self.pool.connection() as connection:
                table = connection.table(config.HBASE_TABLE_RATINGS)
                row = table.row(str(user_id).encode('utf-8'))
                if row:
                    for key, val in row.items():
                        if b':' in key:
                            fam, mid_bytes = key.split(b':', 1)
                            if fam == b'r': 
                                mid = mid_bytes.decode('utf-8')
                                rating = val.decode('utf-8')
                                user_ratings[mid] = rating
            return user_ratings
        except Exception as e:
            print(f"!!! [HBase Error - get_user_ratings] {e}")
            self.pool = None # Reset pool
            return {}

    # Hàm lấy lịch sử chi tiết cho Tab 2
    def get_user_history_detailed(self, user_id):
        self.connect()
        history = []
        ratings_map = {} 
        timestamps_map = {}
        try:
            with self.pool.connection() as connection:
                rating_table = connection.table(config.HBASE_TABLE_RATINGS)
                row = rating_table.row(str(user_id).encode('utf-8'))
                if row:
                    for key, val in row.items():
                        if b':' in key:
                            fam, mid_bytes = key.split(b':', 1)
                            mid = mid_bytes.decode('utf-8')
                            if fam == b'r':
                                ratings_map[mid] = float(val.decode('utf-8'))
                            elif fam == b't':
                                timestamps_map[mid] = int(val.decode('utf-8'))
                
                if not ratings_map: return []
                
                movie_ids = list(ratings_map.keys())
                movie_table = connection.table(config.HBASE_TABLE_MOVIES)
                movie_rows = movie_table.rows([m.encode('utf-8') for m in movie_ids])
                
                movie_info = {}
                for key, data in movie_rows:
                    mid = key.decode('utf-8')
                    movie_info[mid] = {
                        'title': data.get(b'info:title', b'Unknown').decode('utf-8'),
                        'genres': data.get(b'info:genres', b'--').decode('utf-8')
                    }
                
                for mid, rating in ratings_map.items():
                    info = movie_info.get(mid, {'title': f"ID:{mid}", 'genres': 'Unknown'})
                    ts = timestamps_map.get(mid, 0)
                    date_str = datetime.fromtimestamp(ts).strftime('%Y-%m-%d') if ts > 0 else "--"
                    
                    history.append({
                        "movieId": mid,
                        "title": info['title'],
                        "genres": info['genres'],
                        "rating": rating,
                        "date": date_str
                    })
            
            # Sắp xếp theo ngày mới nhất -> cũ nhất
            history.sort(key=lambda x: (x['date'], x['rating']), reverse=True)
            return history
        except Exception as e:
            print(f"!!! [HBase Error - get_user_history] {e}")
            self.pool = None # Reset pool
            return []

    def get_genre_stats(self):
        self.connect()
        data = []
        try:
            with self.pool.connection() as connection:
                tables = [t.decode('utf-8') for t in connection.tables()]
                if config.HBASE_TABLE_GENRE_STATS not in tables: return []
                table = connection.table(config.HBASE_TABLE_GENRE_STATS)
                for key, value in table.scan():
                    genre = key.decode('utf-8')
                    count_val = value.get(b'info:count')
                    if count_val:
                        data.append({"genre": genre, "count": int(count_val.decode('utf-8'))})
            data.sort(key=lambda x: x['count'], reverse=True)
            return data
        except Exception as e:
            print(f"!!! [HBase Error - get_genre_stats] {e}")
            self.pool = None # Reset pool
            return []

    def scan_recommendations(self, limit=100):
        self.connect()
        results = []
        all_movie_ids = set()
        try:
            with self.pool.connection() as connection:
                rec_table = connection.table(config.HBASE_TABLE_RECS)
                movie_table = connection.table(config.HBASE_TABLE_MOVIES)
                temp_rows = []
                for key, data in rec_table.scan(limit=limit):
                    user_id = key.decode('utf-8')
                    raw_val = data.get(b'info:movieIds', b'').decode('utf-8')
                    if raw_val:
                        items = []
                        for item in raw_val.split(','):
                            try:
                                mid, score = item.split(':')
                                items.append((mid, score))
                                all_movie_ids.add(mid)
                            except ValueError: continue
                        temp_rows.append({"user_id": user_id, "items": items})
                movie_map = {}
                if all_movie_ids:
                    movie_rows = movie_table.rows([mid.encode('utf-8') for mid in all_movie_ids])
                    for key, data in movie_rows:
                        movie_map[key.decode('utf-8')] = data.get(b'info:title', b'Unknown').decode('utf-8')
                for row in temp_rows:
                    formatted_recs = []
                    for mid, score in row['items']:
                        title = movie_map.get(mid, f"ID:{mid}")
                        formatted_recs.append(f"{title} ({float(score):.1f}★)")
                    results.append({
                        "User ID": row['user_id'],
                        "Total": len(row['items']),
                        "Recommendations (Details)": " | ".join(formatted_recs)
                    })
            return results
        except Exception as e:
            print(f"!!! [HBase Error - scan_recommendations] {e}")
            self.pool = None # Reset pool
            return []