# FILE: src/utils/hbase_utils.py
import os
import sys

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
        if not self.pool:
            self.pool = happybase.ConnectionPool(size=3, host=self.host, timeout=10000, autoconnect=True)

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
                
                movie_table = connection.table(config.HBASE_TABLE_MOVIES)
                rows = movie_table.rows([mid.encode('utf-8') for mid in movie_ids])
                movies_info = {k.decode(): v for k, v in rows}

                for mid, pred_score in rec_items:
                    data = movies_info.get(mid)
                    if data:
                        results.append({
                            "movieId": mid,
                            "title": data.get(b'info:title', b'Unknown').decode('utf-8'),
                            "genres": data.get(b'info:genres', b'Unknown').decode('utf-8'),
                            "avg_rating": data.get(b'stats:avg_rating', b'N/A').decode('utf-8'),
                            "pred_rating": pred_score
                        })
            return results
        except Exception as e:
            print(f"!!! [HBase Error] {e}")
            return []

    # [FIX] Hàm này đã được cập nhật để trả về đúng cột 'Recommendations (Details)'
    def scan_recommendations(self, limit=100):
        self.connect()
        results = []
        all_movie_ids = set()
        
        try:
            with self.pool.connection() as connection:
                rec_table = connection.table(config.HBASE_TABLE_RECS)
                movie_table = connection.table(config.HBASE_TABLE_MOVIES)
                
                # 1. Scan lấy list ID
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

                # 2. Lấy tên phim
                movie_map = {}
                if all_movie_ids:
                    movie_rows = movie_table.rows([mid.encode('utf-8') for mid in all_movie_ids])
                    for key, data in movie_rows:
                        movie_map[key.decode('utf-8')] = data.get(b'info:title', b'Unknown').decode('utf-8')

                # 3. Format dữ liệu (Tạo cột 'Recommendations (Details)' ở đây)
                for row in temp_rows:
                    formatted_recs = []
                    for mid, score in row['items']:
                        title = movie_map.get(mid, f"ID:{mid}")
                        formatted_recs.append(f"{title} ({float(score):.1f}★)")
                    
                    results.append({
                        "User ID": row['user_id'],
                        "Total": len(row['items']),
                        "Recommendations (Details)": " | ".join(formatted_recs) # <--- Key quan trọng
                    })
            return results
        except Exception as e:
            print(f"!!! [HBase Scan Error] {e}")
            return []
        
    def get_movie_details(self, movie_id):
        """
        Lấy thông tin chi tiết của 1 bộ phim cụ thể từ bảng 'movies'.
        Dùng cho tính năng 'Xem Chi Tiết'.
        """
        self.connect()
        try:
            with self.pool.connection() as connection:
                table = connection.table(config.HBASE_TABLE_MOVIES)
                row = table.row(str(movie_id).encode('utf-8'))
                
                if not row:
                    return None
                
                return {
                    "movieId": str(movie_id),
                    "title": row.get(b'info:title', b'Unknown').decode('utf-8'),
                    "genres": row.get(b'info:genres', b'Unknown').decode('utf-8'),
                    # Lấy thêm các chỉ số thống kê nếu có
                    "avg_rating": row.get(b'stats:avg_rating', b'N/A').decode('utf-8'),
                    "rating_count": row.get(b'stats:rating_count', b'0').decode('utf-8') # Nếu bạn có chạy MapReduce đếm số rating
                }
        except Exception as e:
            print(f"!!! [HBase Get Error] {e}")
            return None