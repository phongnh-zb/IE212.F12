import os
import sys

import happybase

# --- SETUP PATH ---
# Đảm bảo import được config từ thư mục gốc
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(project_root)

from configs import config


class HBaseProvider:
    def __init__(self):
        self.host = config.HBASE_HOST
        self.pool = None
    
    def connect(self):
        """
        Tạo Connection Pool.
        Lợi ích: Giúp tái sử dụng kết nối, tránh việc App phải mở/đóng kết nối liên tục
        làm chậm hệ thống và gây quá tải cho Thrift Server.
        """
        if not self.pool:
            self.pool = happybase.ConnectionPool(
                size=3, 
                host=self.host, 
                timeout=10000,  # 10s timeout cho mỗi request
                autoconnect=True
            )

    def get_recommendations(self, user_id):
        self.connect()
        results = []

        try:
            with self.pool.connection() as connection:
                # 1. Lấy dữ liệu thô từ bảng recommendations
                rec_table = connection.table(config.HBASE_TABLE_RECS)
                row = rec_table.row(str(user_id).encode('utf-8'))
                
                if not row or b'info:movieIds' not in row:
                    return []
                
                # Decode: "1:4.5,10:3.8"
                raw_string = row[b'info:movieIds'].decode('utf-8')
                if not raw_string:
                    return []
                
                # Tách thành list các cặp [(1, 4.5), (10, 3.8)]
                rec_items = []
                movie_ids = []
                
                for item in raw_string.split(','):
                    try:
                        mid, pred_score = item.split(':')
                        rec_items.append((mid, pred_score))
                        movie_ids.append(mid)
                    except ValueError:
                        continue # Bỏ qua nếu lỗi format cũ
                
                # 2. Batch Get thông tin phim từ bảng movies
                movie_table = connection.table(config.HBASE_TABLE_MOVIES)
                rows = movie_table.rows([mid.encode('utf-8') for mid in movie_ids])
                
                # Tạo dictionary để map nhanh thông tin phim
                movies_info = {}
                for mid_bytes, data in rows:
                    movies_info[mid_bytes.decode('utf-8')] = data

                # 3. Gộp kết quả
                for mid, pred_score in rec_items:
                    data = movies_info.get(mid)
                    if data:
                        title = data.get(b'info:title', b'Unknown').decode('utf-8')
                        genres = data.get(b'info:genres', b'Unknown').decode('utf-8')
                        avg_rating = data.get(b'stats:avg_rating', b'N/A').decode('utf-8')
                        
                        results.append({
                            "movieId": mid,
                            "title": title,
                            "genres": genres,
                            "avg_rating": avg_rating,     # Điểm trung bình cộng đồng
                            "pred_rating": pred_score     # Độ phù hợp
                        })
                    
            return results

        except Exception as e:
            print(f"!!! [HBase Error] {e}")
            return []

    def get_movie_details(self, movie_id):
        """Hàm phụ trợ: Lấy thông tin chi tiết 1 phim (Dùng cho trang Search nếu cần)"""
        self.connect()
        try:
            with self.pool.connection() as connection:
                table = connection.table(config.HBASE_TABLE_MOVIES)
                row = table.row(str(movie_id).encode('utf-8'))
                
                if not row:
                    return None
                
                return {
                    "movieId": str(movie_id),
                    "title": row.get(b'info:title', b'').decode('utf-8'),
                    "genres": row.get(b'info:genres', b'').decode('utf-8'),
                    "avg_rating": row.get(b'stats:avg_rating', b'N/A').decode('utf-8')
                }
        except Exception:
            return None