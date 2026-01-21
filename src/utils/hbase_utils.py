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
        """
        Lấy danh sách phim gợi ý đầy đủ thông tin (Title, Genres, Rating).
        Flow:
        1. Vào bảng 'recommendations' -> Lấy danh sách ID phim (vd: "1,10,25")
        2. Vào bảng 'movies' -> Lấy chi tiết cho từng ID đó.
        """
        self.connect()
        
        # Kết quả trả về
        results = []

        try:
            with self.pool.connection() as connection:
                # --- BƯỚC 1: Lấy Movie IDs gợi ý ---
                rec_table = connection.table(config.HBASE_TABLE_RECS)
                
                # RowKey là userId (dạng string bytes)
                row = rec_table.row(str(user_id).encode('utf-8'))
                
                # Kiểm tra xem có dữ liệu không
                if not row or b'info:movieIds' not in row:
                    return []
                
                # Decode chuỗi "1,2,3"
                movie_ids_str = row[b'info:movieIds'].decode('utf-8')
                if not movie_ids_str:
                    return []
                    
                movie_ids = movie_ids_str.split(',')
                
                # --- BƯỚC 2: Lấy chi tiết phim (Batch Get) ---
                # Thay vì query 10 lần, ta query 1 lần lấy luôn 10 phim -> Rất nhanh
                movie_table = connection.table(config.HBASE_TABLE_MOVIES)
                
                # Row keys phải là bytes
                rows = movie_table.rows([mid.encode('utf-8') for mid in movie_ids])
                
                # --- BƯỚC 3: Format dữ liệu ---
                for mid_bytes, data in rows:
                    # Parse thông tin (Cần decode từ bytes sang string)
                    title = data.get(b'info:title', b'Unknown').decode('utf-8')
                    genres = data.get(b'info:genres', b'Unknown').decode('utf-8')
                    
                    # Lấy điểm rating trung bình (từ MapReduce job) nếu có
                    # Mặc định là 'N/A' nếu phim chưa được chấm điểm
                    avg_rating = data.get(b'stats:avg_rating', b'N/A').decode('utf-8')
                    
                    results.append({
                        "movieId": mid_bytes.decode('utf-8'),
                        "title": title,
                        "genres": genres,
                        "avg_rating": avg_rating
                    })
                    
            return results

        except Exception as e:
            # Log lỗi ra console để debug (Streamlit sẽ không hiện lỗi này lên UI)
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