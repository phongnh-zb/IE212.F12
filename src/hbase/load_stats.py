import os
import subprocess
import sys

import happybase
import pandas as pd  # <--- CẦN IMPORT PANDAS

# Setup đường dẫn
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(project_root)

from configs import config


def get_hbase_connection():
    try:
        # Tăng timeout nếu nạp dữ liệu lớn
        connection = happybase.Connection(config.HBASE_HOST, port=9090, timeout=60000, autoconnect=True)
        return connection
    except Exception as e:
        print(f"❌ Lỗi kết nối HBase: {e}")
        return None

# --- JOB 1 & 2: Nạp output của MapReduce từ HDFS vào bảng movies ---
def load_from_hdfs(connection, hdfs_path, column_family, column_name, description):
    print(f"\n🚀 Bắt đầu nạp '{description}' từ HDFS: {hdfs_path}")
    
    # Kiểm tra bảng tồn tại
    if config.HBASE_TABLE_MOVIES.encode() not in connection.tables():
        print(f"❌ Lỗi: Bảng '{config.HBASE_TABLE_MOVIES}' không tồn tại.")
        return

    table = connection.table(config.HBASE_TABLE_MOVIES)
    
    # Lệnh đọc file từ HDFS
    hdfs_cmd = f"hdfs dfs -cat {hdfs_path}/part-*"
    
    try:
        process = subprocess.Popen(hdfs_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        batch_size = 1000 # Tăng batch size lên một chút để nhanh hơn
        batch = table.batch(batch_size=batch_size)
        count = 0
        
        print("   -> Đang đọc stream từ HDFS và ghi vào HBase...")
        for line in process.stdout:
            try:
                line_str = line.decode('utf-8').strip()
                if not line_str: continue

                # Tách bằng dấu TAB (\t) do MapReduce xuất ra
                parts = line_str.split('\t')
                
                # Fallback dấu phẩy (đề phòng)
                if len(parts) < 2:
                    parts = line_str.split(',')
                
                if len(parts) >= 2:
                    movie_id = parts[0].strip()
                    value = parts[1].strip()
                    
                    # Ghi vào HBase
                    col_key = f"{column_family}:{column_name}".encode()
                    
                    batch.put(movie_id.encode(), {
                        col_key: value.encode()
                    })
                    count += 1
                    
                    if count % 5000 == 0:
                        print(f"   -> Đã xử lý {count} dòng...", end='\r')
                        
            except Exception as e:
                continue

        batch.send()
        print(f"\n✅ HOÀN TẤT '{description}'! Tổng cộng: {count} dòng được cập nhật.")
        
    except Exception as e:
        print(f"❌ Lỗi khi đọc HDFS hoặc ghi batch: {e}")

# --- JOB 3: Tính toán phân bố rating từ CSV và nạp vào bảng rating_stats ---
def load_rating_distribution_from_csv(connection):
    description = "Phân Bố Rating Toàn Cục"
    csv_path = os.path.join(config.DATA_DIR_LOCAL, config.RATINGS_FILE)
    # Giả sử bạn đã định nghĩa HBASE_TABLE_RATING_STATS trong config
    table_name = config.HBASE_TABLE_RATING_STATS
    row_key = b'GLOBAL_DIST'
    cf = b'info'

    print(f"\n🚀 Bắt đầu tính toán và nạp '{description}' từ file CSV local: {csv_path}")

    # 1. Kiểm tra file CSV
    if not os.path.exists(csv_path):
        print(f"❌ Lỗi: Không tìm thấy file CSV tại '{csv_path}'. Kiểm tra lại config.")
        return

    # 2. Kiểm tra bảng HBase
    if table_name.encode() not in connection.tables():
         print(f"❌ Lỗi: Bảng '{table_name}' chưa tồn tại. Vui lòng tạo bảng trong HBase Shell trước: create '{table_name}', '{cf.decode()}'")
         return

    try:
        # 3. Dùng Pandas để tính toán nhanh
        print("   -> Đang đọc CSV và tính toán bằng Pandas (có thể mất vài giây)...")
        # Chỉ đọc cột 'rating' để tiết kiệm RAM
        df = pd.read_csv(csv_path, usecols=['rating'])
        # Đếm số lượng (value_counts) và sắp xếp theo index (mức rating)
        rating_counts = df['rating'].value_counts().sort_index()

        print(f"   -> Đã tính xong. Tìm thấy {len(rating_counts)} mức rating khác nhau.")

        # 4. Chuẩn bị dữ liệu để Put vào HBase
        table = connection.table(table_name)
        hbase_data = {}
        for rating_val, count in rating_counts.items():
            rating_str = str(rating_val)
            col_key = f"{cf.decode()}:{rating_str}".encode('utf-8')
            col_value = str(count).encode('utf-8')
            hbase_data[col_key] = col_value

        # 5. Thực hiện 1 lệnh Put duy nhất
        print(f"   -> Đang ghi dữ liệu vào bảng '{table_name}' với RowKey '{row_key.decode()}'...")
        table.put(row_key, hbase_data)
        print(f"✅ HOÀN TẤT '{description}'!")

    except Exception as e:
        print(f"❌ Lỗi khi xử lý CSV hoặc ghi HBase: {e}")

# --- JOB 4 (MỚI): Tính toán tổng quan hệ thống và nạp vào bảng system_stats ---
def load_system_overview_from_csv(connection):
    description = "Tổng Quan Hệ Thống (Users, Movies, Ratings)"
    movies_csv_path = os.path.join(config.DATA_DIR_LOCAL, config.MOVIES_FILE)
    ratings_csv_path = os.path.join(config.DATA_DIR_LOCAL, config.RATINGS_FILE)

    # CẦN ĐẢM BẢO BIẾN NÀY CÓ TRONG FILE CONFIG CỦA BẠN
    table_name = config.HBASE_TABLE_SYSTEM_STATS
    row_key = b'OVERVIEW'
    cf = b'info'

    print(f"\n🚀 Bắt đầu tính toán và nạp '{description}' từ các file CSV local...")

    # 1. Kiểm tra các file CSV
    if not os.path.exists(movies_csv_path) or not os.path.exists(ratings_csv_path):
        print(f"❌ Lỗi: Không tìm thấy file movies.csv hoặc ratings.csv tại thư mục '{config.DATA_DIR_LOCAL}'.")
        return

    # 2. Kiểm tra bảng HBase
    if table_name.encode() not in connection.tables():
         print(f"❌ Lỗi: Bảng '{table_name}' chưa tồn tại. Vui lòng tạo bảng trong HBase Shell trước: create '{table_name}', '{cf.decode()}'")
         return

    try:
        print("   -> Đang đọc CSV và tính toán tổng quan bằng Pandas...")

        # a. Đếm số phim (Chỉ cần đọc 1 cột bất kỳ để đếm dòng)
        movies_df = pd.read_csv(movies_csv_path, usecols=['movieId'])
        movie_count = len(movies_df)
        print(f"      - Tổng số phim: {movie_count:,}")

        # b. Đếm số rating và user (Chỉ đọc các cột cần thiết)
        ratings_df = pd.read_csv(ratings_csv_path, usecols=['userId', 'rating'])
        rating_count = len(ratings_df)
        user_count = ratings_df['userId'].nunique() # Đếm số user duy nhất
        print(f"      - Tổng lượt đánh giá: {rating_count:,}")
        print(f"      - Tổng người dùng (unique): {user_count:,}")

        # 3. Chuẩn bị dữ liệu để Put vào HBase
        table = connection.table(table_name)

        # Dữ liệu phải được encode sang bytes
        data_to_put = {
            f'{cf.decode()}:user_count'.encode(): str(user_count).encode(),
            f'{cf.decode()}:movie_count'.encode(): str(movie_count).encode(),
            f'{cf.decode()}:rating_count'.encode(): str(rating_count).encode(),
        }

        # 4. Thực hiện 1 lệnh Put duy nhất
        print(f"   -> Đang ghi dữ liệu vào bảng '{table_name}' với RowKey '{row_key.decode()}'...")
        table.put(row_key, data_to_put)

        print(f"✅ HOÀN TẤT '{description}'!")

    except Exception as e:
        print(f"❌ Lỗi khi tính toán tổng quan hoặc ghi HBase: {e}")


def load_genre_stats(connection):
    """Nạp kết quả thống kê thể loại từ HDFS vào bảng genre_stats"""
    print(f"\n🚀 Bắt đầu nạp 'Thống Kê Thể Loại' từ: {config.HDFS_OUTPUT_GENRES}")

    table = connection.table(config.HBASE_TABLE_GENRE_STATS)
    hdfs_cmd = f"hdfs dfs -cat {config.HDFS_OUTPUT_GENRES}/part-*"

    try:
        process = subprocess.Popen(hdfs_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        batch = table.batch(batch_size=100)
        count = 0

        for line in process.stdout:
            try:
                line_str = line.decode('utf-8').strip()
                if not line_str: continue

                parts = line_str.split('\t')
                if len(parts) < 2:
                    parts = line_str.split(',')

                if len(parts) >= 2:
                    genre = parts[0].strip()
                    count_val = parts[1].strip()

                    batch.put(genre.encode(), {
                        b'info:count': count_val.encode()
                    })
                    count += 1

            except Exception as e:
                print(f"Lỗi xử lý dòng: {e}")
                continue

        batch.send()
        print(f"✅ HOÀN TẤT 'Thống Kê Thể Loại'! Tổng cộng: {count} thể loại.")

    except Exception as e:
        print(f"❌ Lỗi khi đọc HDFS: {e}")

def main():
    # Tạo kết nối một lần và dùng chung
    conn = get_hbase_connection()
    if not conn: return

    try:
        # --- CÁC JOB CŨ (Đọc từ HDFS MR Output) ---
        # 1. NẠP AVG RATING
        load_from_hdfs(
            conn, 
            config.HDFS_OUTPUT_AVG, 
            "stats", 
            "avg_rating", 
            "Điểm Cộng Đồng (Avg Rating)"
        )

        # 2. NẠP RATING COUNT
        load_from_hdfs(
            conn, 
            config.HDFS_OUTPUT_RATINGS, 
            "stats", 
            "rating_count", 
            "Số Lượt Đánh Giá (Rating Count)"
        )

        # --- CÁC JOB MỚI (Đọc từ CSV Local để thống kê nhanh) ---
        # 3. NẠP RATING DISTRIBUTION
        load_rating_distribution_from_csv(conn)

        # 4. NẠP SYSTEM OVERVIEW (MỚI THÊM)
        load_system_overview_from_csv(conn)

        # 3. NẠP GENRE STATS (Phân bố thể loại)
        # MapReduce Job: Count Genres
        load_genre_stats(conn)

    finally:
        # Luôn đóng kết nối cuối cùng
        if conn:
            conn.close()
            print("\n🔌 Đã đóng kết nối HBase.")

if __name__ == "__main__":
    main()