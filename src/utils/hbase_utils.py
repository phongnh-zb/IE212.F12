# FILE: src/utils/hbase_utils.py
import os
import sys
from datetime import datetime

import happybase
import pandas as pd
from fpdf import FPDF

# --- SETUP PATH ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(project_root)

from configs import config


def load_ratings_from_hbase(spark):

    from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, LongType
    print("[HBASE] loading rating....")
    connection = None
    BATCH_SIZE = 10000
    schema = StructType([
        StructField("userId", IntegerType(), True),
        StructField("movieId", IntegerType(), True),
        StructField("rating", FloatType(), True),
        StructField("timestamp", LongType(), True),
    ])
    try:
        connection = happybase.Connection(host=config.HBASE_HOST, timeout=60000)
        table = connection.table(config.HBASE_TABLE_RATINGS)
        all_dfs = []
        batch_data = []
        user_count = 0
        total_ratings = 0

        for row_key, row_data in table.scan():
            user_id = int(row_key.decode('utf-8'))

            for col_key, col_val in row_data.items():
                col_family, col_qualifier = col_key.split(b':', 1)

                if col_family == b'r':
                    movie_id = int(col_qualifier.decode('utf-8'))
                    rating = float(col_val.decode('utf-8'))

                    ts_key = b't:' + col_qualifier
                    timestamp = int(row_data.get(ts_key, b'0').decode('utf-8'))

                    batch_data.append((user_id, movie_id, rating, timestamp))
                    total_ratings += 1
            user_count += 1

            if user_count % BATCH_SIZE == 0:
                if batch_data:
                    batch_df = spark.createDataFrame(batch_data, schema)
                    all_dfs.append(batch_df)
                    batch_data = []
                print(f'    ->Processed {user_count:,} users, {total_ratings:,} ratings...')
        if batch_data:
            batch_df = spark.createDataFrame(batch_data, schema)
            all_dfs.append(batch_df)

        connection.close()

        if not all_dfs:
            print("[Hbase] no rating found")
            return None

        print(f'    -> Merging {len(all_dfs)} batches...')
        result_df = all_dfs[0]
        for df in all_dfs[1:]:
            result_df = result_df.union(df)

        print(f"Hbase - loaded {total_ratings:,} ratings")
        return result_df
    except Exception as e:
        print(f"Hbase error - load_rating {e}")
        if connection:
            connection.close()
        return None

def load_movies_from_hbase(spark):
    print("Hbase - loading movies...")
    connection = None
    try:
        connection = happybase.Connection(host=config.HBASE_HOST, timeout=30000)
        table = connection.table(config.HBASE_TABLE_MOVIES)
        data = []
        for row_key, row_data in table.scan():
            movie_id = int(row_key.decode('utf-8'))
            title = row_data.get(b'info:title', b'Unknown').decode('utf-8')
            genres = row_data.get(b'info:genres', b'').decode('utf-8')

            data.append({
                'movieId': movie_id,
                'title': title,
                'genres': genres
            })
        connection.close()

        if not data:
            print("Hbase - No movies found")
            return None

        pdf = pd.DataFrame(data)
        df = spark.createDataFrame(pdf)
        print(f"Hbase - loaded {len(pdf)} movies")
        return df
    except Exception as e:
        print(f"HBase error - load_movie {e}")
        if connection:
            connection.close()
        return None


def load_tags_from_hbase(spark):

    from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType

    print("Hbase - loading tags...")
    connection = None
    BATCH_SIZE = 50000
    schema = StructType([
        StructField("userId", IntegerType(), True),
        StructField("movieId", IntegerType(), True),
        StructField("tag", StringType(), True),
        StructField("timestamp", LongType(), True),
    ])

    try:
        connection = happybase.Connection(host=config.HBASE_HOST, timeout=60000)
        table = connection.table(config.HBASE_TABLE_TAGS)

        all_dfs = []
        batch_data = []
        total_tags = 0

        for row_key, row_data in table.scan():
            key_str = row_key.decode('utf-8')

            user_id = None
            movie_id = None
            tag = None
            timestamp = 0

            if b'info:userId' in row_data:
                user_id = int(row_data.get(b'info:userId', b'').decode('utf-8'))
                movie_id = int(row_data.get(b'info:movieId', b'0').decode('utf-8'))
                tag = row_data.get(b'info:tag', b'').decode('utf-8')
                timestamp = int(row_data.get(b'info:timestamp', b'0').decode('utf-8'))
            elif '_' in key_str:
                parts = key_str.split('_')
                if len(parts) >= 2:
                    user_id = int(parts[0])
                    movie_id = int(parts[1])
                    tag = row_data.get(b'info:tag', b'').decode('utf-8')
                    timestamp = int(row_data.get(b'info:timestamp', b'0').decode('utf-8'))
                else:
                    continue

            else:
                user_id = int(key_str)
                for col_key, col_val in row_data.items():
                    if col_key.startswith(b'tag:'):
                        movie_id = int(col_key.split(b':')[1].decode('utf-8'))
                        tag = col_val.decode('utf-8')
                        timestamp = 0

                        if tag:
                            batch_data.append((user_id, movie_id, tag, timestamp))
                            total_tags += 1
                continue

            if tag:
                batch_data.append((user_id, movie_id, tag, timestamp))
                total_tags += 1

            if total_tags > 0 and total_tags % BATCH_SIZE == 0:
                batch_df = spark.createDataFrame(batch_data, schema)
                all_dfs.append(batch_df)
                batch_data = []
                print(f'    ->Processed {total_tags:,} tags...')
        if batch_data:
            batch_df = spark.createDataFrame(batch_data, schema)
            all_dfs.append(batch_df)
        connection.close()

        if not all_dfs:
            print("Hbase - No tags found")
            return None

        print(f"    -> Merging {len(all_dfs)} batches...")

        result_df = all_dfs[0]
        for df in all_dfs[1:]:
            result_df = result_df.union(df)

        print(f"    Loaded {total_tags:,} tags")

        return result_df
    except Exception as e:
        print(f"HBase error - load_tags {e}")
        if connection:
            connection.close()
        return None


def load_all_data_from_hbase(spark):
    print(f"Hbase - loading all data...")
    print("\n" + "-" * 50)
    df_ratings = load_ratings_from_hbase(spark)
    df_movies = load_movies_from_hbase(spark)
    df_tags = load_tags_from_hbase(spark)

    return df_ratings, df_movies, df_tags


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
            
    def get_font_path():
        possible_paths = [
            "fonts/DejaVuSans.ttf"
        ]
        for path in possible_paths:
            if os.path.exists(path):
                return path
        return None

    def get_recommendations(self, user_id, model_name=None):
        self.connect()
        results = []
        try:
            with self.pool.connection() as connection:
                rec_table = connection.table(config.HBASE_TABLE_RECS)
                row = rec_table.row(str(user_id).encode('utf-8'))
                
                # Determine column to use
                col_key = b'info:movieIds'
                if model_name:
                    col_key = f"info:{model_name}".lower().encode('utf-8')
                
                if not row or col_key not in row: return []
                
                raw_string = row[col_key].decode('utf-8')
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

    def save_model_metrics(self, model_name, metrics, is_raw_data=False):
        self.connect()
        try:
            with self.pool.connection() as connection:
                # Sử dụng tên bảng từ config
                table_name = config.HBASE_TABLE_MODEL_METRICS
                if table_name.encode() not in connection.tables():
                    print(f"⚠️ [HBase] Warning: Table '{table_name}' does not exist. Skipping save.")
                    return

                table = connection.table(table_name)
                row_key = model_name.encode()
                data_to_put = {}

                if is_raw_data:
                    # --- LOGIC MỚI: Xử lý dữ liệu thô (cho LATEST_RUN) ---
                    # Input metrics dạng: {'b:winner_model': 'als', 'b:rmse': 0.8973, ...}
                    # Key đã bao gồm Column Family.
                    for col_str, value in metrics.items():
                        # Encode key (ví dụ: 'b:winner_model' -> b'b:winner_model')
                        col_key_bytes = col_str.encode('utf-8')
                        # Encode value sang string rồi sang bytes
                        col_val_bytes = str(value).encode('utf-8')
                        data_to_put[col_key_bytes] = col_val_bytes
                else:
                    # --- LOGIC CŨ: Xử lý metrics chuẩn (cho các model thường) ---
                    # Input metrics dạng: {'rmse': 0.8973, 'mae': 0.685}
                    # Tự động gán vào Column Family 'info' và thêm timestamp.
                    data_to_put = {
                        b'info:rmse': str(metrics.get('rmse', 0.0)).encode(),
                        b'info:mae': str(metrics.get('mae', 0.0)).encode(),
                        b'info:updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S').encode()
                    }
                
                # Thực hiện ghi vào HBase
                table.put(row_key, data_to_put)
                print(f"✅ [HBase] Saved data for RowKey: '{model_name}'")

        except Exception as e:
            print(f"!!! [HBase Error - save_model_metrics] {e}")

    def get_all_model_metrics(self):
        """
        Lấy tất cả metrics của các model để hiển thị dashboard.
        Tự động xử lý lỗi dữ liệu và fallback giữa các column families.
        """
        self.connect()
        results = []
        try:
            with self.pool.connection() as connection:
                tables = [t.decode('utf-8') for t in connection.tables()]
                if config.HBASE_TABLE_MODEL_METRICS not in tables:
                    return []
                
                table = connection.table(config.HBASE_TABLE_MODEL_METRICS)
                for key, data in table.scan():
                    model_name = key.decode('utf-8')
                    
                    # Helper lấy giá trị an toàn từ nhiều family
                    def get_safe_val(col_name, default='0'):
                        # Ưu tiên lấy từ 'info' (cho các model lẻ), sau đó thử 'b' (cho LATEST_RUN)
                        val = data.get(f'info:{col_name}'.encode())
                        if val is None:
                            val = data.get(f'b:{col_name}'.encode())
                        return val.decode('utf-8') if val else default

                    try:
                        rmse_val = float(get_safe_val('rmse'))
                        mae_val = float(get_safe_val('mae'))
                        updated_at = get_safe_val('updated_at', '--')
                        if updated_at == '--': # Thử lấy timestamp nếu là LATEST_RUN
                            updated_at = get_safe_val('timestamp', '--')

                        results.append({
                            "model": model_name,
                            "rmse": rmse_val,
                            "mae": mae_val,
                            "updated_at": updated_at
                        })
                    except (ValueError, TypeError) as conv_err:
                        print(f"⚠️ [HBase] Lỗi chuyển đổi data cho model {model_name}: {conv_err}")
                        continue
            return results
        except Exception as e:
            print(f"!!! [HBase Error - get_all_model_metrics] {e}")
            self.pool = None
            return []
        
    def get_top_rated_movies(self, limit=10):
        """Lấy danh sách phim có lượt đánh giá cao nhất để vẽ chart Top 10"""
        self.connect()
        movies_data = []
        try:
            with self.pool.connection() as connection:
                table = connection.table(config.HBASE_TABLE_MOVIES)
                # Quét bảng movies để lấy cột rating_count từ kết quả MapReduce
                for key, data in table.scan():
                    count_bytes = data.get(b'stats:rating_count', b'0')
                    title_bytes = data.get(b'info:title', b'Unknown')
                    
                    count = int(count_bytes.decode('utf-8'))
                    if count > 0:
                        movies_data.append({
                            'title': title_bytes.decode('utf-8'), 
                            'count': count
                        })
            
            # Top phim phổ biến nhất
            import pandas as pd
            if not movies_data:
                return pd.DataFrame(columns=['title', 'count'])
            df = pd.DataFrame(movies_data).sort_values(by='count', ascending=False)
            return df.head(limit)
        except Exception as e:
            print(f"!!! [HBase Error - get_top_rated_movies] {e}")
            import pandas as pd
            return pd.DataFrame(columns=['title', 'count'])

    def get_rating_distribution(self):
        """
        Lấy phân bố số lượng theo mức điểm (0.5 - 5.0) từ HBase.
        Dữ liệu này được tính toán trước và lưu trong bảng thống kê.
        """
        # 1. Tên bảng và RowKey chứa dữ liệu thống kê đã tính trước
        # (Bạn nên đưa tên bảng vào file config thay vì hardcode)
        ROW_KEY = b'GLOBAL_DIST'
        COLUMN_FAMILY = b'info'

        data_points = []
        
        # Danh sách các mức rating chuẩn cần hiển thị để đảm bảo thứ tự
        expected_ratings = ["0.5", "1.0", "1.5", "2.0", "2.5", "3.0", "3.5", "4.0", "4.5", "5.0"]

        try:
            # 2. Sử dụng connection pool để kết nối an toàn
            # Giả định self.pool là happybase.ConnectionPool đã được khởi tạo
            with self.pool.connection() as connection:
                table = connection.table(config.HBASE_TABLE_RATING_STATS)
                
                # 3. Thực hiện Get (lấy 1 dòng duy nhất) - Rất nhanh
                row_data = table.row(ROW_KEY)

                # row_data sẽ là một dict dạng: {b'info:0.5': b'1200', b'info:1.0': b'3500', ...}
                
                if not row_data:
                    print(f"Warning: Không tìm thấy dữ liệu thống kê cho key {ROW_KEY}")
                    # Trả về danh sách rỗng với count 0 nếu chưa có dữ liệu
                    return [{"rating": r, "count": 0} for r in expected_ratings]

                # 4. Xử lý dữ liệu trả về
                for rating_str in expected_ratings:
                    # Tạo key để lookup trong dictionary kết quả (ví dụ: b'info:3.5')
                    hbase_col_key = f"{COLUMN_FAMILY.decode()}:{rating_str}".encode('utf-8')
                    
                    # Lấy giá trị count (dạng bytes), mặc định là b'0' nếu không có rating đó
                    count_bytes = row_data.get(hbase_col_key, b'0')
                    
                    # Convert bytes sang int
                    count_val = int(count_bytes.decode('utf-8'))
                    
                    data_points.append({
                        "rating": rating_str,
                        "count": count_val
                    })
                    
        except Exception as e:
            print(f"Error getting rating distribution from HBase: {e}")
            # Trong trường hợp lỗi, có thể trả về dữ liệu mặc định để không crash app
            return [{"rating": r, "count": 0} for r in expected_ratings]

        return data_points
    
    def get_system_overview(self):
        """
        Lấy số liệu tổng quan hệ thống.
        - Counts (Users, Movies, Ratings) -> từ bảng 'system_stats'
        - Metrics (RMSE, MAE) -> từ bảng 'model_metrics' (row ID: 'LATEST')
        """
        self.connect()

        # 1. LẤY TÊN CÁC BẢNG TỪ CONFIG
        STATS_TABLE = config.HBASE_TABLE_SYSTEM_STATS
        MODEL_METRICS_TABLE = config.HBASE_TABLE_MODEL_METRICS

        FAMILY = b'info'

        # Dữ liệu mặc định
        overview_data = {
            'user_count': 'N/A', 'movie_count': 'N/A', 'rating_count': 'N/A',
            'rmse_score': 'N/A', 'rmse_delta': None
        }

        if not self.pool: return overview_data

        try:
            with self.pool.connection() as connection:
                # ===================================================
                # 1. Lấy Counts từ system_stats
                # ===================================================
                if STATS_TABLE.encode() in connection.tables():
                    table_stats = connection.table(STATS_TABLE)
                    # RowKey cố định cho thống kê tổng quan
                    row_stats = table_stats.row(b'OVERVIEW')

                    if row_stats:
                        def get_fmt_int(col):
                            val = row_stats.get(f'{FAMILY.decode()}:{col}'.encode())
                            return f"{int(val):,}" if val and val.isdigit() else 'N/A'

                        overview_data['user_count'] = get_fmt_int('user_count')
                        overview_data['movie_count'] = get_fmt_int('movie_count')
                        overview_data['rating_count'] = get_fmt_int('rating_count')

                # ===================================================
                # 2. Lấy RMSE từ model_metrics
                # ===================================================
                if MODEL_METRICS_TABLE.encode() in connection.tables():
                    table_metrics = connection.table(MODEL_METRICS_TABLE)
                    # Giả định: Luôn có 1 row với key 'LATEST_RUN' chứa metrics model hiện tại
                    row_metrics = table_metrics.row(b'LATEST_RUN')

                    if row_metrics:
                        def get_str(col):
                            val = row_metrics.get(f'{FAMILY.decode()}:{col}'.encode())
                            return val.decode('utf-8') if val else 'N/A'

                        # Lấy RMSE hiện tại
                        overview_data['rmse_score'] = get_str('rmse')

                        # Tính toán Delta (Nếu có lưu rmse_prev)
                        rmse_prev_str = get_str('rmse_prev')
                        current_rmse_str = overview_data['rmse_score']

                        if current_rmse_str != 'N/A' and rmse_prev_str != 'N/A':
                            try:
                                delta = float(current_rmse_str) - float(rmse_prev_str)
                                # Format: dấu +/-, 4 số thập phân. Ví dụ: -0.0150
                                overview_data['rmse_delta'] = f"{delta:+.4f}"
                            except ValueError: pass
                    else:
                         print(f"ℹ️ Info: Chưa có dữ liệu model 'LATEST' trong bảng '{MODEL_METRICS_TABLE}'.")
                else:
                    print(f"⚠️ Warning: Bảng '{MODEL_METRICS_TABLE}' chưa được tạo.")


        except Exception as e:
            print(f"❌ Lỗi ngoại lệ khi lấy dữ liệu tổng quan: {e}")

        return overview_data
    
    def get_latest_run_info(self):
        """
        Lấy thông tin về model chiến thắng (Winner) trong lần train gần nhất.
        Đọc từ RowKey 'LATEST_RUN' trong bảng model_metrics.
        """
        self.connect()
        # Giá trị mặc định
        latest_info = {
            'winner_model': 'N/A',
            'rmse': 'N/A',
            'timestamp': 'N/A'
        }

        try:
            with self.pool.connection() as connection:
                # Kiểm tra bảng metrics có tồn tại không
                tables = [t.decode('utf-8') for t in connection.tables()]
                if config.HBASE_TABLE_MODEL_METRICS not in tables:
                    return latest_info
                
                table = connection.table(config.HBASE_TABLE_MODEL_METRICS)
                # RowKey đặc biệt ta đã quy ước
                row = table.row(b'LATEST_RUN')

                if row:
                    # Helper lấy dữ liệu an toàn (lưu ý family 'b' cho benchmark/info)
                    # Nếu lúc save bạn dùng 'info', hãy sửa 'b' thành 'info' ở đây
                    def get_val(col_name):
                        # Thử lấy từ family 'b' trước (như code training), nếu không có thử 'info'
                        val = row.get(f'b:{col_name}'.encode())
                        if not val:
                             val = row.get(f'info:{col_name}'.encode())
                        return val.decode('utf-8') if val else 'N/A'

                    latest_info['winner_model'] = get_val('winner_model').upper()
                    
                    rmse_str = get_val('rmse')
                    if rmse_str != 'N/A':
                        try:
                            # Làm tròn 4 chữ số
                            latest_info['rmse'] = f"{float(rmse_str):.4f}"
                        except:
                            latest_info['rmse'] = rmse_str
                    
                    latest_info['timestamp'] = get_val('timestamp')
        
        except Exception as e:
            print(f"!!! [HBase Error - get_latest_run_info] {e}")
            self.pool = None

        return latest_info
    
    def generate_pdf_report(self, metrics_data, genre_data, system_info):
        pdf = FPDF()
        pdf.add_page()
        
        # --- CẤU HÌNH FONT TIẾNG VIỆT (QUAN TRỌNG) ---
        # 1. Xác định đường dẫn file font
        # Đi từ file hiện tại (hbase_utils.py) -> ra utils -> ra src -> vào fonts
        font_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'fonts', 'DejaVuSans.ttf')
        
        # Kiểm tra file có tồn tại không
        if not os.path.exists(font_path):
            print(f"❌ Không tìm thấy font tại: {font_path}")
            # Fallback về font mặc định (sẽ lỗi font tiếng Việt nhưng không crash app)
            pdf.set_font("Arial", 'B', 16)
            pdf.cell(0, 10, txt="BAO CAO TONG QUAN (LOI FONT - THIEU FILE TTF)", ln=True, align='C')
            return bytes(pdf.output())

        # 2. Đăng ký font Unicode
        # 'DejaVu' là tên ta tự đặt để gọi sau này, uni=True bật chế độ Unicode
        pdf.add_font('DejaVu', '', font_path)
        
        # 3. Set font đã đăng ký
        pdf.set_font('DejaVu', '', 16)
        
        # --- NỘI DUNG BÁO CÁO ---
        pdf.cell(0, 10, txt="BÁO CÁO TỔNG QUAN HỆ THỐNG GỢI Ý PHIM THÔNG MINH", ln=True, align='C')
        
        pdf.set_font('DejaVu', '', 10)
        pdf.cell(0, 10, txt=f"Ngày xuất: {datetime.now().strftime('%d/%m/%Y %H:%M')}", ln=True, align='C')
        pdf.ln(5)

        # 1. QUY MÔ DỮ LIỆU
        # Lấy dữ liệu an toàn từ dict system_info
        u_cnt = system_info.get('user_count', 'N/A')
        m_cnt = system_info.get('movie_count', 'N/A')
        r_cnt = system_info.get('rating_count', 'N/A')

        pdf.set_font('DejaVu', '', 12)
        pdf.cell(0, 10, txt="1. Quy mô Dữ liệu (MovieLens Dataset):", ln=True)
        
        pdf.set_font('DejaVu', '', 11)
        pdf.cell(0, 8, txt=f"- Tổng số Ratings đã xử lý: {r_cnt}", ln=True)
        pdf.cell(0, 8, txt=f"- Tổng số Phim trong kho: {m_cnt}", ln=True)
        pdf.cell(0, 8, txt=f"- Số lượng người dùng: {u_cnt}", ln=True)
        pdf.ln(5)

        # 2. HIỆU NĂNG MÔ HÌNH
        pdf.set_font('DejaVu', '', 12)
        pdf.cell(0, 10, txt="2. Kết quả Huấn luyện và Đánh giá (Accuracy):", ln=True)
        
        pdf.set_font('DejaVu', '', 11)
        if metrics_data:
            for m in metrics_data:
                # Bỏ qua dòng LATEST_RUN
                if m.get('model') == 'LATEST_RUN': continue
                
                name = m.get('model', 'Unknown').upper()
                rmse = m.get('rmse', 0.0)
                mae = m.get('mae', 0.0)
                pdf.cell(0, 8, txt=f"- Model {name}: RMSE = {rmse:.4f} | MAE = {mae:.4f}", ln=True)
        else:
            pdf.cell(0, 8, txt="- Chưa có dữ liệu metrics.", ln=True)

        # 3. THỐNG KÊ THỂ LOẠI
        pdf.ln(5)
        pdf.set_font('DejaVu', '', 12)
        pdf.cell(0, 10, txt="3. Top Thể loại phổ biến:", ln=True)
        pdf.set_font('DejaVu', '', 11)
        
        if genre_data:
            for g in genre_data[:3]:
                pdf.cell(0, 8, txt=f"- {g['genre']}: {g['count']} phim", ln=True)

        # FOOTER
        pdf.ln(10)
        pdf.set_font('DejaVu', '', 10)
        pdf.multi_cell(0, 6, txt="Ghi chú: RMSE càng thấp thì độ chính xác dự báo càng cao.")

        return bytes(pdf.output())