import os
import sys

import happybase
from pyspark.sql import SparkSession
from pyspark.sql.types import (FloatType, IntegerType, LongType, StructField,
                               StructType)

# --- SETUP PATH ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

from configs import config
from src.models.als_recommender import ALSRecommender


def get_hbase_conn():
    """Hàm tạo kết nối HBase (được gọi bên trong từng Worker của Spark)"""
    return happybase.Connection(config.HBASE_HOST, timeout=10000)

def save_partition_to_hbase(iterator):
    """
    Hàm này chạy trên từng Partition của RDD để ghi dữ liệu xuống HBase.
    Mỗi partition mở 1 kết nối để tối ưu hiệu năng.
    """
    conn = None
    try:
        conn = get_hbase_conn()
        table = conn.table(config.HBASE_TABLE_RECS)
        batch = table.batch(batch_size=1000)
        
        # Tên cột chứa danh sách gợi ý
        col_family = b'info:movieIds'

        for row in iterator:
            # row dạng: (userId, recommendations=[Row(movieId, rating), ...])
            user_id = str(row.userId)
            
            clean_recs = []
            
            # [LOGIC QUAN TRỌNG] Xử lý từng gợi ý
            for rec in row.recommendations:
                # 1. Lấy điểm dự đoán thô
                raw_rating = float(rec.rating)
                
                # 2. CẮT NGỌN (CLIPPING): Ép về khoảng [0.0, 5.0]
                clean_rating = max(0.0, min(5.0, raw_rating))
                
                # 3. Format chuỗi "MovieID:Rating"
                clean_recs.append(f"{rec.movieId}:{clean_rating:.2f}")

            # Nối lại thành chuỗi ngăn cách bởi dấu phẩy
            rec_string = ",".join(clean_recs)
            
            # Đưa vào Batch
            batch.put(user_id.encode(), {col_family: rec_string.encode()})

        # Gửi dữ liệu đi
        batch.send()
        
    except Exception as e:
        print(f"❌ Error writing to HBase: {e}")
    finally:
        if conn:
            conn.close()

def main():
    print("🚀 Starting Spark ALS Training Pipeline...")
    
    # 1. Khởi tạo Spark Session
    spark = SparkSession.builder \
        .appName("MovieLens_ALS_Training") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 2. Đọc dữ liệu từ FILE CSV
    raw_csv_path = os.path.join(config.DATA_DIR_LOCAL, config.RATINGS_FILE)
    
    # [FIX QUAN TRỌNG] Thêm prefix 'file://' để Spark biết đây là local file
    csv_path = f"file://{raw_csv_path}"
    
    if not os.path.exists(raw_csv_path):
        print(f"❌ File not found: {raw_csv_path}")
        return

    print(f"📂 Reading raw data from: {csv_path}")
    
    # Định nghĩa Schema rõ ràng để tối ưu hiệu năng
    schema = StructType([
        StructField("userId", IntegerType(), True),
        StructField("movieId", IntegerType(), True),
        StructField("rating", FloatType(), True),
        StructField("timestamp", LongType(), True)
    ])
    
    df_ratings = spark.read.csv(csv_path, header=True, schema=schema)
    
    # Cache dữ liệu để train nhanh hơn
    df_ratings.cache()
    print(f"📊 Training data loaded. Total rows: {df_ratings.count()}")

    # 3. Train Model
    print("🧠 Training ALS Model...")
    recommender = ALSRecommender(spark)
    recommender.train(df_ratings)
    
    # 4. Tạo gợi ý (Top 10 phim cho MỌI User)
    print("🔮 Generating recommendations for all users...")
    user_recs = recommender.get_recommendations(k=10)
    
    # 5. Lưu vào HBase (Sử dụng hàm foreachPartition)
    print(f"💾 Saving recommendations to HBase table: {config.HBASE_TABLE_RECS}...")
    
    # Kiểm tra bảng có tồn tại không trước khi ghi (Optional)
    try:
        tmp_conn = happybase.Connection(config.HBASE_HOST)
        # Fix lỗi decode nếu tên bảng dạng bytes
        tables = [t.decode('utf-8') for t in tmp_conn.tables()]
        if config.HBASE_TABLE_RECS not in tables:
            print(f"🛠 Creating table {config.HBASE_TABLE_RECS}...")
            tmp_conn.create_table(config.HBASE_TABLE_RECS, {'info': dict()})
        tmp_conn.close()
    except Exception as e:
        print(f"⚠️ Warning checking table: {e}")

    # Ghi dữ liệu phân tán
    user_recs.foreachPartition(save_partition_to_hbase)
    
    print("✅ Training Pipeline Completed Successfully!")
    spark.stop()

if __name__ == "__main__":
    main()