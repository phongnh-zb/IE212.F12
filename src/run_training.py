import argparse
import os
import sys
import time

import happybase
from pyspark.sql import SparkSession
from pyspark.sql.types import (FloatType, IntegerType, LongType, StringType,
                               StructField, StructType)

# --- SETUP PATH ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

from configs import config
# --- IMPORT MODELS ---
from src.models.als_recommender import ALSRecommender
from src.models.content_based_recommender import ContentBasedRecommender
from src.models.hybrid_recommender import HybridRecommender


def save_partition_to_hbase(iterator, table_name, column_family):
    """
    Worker ghi dữ liệu vào HBase với cơ chế 'Smart Retry'.
    [UPDATE]: Lưu format 'ID:Rating' để hiển thị độ phù hợp.
    """
    BATCH_SIZE = 50
    MAX_RETRIES = 5
    RETRY_SLEEP = 5
    CONN_TIMEOUT = 120000 

    def get_conn():
        return happybase.Connection(config.HBASE_HOST, timeout=CONN_TIMEOUT, autoconnect=True)

    connection = None
    table = None

    try:
        connection = get_conn()
        table = connection.table(table_name)
        batch_data = []
        col_name = b'info:movieIds'

        for row in iterator:
            clean_recs = []
            
            for r in row.recommendations:
                # 1. Lấy giá trị rating thô từ thuật toán
                raw_rating = float(r.rating)
                
                # 2. Ep về 0.0 - 5.0
                clean_rating = max(0.0, min(5.0, raw_rating))
                
                # 3. Format chuỗi "ID:Rating"
                clean_recs.append(f"{r.movieId}:{clean_rating:.2f}")

            # Nối lại thành chuỗi để lưu HBase
            # Ví dụ: "1:4.50,296:5.00,..."
            rec_data = ",".join(clean_recs)
            
            batch_data.append((str(row.userId).encode(), {col_name: rec_data.encode()}))
            
            if len(batch_data) >= BATCH_SIZE:
                for attempt in range(MAX_RETRIES):
                    try:
                        batch = table.batch(batch_size=BATCH_SIZE)
                        for k, v in batch_data:
                            batch.put(k, v)
                        batch.send()
                        break 
                    except Exception as e:
                        time.sleep(RETRY_SLEEP)
                        try: connection.close()
                        except: pass
                        try:
                            connection = get_conn()
                            table = connection.table(table_name)
                        except: pass
                batch_data = []

        if batch_data:
            try:
                batch = table.batch(batch_size=len(batch_data))
                for k, v in batch_data:
                    batch.put(k, v)
                batch.send()
            except Exception: pass

    except Exception as e:
        print(f"!!! [FATAL Worker Error] {e}")
    finally:
        if connection:
            try: connection.close()
            except: pass

def run_single_model(spark, model_type, df_ratings, df_movies):
    """Hàm chạy logic cho 1 model cụ thể"""
    print(f"\n>>> [TRAINING] Đang chạy Model: {model_type.upper()}...")
    df_recs = None
    
    if model_type == "als":
        recommender = ALSRecommender(spark)
        recommender.train(df_ratings)
        df_recs = recommender.get_recommendations(k=10)
        
    elif model_type == "cbf":
        recommender = ContentBasedRecommender(spark)
        recommender.train(df_ratings, df_movies)
        df_recs = recommender.get_recommendations(k=10)
        
    elif model_type == "hybrid":
        recommender = HybridRecommender(spark)
        recommender.train(df_ratings, df_movies)
        df_recs = recommender.get_recommendations(k=10)
        
    # Lưu vào HBase
    if df_recs:
        print(f">>> [SAVING] Lưu kết quả {model_type.upper()} vào HBase...")
        df_recs.coalesce(1).foreachPartition(
            lambda iter: save_partition_to_hbase(iter, config.HBASE_TABLE_RECS, model_type)
        )
        print(f">>> [DONE] Hoàn tất {model_type.upper()}.")
    else:
        print(f">>> [SKIP] Model {model_type} không trả về kết quả.")

def main(args_model):
    spark = SparkSession.builder \
        .appName("MovieLens_Unified_Pipeline") \
        .master(config.SPARK_MASTER) \
        .config("spark.hadoop.validateOutputSpecs", "false") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 1. LOAD DATA (Chỉ làm 1 lần duy nhất để tiết kiệm RAM)
    print(">>> [INIT] Loading Data from HDFS...")
    
    schema_ratings = StructType([
        StructField("userId", IntegerType()), 
        StructField("movieId", IntegerType()), 
        StructField("rating", FloatType()), 
        StructField("timestamp", LongType())
    ])
    df_ratings = spark.read.csv(f"{config.HDFS_BASE_PATH}/{config.RATINGS_FILE}", header=True, schema=schema_ratings).cache()

    schema_movies = StructType([
        StructField("movieId", IntegerType()),
        StructField("title", StringType()),
        StructField("genres", StringType())
    ])
    df_movies = spark.read.csv(f"{config.HDFS_BASE_PATH}/{config.MOVIES_FILE}", header=True, schema=schema_movies).cache()
    
    print(f">>> Data Loaded. Ratings: {df_ratings.count()}, Movies: {df_movies.count()}")

    # 2. XÁC ĐỊNH DANH SÁCH MODEL CẦN CHẠY
    if args_model == "all":
        # Chạy lần lượt cả 3
        models_to_run = ["als", "cbf", "hybrid"]
    else:
        models_to_run = [args_model]

    # 3. CHẠY VÒNG LẶP
    for model in models_to_run:
        run_single_model(spark, model, df_ratings, df_movies)

    print("\n>>> ALL TASKS FINISHED SUCCESSFULLY!")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # Thêm lựa chọn 'all'
    parser.add_argument("--model", type=str, default="all", choices=["als", "cbf", "hybrid", "all"], help="Choose model to run")
    args = parser.parse_args()
    main(args.model)