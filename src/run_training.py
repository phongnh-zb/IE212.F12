import argparse
import os
import sys
import time

# Import Spark
from pyspark.sql import SparkSession
from pyspark.sql.types import (FloatType, IntegerType, LongType, StringType,
                               StructField, StructType)

# --- SETUP PATH ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.append(project_root)

from configs import config
# --- IMPORT MODELS ---
from src.models.als_recommender import ALSRecommender
from src.models.content_based_recommender import ContentBasedRecommender
from src.models.hybrid_recommender import HybridRecommender

# ==============================================================================
# 1. CÁC HÀM WORKER
# ==============================================================================

def worker_save_recs(iterator, table_name):
    """
    Worker lưu User Recommendations vào HBase.
    """
    import happybase
    
    BATCH_SIZE = 1000
    connection = None
    try:
        connection = happybase.Connection(config.HBASE_HOST, timeout=60000, autoconnect=True)
        table = connection.table(table_name)
        batch = table.batch(batch_size=BATCH_SIZE)
        col_name = b'info:movieIds'

        count = 0
        for row in iterator:
            if not hasattr(row, 'recommendations'): continue
            
            clean_recs = []
            for r in row.recommendations:
                val = max(0.0, min(5.0, float(r.rating)))
                clean_recs.append(f"{r.movieId}:{val:.2f}")

            if clean_recs:
                rec_str = ",".join(clean_recs)
                batch.put(str(row.userId).encode(), {col_name: rec_str.encode()})
                count += 1

        batch.send()
    except Exception as e:
        print(f"!!! [Worker Error] {e}")
    finally:
        if connection:
            try: connection.close()
            except: pass

# ==============================================================================
# 2. CÁC HÀM QUẢN LÝ
# ==============================================================================

def run_single_model(spark, model_type, df_ratings, df_movies):
    print(f"\n>>> [TRAINING] Đang chạy Model: {model_type.upper()}...")
    df_recs = None
    
    # Đăng ký bảng tạm để Hybrid Model có thể gọi lại
    df_ratings.createOrReplaceTempView("ratings")
    
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
        
    if df_recs:
        print(f">>> [CACHING] Đang tính toán kết quả cuối cùng cho {model_type.upper()}...")
        df_recs.cache()
        try:
            total_recs = df_recs.count()
            print(f">>> [READY] Đã sẵn sàng lưu {total_recs} users vào HBase.")
            
            if total_recs > 0:
                print(f">>> [SAVING] Đang ghi xuống HBase (Table: {config.HBASE_TABLE_RECS})...")
                df_recs.foreachPartition(lambda iter: worker_save_recs(iter, config.HBASE_TABLE_RECS))
                print(f">>> [DONE] Hoàn tất {model_type.upper()}.")
            else:
                print(">>> [WARN] Model chạy xong nhưng không tìm thấy gợi ý nào.")
        except Exception as e:
            print(f"❌ [CRITICAL ERROR] Lỗi trong quá trình tính toán/lưu trữ: {e}")
        finally:
            df_recs.unpersist()
    else:
        print(f">>> [SKIP] Model {model_type} không trả về kết quả.")

def run_training_and_evaluate(spark, model_type, df_ratings, df_movies):
    """
    Chạy model và lưu metrics vào HBase.
    """
    print(f"\n>>> [TRAINING] Đang chạy Model: {model_type.upper()}...")
    df_ratings.createOrReplaceTempView("ratings")
    
    metrics = {}
    recommender = None
    
    if model_type == "als":
        recommender = ALSRecommender(spark)
        metrics = recommender.train(df_ratings)
    elif model_type == "cbf":
        recommender = ContentBasedRecommender(spark)
        metrics = recommender.train(df_ratings, df_movies)
    elif model_type == "hybrid":
        recommender = HybridRecommender(spark)
        metrics = recommender.train(df_ratings, df_movies)
        
    # Lưu metrics vào HBase
    from src.utils.hbase_utils import HBaseProvider
    provider = HBaseProvider()
    provider.save_model_metrics(model_type, metrics)
    
    return recommender, metrics

# ==============================================================================
# 3. MAIN
# ==============================================================================

def main(args_model):
    spark = SparkSession.builder \
        .appName("MovieLens_10M_Pipeline") \
        .master("local[*]") \
        .config("spark.driver.memory", "6g") \
        .config("spark.executor.memory", "6g") \
        .config("spark.driver.maxResultSize", "2g") \
        .config("spark.sql.shuffle.partitions", "500") \
        .config("spark.default.parallelism", "500") \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", "2g") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Load Data
    data_dir = os.path.join(project_root, 'data')
    ratings_path = f"file://{os.path.join(data_dir, config.RATINGS_FILE)}"
    movies_path = f"file://{os.path.join(data_dir, config.MOVIES_FILE)}"
    

    print(f"📂 Ratings: {ratings_path}")
    print(f"📂 Movies:  {movies_path}")
    
    if not os.path.exists(os.path.join(data_dir, config.RATINGS_FILE)):
        print(f"❌ ERROR: Không tìm thấy file data")
        return

    schema_ratings = StructType([
        StructField("userId", IntegerType()), 
        StructField("movieId", IntegerType()), 
        StructField("rating", FloatType()), 
        StructField("timestamp", LongType())
    ])
    df_ratings = spark.read.csv(ratings_path, header=True, schema=schema_ratings).cache()

    schema_movies = StructType([
        StructField("movieId", IntegerType()),
        StructField("title", StringType()),
        StructField("genres", StringType())
    ])
    df_movies = spark.read.csv(movies_path, header=True, schema=schema_movies).cache()
    
    print(f">>> Data Loaded. Ratings: {df_ratings.count()}, Movies: {df_movies.count()}")

    # --- LOGIC CHẠY TỐI ƯU ---

    if args_model == "all":
        print(">>> Mode 'ALL' detected: Chạy tất cả các Model để tìm cái tốt nhất...")
        
        results = {}
        # 1. Chạy ALS
        als_model, als_metrics = run_training_and_evaluate(spark, "als", df_ratings, df_movies)
        results["als"] = (als_model, als_metrics)
        
        # 2. Chạy CBF
        cbf_model, cbf_metrics = run_training_and_evaluate(spark, "cbf", df_ratings, df_movies)
        results["cbf"] = (cbf_model, cbf_metrics)
        
        # 3. Chạy Hybrid
        hybrid_model, hybrid_metrics = run_training_and_evaluate(spark, "hybrid", df_ratings, df_movies)
        results["hybrid"] = (hybrid_model, hybrid_metrics)
        
        # Tìm Model tốt nhất dựa trên RMSE
        best_model_name = min(results, key=lambda k: results[k][1]['rmse'])
        print(f"\n🏆 [WINNER] Model tốt nhất là: {best_model_name.upper()} (RMSE: {results[best_model_name][1]['rmse']:.4f})")
        
        # Chỉ lưu recommendations của model tốt nhất
        print(f">>> [SAVING] Đang lưu kết quả của model tốt nhất ({best_model_name.upper()}) vào HBase...")
        best_recommender = results[best_model_name][0]
        df_recs = best_recommender.get_recommendations(k=10)
        
        if df_recs:
            df_recs.cache()
            df_recs.foreachPartition(lambda iter: worker_save_recs(iter, config.HBASE_TABLE_RECS))
            df_recs.unpersist()
            
    elif args_model in ["als", "cbf", "hybrid"]:
        # Chạy model đơn lẻ
        model, metrics = run_training_and_evaluate(spark, args_model, df_ratings, df_movies)
        df_recs = model.get_recommendations(k=10)
        if df_recs:
            df_recs.cache()
            df_recs.foreachPartition(lambda iter: worker_save_recs(iter, config.HBASE_TABLE_RECS))
            df_recs.unpersist()

    print("\n>>> ALL TASKS FINISHED SUCCESSFULLY!")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", type=str, default="all", choices=["als", "cbf", "hybrid", "all"])
    args = parser.parse_args()
    main(args.model)