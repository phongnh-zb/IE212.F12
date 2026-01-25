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

def worker_save_recs(iterator, table_name, col_name_str='info:movieIds'):
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
        col_name = col_name_str.encode()

        count = 0
        for row in iterator:
            if not hasattr(row, 'recommendations'): continue
            
            clean_recs = []
            for r in row.recommendations:
                val = max(0.0, min(5.0, float(r.rating)))
                # Đảm bảo format movieId:rating
                clean_recs.append(f"{getattr(r, 'movieId')}:{val:.2f}")

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
    
    # 0. Split Train/Test
    # Chia 80/20 dùng chung cho tất cả các model để so sánh công bằng
    print("\n>>> [DATA] Splitting Data 80/20 for Evaluation...")
    train_df, test_df = df_ratings.randomSplit([0.8, 0.2], seed=42)
    train_df = train_df.cache()
    test_df = test_df.cache()
    print(f">>> Train Size: {train_df.count()}, Test Size: {test_df.count()}")

    models_to_run = ["als", "cbf", "hybrid"] if args_model == "all" else [args_model]
    metrics_log = {}

    best_model_name = None
    best_rmse = float('inf')
    best_recommender = None

    # Khởi tạo objects và train/eval
    trained_models = {}

    for m_name in models_to_run:
        print(f"\n==========================================")
        print(f">>> [TRAINING] Running Model: {m_name.upper()}")
        print(f"==========================================")
        
        recommender = None
        metrics = {}
        
        if m_name == "als":
            recommender = ALSRecommender(spark)
            recommender.train(train_df)
            metrics = recommender.evaluate(test_df)
            trained_models["als"] = recommender
            
        elif m_name == "cbf":
            recommender = ContentBasedRecommender(spark)
            recommender.train(train_df, df_movies)
            metrics = recommender.evaluate(test_df)
            trained_models["cbf"] = recommender
            
        elif m_name == "hybrid":
            recommender = HybridRecommender(spark)
            # Inject pre-trained sub-models if available
            if "als" in trained_models: 
                recommender.als = trained_models["als"]
            if "cbf" in trained_models:
                recommender.cbf = trained_models["cbf"]
                
            # Train (skip sub-models if injected)
            train_sub = "als" not in trained_models or "cbf" not in trained_models
            recommender.train(train_df, df_movies, train_submodels=train_sub)
            
            metrics = recommender.evaluate(test_df)
            trained_models["hybrid"] = recommender
        
        # Log metrics
        metrics_log[m_name] = metrics
        
        # Check Best
        if metrics["rmse"] < best_rmse:
            best_rmse = metrics["rmse"]
            best_model_name = m_name
            best_recommender = recommender

    # --- REPORT & SELECTION ---
    print("\n\n")
    print("╔════════════════════════════════════════╗")
    print("║        MODEL COMPARISON RESULTS        ║")
    print("╠══════════╦══════════════╦══════════════╣")
    print("║  Model   ║     RMSE     ║     MAE      ║")
    print("╠══════════╬══════════════╬══════════════╣")
    for m, vals in metrics_log.items():
        print(f"║  {m.upper():<6}  ║    {vals['rmse']:.4f}    ║    {vals['mae']:.4f}    ║")
    print("╚══════════╩══════════════╩══════════════╝")
    
    print(f"\n🏆 WINNER: {best_model_name.upper()} (RMSE: {best_rmse:.4f})")
    # --- SAVE METRICS TO HBASE ---
    from src.utils.hbase_utils import HBaseProvider
    provider = HBaseProvider()
    for m_name, vals in metrics_log.items():
        provider.save_model_metrics(m_name, vals)
    
    # --- GENERATE & SAVE TO HBASE FOR ALL MODELS ---
    print("\n>>> [FINALIZING] Saving recommendations for all models to HBase...")
    for m_name, recommender in trained_models.items():
        if not recommender: continue
        
        print(f"\n>>> Processing recommendations for {m_name.upper()}...")
        # Đảm bảo temp view luôn đúng cho Hybrid và các model khác
        train_df.createOrReplaceTempView("ratings")
        
        df_recs = recommender.get_recommendations(k=10)
        
        if df_recs:
            df_recs.cache()
            try:
                total_recs = df_recs.count()
                print(f"    -> Ready to save {total_recs} users for {m_name.upper()}.")
                
                # 1. Save to model-specific column (info:als, info:cbf, info:hybrid)
                col_name = f"info:{m_name}"
                df_recs.foreachPartition(lambda iter: worker_save_recs(iter, config.HBASE_TABLE_RECS, col_name))
                
                # 2. If this is the winner, also save to info:movieIds (Backward compatibility)
                if m_name == best_model_name:
                    print(f"    -> Saving {m_name.upper()} as the WINNER to 'info:movieIds'...")
                    df_recs.foreachPartition(lambda iter: worker_save_recs(iter, config.HBASE_TABLE_RECS, 'info:movieIds'))
                
                print(f"    -> [DONE] Saved {m_name.upper()}.")
            except Exception as e:
                print(f"❌ [ERROR] Lỗi khi lưu {m_name}: {e}")
            finally:
                df_recs.unpersist()
        else:
            print(f"    -> [SKIP] No recommendations for {m_name}.")

    print("\n>>> ALL TASKS FINISHED SUCCESSFULLY!")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", type=str, default="all", choices=["als", "cbf", "hybrid", "all"])
    args = parser.parse_args()
    main(args.model)