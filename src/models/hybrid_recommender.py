from pyspark.sql.functions import (col, collect_list, count, desc, explode,
                                   row_number, struct, when)
from pyspark.sql.window import Window

from src.models.als_recommender import ALSRecommender
from src.models.content_based_recommender import ContentBasedRecommender


class HybridRecommender:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.als = ALSRecommender(spark_session)
        self.cbf = ContentBasedRecommender(spark_session)
        
        # Tỷ trọng (Weights) - Tổng nên là 1.0
        self.WEIGHT_ALS = 0.7
        self.WEIGHT_CBF = 0.3
        
        # Biến lưu danh sách phim "hợp lệ" (Quality Filter)
        self.valid_movies_df = None

    def train(self, df_ratings, df_movies):
        print("   -> [Hybrid] Training Sub-models...")
        
        # 1. Train ALS
        self.als.train(df_ratings)
        
        # 2. Train CBF
        self.cbf.train(df_ratings, df_movies)
        
        # 3. Tạo bộ lọc phim chất lượng (Quality Filter)
        # Chỉ những phim có ít nhất 10 lượt đánh giá mới được đưa vào danh sách Hybrid
        print("   -> [Hybrid] Building Quality Filter (Min Votes >= 10)...")
        self.valid_movies_df = df_ratings.groupBy("movieId") \
            .agg(count("rating").alias("vote_count")) \
            .filter("vote_count >= 10") \
            .select("movieId") 
        
        print("   -> [Hybrid] Training Done.")
        return self

    def get_recommendations(self, k=10):
        print(f"   -> [Hybrid] Generating Top-{k} Recommendations...")

        # 1. Lấy kết quả thô từ 2 model
        pool_k = k * 2 
        
        df_als_recs = self.als.get_recommendations(k=pool_k)
        df_cbf_recs = self.cbf.get_recommendations(k=pool_k)

        # Xử lý trường hợp thiếu model
        if not df_als_recs and not df_cbf_recs: return None
        if not df_als_recs: return df_cbf_recs
        if not df_cbf_recs: return df_als_recs

        # 2. "Bung" (Explode) nhưng KHÔNG nhân trọng số vội
        # Để giữ nguyên điểm gốc (Raw Score) cho việc tính toán trung bình sau này
        
        # Xử lý ALS
        flat_als = df_als_recs.select(
            col("userId"), 
            explode(col("recommendations")).alias("rec")
        ).select(
            col("userId"),
            col("rec.movieId").alias("movieId"),
            col("rec.rating").alias("rating_als") # Giữ điểm gốc
        )

        # Xử lý CBF
        flat_cbf = df_cbf_recs.select(
            col("userId"), 
            explode(col("recommendations")).alias("rec")
        ).select(
            col("userId"),
            col("rec.movieId").alias("movieId"),
            col("rec.rating").alias("rating_cbf") # Giữ điểm gốc
        )

        # 3. Full Outer Join
        df_joined = flat_als.join(flat_cbf, ["userId", "movieId"], "outer")
        
        # 4. Tính điểm Final (Smart Scoring)
        df_final_scores = df_joined.select(
            col("userId"),
            col("movieId"),
            when(
                col("rating_als").isNotNull() & col("rating_cbf").isNotNull(),
                (col("rating_als") * self.WEIGHT_ALS + col("rating_cbf") * self.WEIGHT_CBF)
            ).when(
                col("rating_als").isNotNull(),
                col("rating_als")
            ).otherwise(
                col("rating_cbf")
            ).alias("final_score")
        )

        # 5. LỌC BỎ LỊCH SỬ (History Filter)
        # Loại bỏ TẤT CẢ phim đã có trong bảng ratings của user đó
        # Bất kể rating là 1.0 hay 5.0, đã xem rồi thì không gợi ý nữa để nhường chỗ cho phim mới.
        df_history = self.als.spark.table("ratings").select("userId", "movieId")
        df_final_scores = df_final_scores.join(df_history, ["userId", "movieId"], "left_anti")

        # 6. Áp dụng bộ lọc chất lượng (Min Votes >= 10)
        if self.valid_movies_df:
            df_final_scores = df_final_scores.join(self.valid_movies_df, "movieId", "inner")

        # 7. Lấy Top K (Giữ nguyên)
        windowSpec = Window.partitionBy("userId").orderBy(desc("final_score"))
        
        final_recs = df_final_scores.withColumn("rank", row_number().over(windowSpec)) \
            .filter(f"rank <= {k}") \
            .groupBy("userId") \
            .agg(collect_list(struct(
                col("movieId"), 
                col("final_score").alias("rating")
            )).alias("recommendations"))

        return final_recs