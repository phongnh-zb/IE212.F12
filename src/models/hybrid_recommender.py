from pyspark.sql.functions import (coalesce, col, collect_list, desc, explode,
                                   lit, row_number, struct)
from pyspark.sql.window import Window

from src.models.als_recommender import ALSRecommender
from src.models.content_based_recommender import ContentBasedRecommender


class HybridRecommender:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.als = ALSRecommender(spark_session)
        self.cbf = ContentBasedRecommender(spark_session)
        # Tỷ trọng (Weights): ALS thường chính xác hơn nên cho điểm cao hơn
        self.WEIGHT_ALS = 0.7
        self.WEIGHT_CBF = 0.3

    def train(self, df_ratings, df_movies):
        print("   -> [Hybrid] Training Sub-models...")
        
        # 1. Train ALS
        self.als.train(df_ratings)
        
        # 2. Train CBF
        self.cbf.train(df_ratings, df_movies)
        
        print("   -> [Hybrid] Training Done.")
        return self

    def get_recommendations(self, k=10):
        print(f"   -> [Hybrid] Generating Top-{k} Recommendations...")

        # 1. Lấy kết quả thô từ 2 model (Lấy nhiều hơn k một chút để trộn cho đều)
        # Ví dụ: Cần 10 thì lấy 20 từ mỗi model để lọc
        pool_k = k * 2 
        
        df_als_recs = self.als.get_recommendations(k=pool_k)
        df_cbf_recs = self.cbf.get_recommendations(k=pool_k)

        # Nếu một trong 2 model lỗi/trả về None
        if not df_als_recs and not df_cbf_recs: return None
        if not df_als_recs: return df_cbf_recs
        if not df_cbf_recs: return df_als_recs

        # 2. "Bung" (Explode) mảng gợi ý ra để tính toán
        # Format: userId, movieId, rating
        # Xử lý ALS
        flat_als = df_als_recs.select(
            col("userId"), 
            explode(col("recommendations")).alias("rec")
        ).select(
            col("userId"),
            col("rec.movieId").alias("movieId"),
            (col("rec.rating") * self.WEIGHT_ALS).alias("score_als") # Nhân trọng số
        )

        # Xử lý CBF
        flat_cbf = df_cbf_recs.select(
            col("userId"), 
            explode(col("recommendations")).alias("rec")
        ).select(
            col("userId"),
            col("rec.movieId").alias("movieId"),
            (col("rec.rating") * self.WEIGHT_CBF).alias("score_cbf") # Nhân trọng số
        )

        # 3. Full Outer Join để kết hợp điểm số
        # Nếu phim có ở cả 2 bên -> Cộng điểm
        # Nếu chỉ có ở 1 bên -> Lấy điểm bên đó
        df_joined = flat_als.join(flat_cbf, ["userId", "movieId"], "outer")
        
        # Tính điểm cuối cùng (Final Score)
        # Dùng coalesce để xử lý null (nếu chỉ xuất hiện ở 1 bảng)
        df_final_scores = df_joined.select(
            col("userId"),
            col("movieId"),
            (coalesce(col("score_als"), lit(0)) + coalesce(col("score_cbf"), lit(0))).alias("final_score")
        )

        # 4. Lấy Top K cho mỗi User dựa trên Final Score
        windowSpec = Window.partitionBy("userId").orderBy(desc("final_score"))
        
        final_recs = df_final_scores.withColumn("rank", row_number().over(windowSpec)) \
            .filter(f"rank <= {k}") \
            .groupBy("userId") \
            .agg(collect_list(struct(
                col("movieId"), 
                col("final_score").alias("rating") # Đổi tên thành rating để khớp format chung
            )).alias("recommendations"))

        return final_recs