# FILE: src/models/hybrid_recommender.py
from src.models.als_recommender import ALSRecommender
from src.models.content_based_recommender import ContentBasedRecommender


class HybridRecommender:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.als = ALSRecommender(spark_session)
        self.cbf = ContentBasedRecommender(spark_session)
        self.hybrid_recs = None

    def train(self, ratings_df, movies_df):
        print("   -> Training Hybrid Model...")
        
        # 1. Chạy ALS
        self.als.train(ratings_df)
        df_als = self.als.get_recommendations(k=10)
        
        # 2. Chạy CBF
        self.cbf.train(ratings_df, movies_df)
        df_cbf = self.cbf.get_recommendations(k=10)

        # 3. Kết hợp (Đơn giản là lấy kết quả ALS, nếu user thiếu thì bù bằng CBF)
        # Trong thực tế sẽ phức tạp hơn (Weighted Average), nhưng để chạy được ta dùng COALESCE
        # Ưu tiên ALS, nếu null thì lấy CBF
        
        # Full Outer Join
        df_joined = df_als.alias("a").join(df_cbf.alias("c"), "userId", "outer")
        
        from pyspark.sql.functions import coalesce, col
        
        self.hybrid_recs = df_joined.select(
            coalesce(col("a.userId"), col("c.userId")).alias("userId"),
            coalesce(col("a.recommendations"), col("c.recommendations")).alias("recommendations")
        )
        
        print("   -> Hybrid Training Done.")
        return self

    def get_recommendations(self, k=10):
        return self.hybrid_recs