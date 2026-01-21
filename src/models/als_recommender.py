# FILE: src/models/als_recommender.py
from pyspark.ml.recommendation import ALS


class ALSRecommender:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.model = None
        
    def train(self, ratings_df):
        print("   -> Training ALS Model...")
        # Sử dụng tham số cơ bản, bạn có thể tune lại nếu muốn
        als = ALS(
            maxIter=5, 
            regParam=0.01, 
            userCol="userId", 
            itemCol="movieId", 
            ratingCol="rating",
            coldStartStrategy="drop" # Bỏ qua user/item mới chưa có data
        )
        self.model = als.fit(ratings_df)
        print("   -> ALS Training Done.")
        return self

    def get_recommendations(self, k=10):
        if not self.model:
            raise Exception("Model chưa được train!")
        
        # Trả về DataFrame: [userId, recommendations: array<struct<movieId, rating>>]
        return self.model.recommendForAllUsers(k)