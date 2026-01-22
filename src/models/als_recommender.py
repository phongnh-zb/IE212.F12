# FILE: src/models/als_recommender.py

from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import col  # [THÊM] import function cần thiết
from pyspark.sql.functions import desc, lit, when


class ALSRecommender:
    def __init__(self, spark):
        self.spark = spark
        self.model = None

    def train(self, ratings_df):
        als = ALS(
            maxIter=10, 
            regParam=0.1, 
            userCol="userId", 
            itemCol="movieId", 
            ratingCol="rating",
            coldStartStrategy="drop"
        )
        self.model = als.fit(ratings_df)

    def get_recommendations(self, k=10):
        user_recs = self.model.recommendForAllUsers(k)
        return user_recs