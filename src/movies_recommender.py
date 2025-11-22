from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.sql.functions import col, explode
from pyspark.ml.linalg import Vectors
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
import numpy as np

class MovieRecommender:
    def __init__(self):
        self.spark = SparkSession.builder.appName("MovieRecommender") \
            .master('local[*]') \
            .config('spark.driver.memory', '8g') \
            .config('spark.sql.shuffle.partitions', '10') \
            .config('spark.driver.host', 'localhost') \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel('ERROR')
        self.model = None
        self.rating_df = None
        self.movies_df = None
    
    def load_data(self):
        """loading rating and movie data"""
        self.rating_df = self.spark.read.csv(
            './data/ratings.csv',
            header=True,
            inferSchema=True
        )
        self.movies_df = self.spark.read.csv(
            './data/movies.csv',
            header=True,
            inferSchema=True
        )
        print(f'Loaded {self.rating_df.count()} ratings and {self.movies_df.count()} movies')
    
    def train_model(self, rank=10, maxIter=10, regParam=0.1):
        """Train ALS model"""
        train, test = self.rating_df.randomSplit([0.8, 0.2], seed=42)
        als = ALS(
            rank=rank,
            maxIter=maxIter,
            regParam=regParam,
            userCol="userId",
            itemCol="movieId",
            ratingCol="rating",
            coldStartStrategy="drop"
        )

        self.model = als.fit(train)
        predictions = self.model.transform(test)
        evaluator = RegressionEvaluator(
            metricName="rmse",
            labelCol="rating",
            predictionCol="prediction"
        )

        rmse = evaluator.evaluate(predictions)
        print(f"Model trained! RMSE: {rmse:.3f}")
        return rmse

    def train_model_bigger_data(self):
        """Train ALS model with automatic hyperparameter tuning"""
        train, test = self.rating_df.randomSplit([0.8, 0.2], seed=42)
        als = ALS(
            userCol="userId",
            itemCol="movieId",
            ratingCol="rating",
            coldStartStrategy="drop"
        )

        paramGrid = ParamGridBuilder() \
            .addGrid(als.rank, [10, 20, 30]) \
            .addGrid(als.regParam, [0.01, 0.1]) \
            .addGrid(als.maxIter, [10, 15]) \
            .build()

       
        evaluator = RegressionEvaluator(
            metricName="rmse",
            labelCol="rating",
            predictionCol="prediction"
        )

        tvs = TrainValidationSplit(
            estimator=als,
            estimatorParamMaps=paramGrid,
            evaluator=evaluator,
            trainRatio=0.8,
            seed=42
        )
        print("training with hyperParameter tuning (testing 12 combinations)....")

        model = tvs.fit(train)
        self.model = model.bestModel
        print(f"Best parameter found:")
        print(f"   Rank: {self.model.rank}")
        print(f"   regParam: {self.model._java_obj.parent().getRegParam()}")
        print(f"   MaxIter: {self.model._java_obj.parent().getMaxIter()}")

        predictons = self.model.transform(test)
        rmse = evaluator.evaluate(predictons)

        print(f"Model trained! RMSE: {rmse:.3f}")
        return rmse

    def recommend_movies_for_user(self, user_id=1, n=10):
        """recommend top N movies to a user"""
        user_df = self.spark.createDataFrame([(user_id,)], ["userId"])

        recommendations = self.model.recommendForUserSubset(user_df, n)

        result = recommendations.select(
            "userId",
            explode("recommendations").alias("rec")
        ).select(
            "userId",
            col("rec.movieId").alias("movieId"),
            col("rec.rating").alias("predicted_rating")
        ).join(self.movies_df, "movieId")
        return result.select("movieId", "title", "genres", "predicted_rating")

    def find_similar_movies(self, movie_id, n=10):
        """Finding N similar movies to a given movie"""
        movie_factor = self.model.itemFactors
        target_movie = movie_factor.filter(col("id") == movie_id).first()

        if not target_movie:
            return None
        
        target_vector = target_movie["features"]

        def cosine_similarity(features):
            dot = np.dot(target_vector, features)
            norm = np.linalg.norm(target_vector) * np.linalg.norm(features)
            return float(dot/norm) if norm != 0 else 0.0
        
        similarity_udf = udf(cosine_similarity, DoubleType())
        
        similar = movie_factor.filter(col("id") != movie_id) \
                    .withColumn("similarity", similarity_udf(col("features"))) \
                    .orderBy(col("similarity").desc()) \
                    .limit(n) \
                    .join(self.movies_df, col("id") == col("movieId")) \
                    .select("movieId", "title", "genres", "similarity")
        return similar
    
    def predict_rating(self, user_id, movie_id):
        """predict rating  user would give to a movie"""

        user_movie_df = self.spark.createDataFrame(
            [(user_id, movie_id)],
            ["userId", "movieId"]
        )
        prediction = self.model.transform(user_movie_df)

        result = prediction.join(self.movies_df, "movieId") \
            .select("userId", "movieId", "title", "prediction")
        
        return result
    
    def get_user_history(self, user_id, n=10):
        """get user rating history"""
        history = self.rating_df.filter(col("userId") == user_id) \
            .join(self.movies_df, "movieId") \
            .select("movieId", "title", "genres", "rating") \
            .orderBy(col("rating").desc())\
            .limit(n)
        
        return history
    
    def stop(self):
        self.spark.stop()

if __name__ == "__main__":
    recommender = MovieRecommender()
    recommender.load_data()

    # recommender.train_model(rank=10, maxIter=10, regParam=10)
    recommender.train_model_bigger_data()


    print("\n=== top 10 recommendations for user 1 ===")
    recommendation = recommender.recommend_movies_for_user(user_id=1, n=10)
    recommendation.show(truncate=False)

    print("\n=== Movies similar to Movie ID 1 ===")
    similar = recommender.find_similar_movies(movie_id=1, n=10)
    if similar:
        similar.show(truncate=False)


    print("\n=== predict rating for user 1 , movie 100 ===")
    prediction = recommender.predict_rating(movie_id=100, user_id=1)
    prediction.show(truncate=False)


    print("\n=== user 1 top rated movies ===")
    history = recommender.get_user_history(user_id=1, n=10)
    history.show(truncate=False)

    recommender.stop()
