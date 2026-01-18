from pyspark.sql import SparkSession
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, Normalizer
from pyspark.sql.functions import col, concat_ws, collect_list, udf, lit, coalesce
from pyspark.sql.types import DoubleType, StringType
import numpy as np


class ContentBasedRecommender:
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.model = None
        self.movie_features = None
        self.movie_df = None
        self.user_profiles = {}
        self.name = "Content-Based"

    def train(self, movies_df, tag_df=None):
        self.movie_df = movies_df
        clean_genres_udf = udf(lambda x: x.replace("|", " ") if x else "", StringType())

        movies_processed = movies_df.withColumn(
            "genres_text",
            concat_ws(" ", col("genres"))
        ).withColumn(
            "genres_clean",
            clean_genres_udf(col("genres")),
        )

        if tag_df is not None:
            tags_agg = tag_df.groupBy("movieId").agg(
                concat_ws(" ", collect_list("tag")).alias("tags_text")
            )
            movies_processed = movies_processed.join(tags_agg, "movieId", "left")
            movies_processed = movies_processed.withColumn(
                "tags_text",
                coalesce(col("tags_text"), lit(""))
            )
            movies_processed = movies_processed.withColumn(
                "content",
                concat_ws(" ", col("genres_clean"), col("tags_text"))
            )
        else:
            movies_processed = movies_processed.withColumn(
                "content",
                col("genres_clean")
            )
        tokenizer = Tokenizer(inputCol="content", outputCol="tokens")
        tokenized = tokenizer.transform(movies_processed)

        hashingTF = HashingTF(inputCol="tokens", outputCol="raw_features", numFeatures=100)
        featurized = hashingTF.transform(tokenized)

        idf = IDF(inputCol="raw_features", outputCol="tfidf_features")
        idf_model = idf.fit(featurized)
        tfidf_data = idf_model.transform(featurized)

        normalizer = Normalizer(inputCol="tfidf_features", outputCol="features", p=2.0)
        self.movie_features = normalizer.transform(tfidf_data)
        self.movie_features.cache()
        self.model = {
            "tokenizer": tokenizer,
            "hashingTF": hashingTF,
            "idf": idf_model,
            "normalizer": normalizer
        }
        return self

    def find_similar_movies(self, movie_id, n=10):
        target = self.movie_features.filter(col("movieId") == movie_id).first()
        if not target:
            return None

        target_vec = target["features"].toArray()

        def cosine_sim(features):
            if features is None:
                return 0.0
            vec = features.toArray()
            dot = float(np.dot(target_vec, vec))
            norm = float(np.linalg.norm(target_vec) * np.linalg.norm(vec))
            return dot / norm if norm > 0 else 0.0

        cosine_udf = udf(cosine_sim, DoubleType())
        similar = self.movie_features.filter(col("movieId") != movie_id) \
            .withColumn("score", cosine_udf(col("features"))) \
            .orderBy(col("score").desc()) \
            .limit(n) \
            .select("movieId", "title", "genres", "score")
        return similar

    def build_user_profiles(self, rating_df):
        """Pre-calculate user profiles based on their highly rated movies"""
        # Get users who have rated movies > 3.5
        highly_rated = rating_df.filter(col("rating") >= 3.5)
        
        # Join with movie features
        user_movie_features = highly_rated.join(
            self.movie_features.select("movieId", "features"),
            "movieId"
        )
        
        # Group by userId and collect feature vectors
        user_data = user_movie_features.groupBy("userId").agg(
            collect_list("features").alias("all_features")
        ).collect()
        
        self.user_profiles = {}
        for row in user_data:
            vectors = [vec.toArray() for vec in row.all_features]
            if vectors:
                self.user_profiles[row.userId] = np.mean(vectors, axis=0)
        
        return self

    def recommend_for_user(self, user_id, rating_df=None, n=10):
        # If user profile is not pre-calculated, try to build it on the fly if rating_df is provided
        if user_id not in self.user_profiles and rating_df is not None:
            user_ratings = rating_df.filter(col("userId") == user_id)
            top_rated = user_ratings.filter(col("rating") >= 3.5).select("movieId")
            liked_features = self.movie_features.join(top_rated, "movieId").select("features").collect()
            if liked_features:
                vectors = [row.features.toArray() for row in liked_features]
                self.user_profiles[user_id] = np.mean(vectors, axis=0)

        if user_id not in self.user_profiles:
            # Return empty if no profile can be built
            from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType as DT
            schema = StructType([
                StructField("userId", IntegerType(), True),
                StructField("movieId", IntegerType(), True),
                StructField("score", DT(), True)
            ])
            return self.spark.createDataFrame([], schema)

        user_profile = self.user_profiles[user_id]
        watched_ids = []
        if rating_df is not None:
            watched_ids = [row.movieId for row in rating_df.filter(col("userId") == user_id).select("movieId").distinct().collect()]

        def profile_similarity(features):
            if features is None:
                return 0.0
            vec = features.toArray()
            dot = float(np.dot(user_profile, vec))
            norm = float(np.linalg.norm(user_profile) * np.linalg.norm(vec))
            return dot / norm if norm > 0 else 0.0

        sim_udf = udf(profile_similarity, DoubleType())
        recommendations = self.movie_features \
            .filter(~col("movieId").isin(watched_ids)) \
            .withColumn("score", sim_udf(col("features"))) \
            .orderBy(col("score").desc()) \
            .limit(n) \
            .withColumn("userId", lit(user_id)) \
            .select("userId", "movieId", "score")
        return recommendations

    def predict(self, test_data, ratings_df=None):
        # Ensure profiles exist for users in test_data
        if ratings_df is not None:
            # Only build for users we don't have yet if we want to be efficient
            test_users = [row.userId for row in test_data.select("userId").distinct().collect()]
            missing_users = [u for u in test_users if u not in self.user_profiles]
            if missing_users:
                self.build_user_profiles(ratings_df.filter(col("userId").isin(missing_users)))

        # Broadcast user profiles for efficiency and to avoid serialization issues
        broadcast_profiles = self.spark.sparkContext.broadcast(self.user_profiles)

        def predict_rating(user_id, movie_features_vec):
            profiles = broadcast_profiles.value
            if user_id not in profiles or movie_features_vec is None:
                return 3.0

            user_vec = profiles[user_id]
            movie_vec = movie_features_vec.toArray()

            dot = float(np.dot(user_vec, movie_vec))
            norm = float(np.linalg.norm(user_vec) * np.linalg.norm(movie_vec))
            similarity = dot / norm if norm > 0 else 0.0
            return 0.5 + similarity * 4.5

        predict_udf = udf(predict_rating, DoubleType())

        # Join test_data with movie_features to get relevant features for UDF
        predictions = test_data.join(
            self.movie_features.select("movieId", col("features").alias("movie_features_vec")),
            "movieId",
            "left"
        ).withColumn(
            "prediction",
            predict_udf(col("userId"), col("movie_features_vec"))
        )
        # Defensive column selection
        available_cols = predictions.columns
        target_cols = ["userId", "movieId", "rating", "prediction"]
        select_cols = [c for c in target_cols if c in available_cols]
            
        return predictions.select(*select_cols)
