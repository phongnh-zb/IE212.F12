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
        self.name = "Content-Based"

    def train(self, movies_df, tag_df=None):
        self.movie_df = movies_df

        movies_processed = movies_df.withColumn(
            "genres_text",
            concat_ws(" ", col("genres"))
        ).withColumn(
            "genres_clean",
            udf(lambda x: x.replace("|", " ") if x else "")(col("genres")),
        )

        if tag_df is not None:
            tags_agg = tag_df.groupBy("movieId").agg(
                concat_ws(" ", collect_list("tags")).alias("tags_text")
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

        target_vec = target["features"].toarray()

        def cosine_sim(features):
            if features is None:
                return 0.0
            vec = features.toarray()
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

    def recommend_for_user(self, user_id, rating_df, n=10):
        user_ratings = rating_df.filter(col("userId") == user_id)
        top_rated = user_ratings.filter(col("rating") > 4.0) \
            .orderBy(col("rating").desc()) \
            .limit(5).select("movieId").collect()

        if not top_rated:
            top_rated = user_ratings.orderBy(col("rating").desc()).limit(3).select("movieId").collect()

        liked_movie_ids = [row.movieId for row in top_rated]
        watched_ids = [row.movieId for row in user_ratings.select("movieId").distinct().collect()]

        liked_features = self.movie_features.filter(col("movieId").isin(liked_movie_ids)
                                                    ).select("features").collect()

        if not liked_features:
            return self.spark.createDataFrame([], "userId INT, movieId INT, score DOUBLE")

        vectors = [row.features.toArray() for row in liked_features]
        user_profile = np.mean(vectors, axis=0)

        def profile_similarity(features):
            if features is None:
                return 0.0
            vec = features.toarray()
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

    def predict(self, test_data, ratings_df):
        user_profiles = {}
        users = [row.userId for row in test_data.select("userId").distinct().collect()]
        for user_id in users:
            user_ratings = ratings_df.filter(col("userId") == user_id)
            top_rated = user_ratings.filter(col("rating") >= 3.5) \
                .select("movieId").collect()

            if top_rated:
                liked_ids = [row.movieId for row in top_rated]
                liked_features = self.movie_features.filter(col("movieId").isin(liked_ids)).select("features").collect()

                if liked_features:
                    vectors = [row.features.toArray() for row in liked_features]
                    user_profiles[user_id] = np.mean(vectors, axis=0)

        def predict_rating(user_id, movie_id):
            if user_id not in user_profiles:
                return 3.0

            movie_row = self.movie_features.filter(col("movieId") == movie_id).first()
            if not movie_row:
                return 3.0
            user_vec = user_profiles[user_id]
            movie_vec = movie_row.features.toArray()

            dot = float(np.dot(user_vec, movie_vec))
            norm = float(np.linalg.norm(user_vec) * np.linalg.norm(movie_vec))
            similarity = dot / norm if norm > 0 else 0.0
            return 0.5 + similarity * 4.5

        predict_udf = udf(predict_rating, DoubleType())
        predictions = test_data.withColumn(
            "prediction",
            predict_udf(col("userId"), col("movieId"))
        )
        return predictions
