from pyspark.sql.functions import (avg, col, collect_list, concat_ws, count, desc, lit, row_number,
                                   split, struct, udf, coalesce, size, slice)
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType, ArrayType, StringType, StructType, StructField
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover
import numpy as np


class ContentBasedRecommender:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.user_profiles = None
        self.movie_features = None
        self.idf_model = None
        self.final_recs = None

    def train(self, df_ratings, df_movies, df_tags=None):
        print(f"   -> [CBF] Training with TF-IDF vectors Start...")
        print("     -> buoc 1: tao Movie content tu genres + tags...")

        movies_content = df_movies.withColumn(
            "genres_text",
            concat_ws(" ", split(col("genres"), r"\|"))
        )

        if df_tags is not None:
            print(f"  -> dang xu ly tags...")
            tags_agg = df_tags.groupBy("movieId").agg(
                concat_ws(" ", slice(collect_list(col("tag")), 1, 100)).alias("tags_text")
            )
            movies_content = movies_content.join(tags_agg, 'movieId', 'left')
            movies_content = movies_content.withColumn(
                "tags_text",
                coalesce(col("tags_text"), lit(""))
            )
            movies_content = movies_content.withColumn(
                "content",
                concat_ws(" ", col("genres_text"), col("tags_text"))
            )
        else:
            movies_content = movies_content.withColumn(
                "content",
                col("genres_text")
            )

        print(f"    -> buoc 2: tinh TD-IDF vetors")

        tokenizer = Tokenizer(inputCol="content", outputCol="words")
        words_df = tokenizer.transform(movies_content)
        remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
        filtered_df = remover.transform(words_df)

        hashingTF = HashingTF(inputCol="filtered_words", outputCol="raw_features", numFeatures=512)

        tf_df = hashingTF.transform(filtered_df)

        idf = IDF(inputCol="raw_features", outputCol="tfidf_features")
        self.idf_model = idf.fit(tf_df)
        tfidf_df = self.idf_model.transform(tf_df)

        movie_stats = df_ratings.groupBy("movieId") .agg(
            avg("rating").alias("avg_rating"),
            count("rating").alias("vote_count")
        )

        self.movie_features = tfidf_df.join(movie_stats, "movieId", "left") \
            .select("movieId", 'title', 'genres', 'content', 'tfidf_features',
                    coalesce(col("avg_rating"), lit(3.0)).alias("avg_rating"),
                    coalesce(col("vote_count"), lit(0)).alias("vote_count"))

        self.movie_features = self.movie_features.cache()

        print(f"     Da ta TF-IDF vectors cho {self.movie_features.count()} phim")

        print(f"    -> buoc 3: tao user profile vector (max 30 movies/user)...")
        windowSpecProfile = Window.partitionBy("userId").orderBy(desc("rating"))
        high_rated = df_ratings.filter(col("rating") >= 4.0)\
            .withColumn("rank", row_number().over(windowSpecProfile))\
            .filter(col("rank") <= 30)\
            .select("userId", "movieId", "rating")

        user_movie_features = high_rated.join(
            self.movie_features.select("movieId", "tfidf_features"),
            "movieId"
        )

        @udf(ArrayType(FloatType()))
        def weighted_average_vectors(features_list, ratings_list):
            if not features_list or len(features_list) == 0:
                return None

            vectors = [f.toArray() for f in features_list]
            weights = [float(r) for r in ratings_list]

            total_weight = sum(weights)
            if total_weight == 0:
                return None

            avg_vec = np.zeros(len(vectors[0]))

            for vec, weight in zip(vectors, weights):
                avg_vec += np.array(vec) * weight
            avg_vec /= total_weight
            return avg_vec.tolist()

        user_vectors = user_movie_features.groupBy("userId").agg(
            collect_list("tfidf_features").alias("features_list"),
            collect_list("rating").alias("rating_list")
        )

        self.user_profiles = user_vectors.withColumn(
            "user_vector",
            weighted_average_vectors(col("features_list"), col("rating_list"))
        ).select("userId", "user_vector").filter(col("user_vector").isNotNull())

        self.user_profiles = self.user_profiles.cache()

        print(f"    Da tao profile vectors cho {self.user_profiles.count()} user")

        # Tối ưu: Sử dụng Broadcast thay vì crossJoin để tránh bùng nổ dữ liệu (OOM)
        # Thu thập danh sách phim chất lượng cao về Driver và broadcast
        print(f"    Buoc 4: tinh Cosine Similarity va tao recommendations...")
        
        quality_movies = self.movie_features.filter(
            (col("avg_rating") >= 3.8) & (col("vote_count") >= 100)
        ).limit(1000)
        
        print(f"    -> Broadcasting {quality_movies.count()} quality movies...")
        movie_list = quality_movies.select("movieId", "tfidf_features", "avg_rating").collect()
        bc_movies = self.spark.sparkContext.broadcast(movie_list)

        rec_schema = ArrayType(StructType([
            StructField("movieId", StringType(), False),
            StructField("rating", FloatType(), False)
        ]))

        @udf(rec_schema)
        def get_top_k_recommendations(user_vec):
            if user_vec is None:
                return []
            
            movies = bc_movies.value
            u_v = np.array(user_vec)
            u_norm = np.linalg.norm(u_v)
            
            if u_norm == 0:
                return []
                
            scores = []
            for m in movies:
                m_v = m['tfidf_features'].toArray()
                m_id = str(m['movieId'])
                m_avg = float(m['avg_rating'])
                
                # Cosine Similarity
                m_norm = np.linalg.norm(m_v)
                if m_norm == 0:
                    sim = 0.0
                else:
                    sim = float(np.dot(u_v, m_v) / (u_norm * m_norm))
                
                # Final Score: 60% similarity + 40% avg_rating
                final_score = sim * 0.6 + (m_avg / 5.0) * 0.4
                scores.append((m_id, final_score))
            
            # Sắp xếp và lấy top 10
            scores.sort(key=lambda x: x[1], reverse=True)
            top_k = scores[:10]
            
            return [{"movieId": s[0], "rating": float(s[1])} for s in top_k]

        # Chia mẻ người dùng (User Batching)
        USER_BATCH_SIZE = 5000
        user_ids_df = self.user_profiles.select("userId").distinct()
        user_id_list = [r['userId'] for r in user_ids_df.collect()]
        total_users = len(user_id_list)
        num_batches = (total_users + USER_BATCH_SIZE - 1) // USER_BATCH_SIZE
        
        print(f"    -> Bat dau xu ly {total_users} users theo {num_batches} me (Batch Size: {USER_BATCH_SIZE})...")

        batch_results = []
        for i in range(num_batches):
            start = i * USER_BATCH_SIZE
            end = min((i + 1) * USER_BATCH_SIZE, total_users)
            current_batch_ids = user_id_list[start:end]
            
            print(f"       -> Processing Batch {i+1}/{num_batches} (Users {start} to {end})...")
            
            df_batch = self.user_profiles.filter(col("userId").isin(current_batch_ids))
            
            res_batch = df_batch.withColumn(
                "recommendations_raw",
                get_top_k_recommendations(col("user_vector"))
            ).filter(col("recommendations_raw").isNotNull() & (size(col("recommendations_raw")) > 0)) \
            .select(
                "userId",
                col("recommendations_raw").alias("recommendations")
            )
            
            # Force computation for this batch to avoid plan explosion
            res_batch.cache()
            _ = res_batch.count() 
            batch_results.append(res_batch)

        # Union all batches
        print(f"    -> Dang hop nhat {len(batch_results)} me ket qua...")
        from functools import reduce
        from pyspark.sql import DataFrame
        self.final_recs = reduce(DataFrame.union, batch_results)
        
        self.final_recs = self.final_recs.cache()
        print(f"    Training Done")

        return self
    def evaluate(self, test_data):

        from pyspark.ml.evaluation import RegressionEvaluator
        print("     Dang danh gia tren tap test...")

        if self.user_profiles is None or self.movie_features is None:
            print("Model chua duoc train")
            return {"rmse": float('inf'), "mae": float('inf')}

        @udf(FloatType())
        def cosine_similarity(vec1, vec2):
            if vec1 is None or vec2 is None:
                return 3.0
            v1 = np.array(vec1) if isinstance(vec1, list) else vec1.toArray()
            v2 = np.array(vec2) if isinstance(vec2, list) else vec2.toArray()
            dot = float(np.dot(v1, v2))
            norm1 = float(np.linalg.norm(v1))
            norm2 = float(np.linalg.norm(v2))
            if norm1 == 0 or norm2 == 0:
                return 3.0
            sim = dot / (norm1 * norm2)
            return 1.0 + sim * 4.0

        # Tối ưu: Sample 10% test data để đánh giá nhanh và đỡ tốn bộ nhớ
        test_sampled = test_data.sample(False, 0.1, seed=42)

        test_with_user = test_sampled.join(
            self.user_profiles, "userId", "left"
        )

        from pyspark.sql.functions import broadcast
        test_with_all = test_with_user.join(
            broadcast(self.movie_features.select("movieId", "tfidf_features")),
            "movieId", "left"
        )

        predictions = test_with_all.withColumn(
            "prediction",
            cosine_similarity(col("user_vector"), col("tfidf_features"))
            ).select("userId", "movieId", col("rating").alias("actual"), "prediction")

        predictions = predictions.na.fill(3.0, subset=["prediction"])

        evaluator_rmse = RegressionEvaluator(
            metricName="rmse", labelCol="actual", predictionCol="prediction"
        )
        evaluator_mae = RegressionEvaluator(
            metricName="mae", labelCol="actual", predictionCol="prediction"
        )

        rmse = evaluator_rmse.evaluate(predictions)
        mae = evaluator_mae.evaluate(predictions)

        print(f"     Ket qua: RMSE: {rmse}, MAE: {mae}")

        return {"rmse": rmse, "mae": mae}
    def get_recommendations(self, k=10):

        return self.final_recs