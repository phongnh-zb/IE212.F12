from pyspark.sql.functions import (avg, broadcast, col, collect_list, count,
                                   desc, explode, row_number, split, struct)
from pyspark.sql.window import Window


class ContentBasedRecommender:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.user_profiles = None
        self.movie_profiles = None
        self.final_recs = None

    def train(self, df_ratings, df_movies):
        print("   -> [CBF] Training Optimized for 10M Dataset...")
        
        # 1. TẠO USER PROFILE (SỞ THÍCH)
        # Chỉ lấy Top 2 thể loại user thích nhất dựa trên phim họ chấm >= 4.0
        user_movies = df_ratings.filter("rating >= 4.0") \
            .join(df_movies, "movieId") \
            .select("userId", "genres")

        # Đếm số lần user xem từng thể loại
        user_genre_counts = user_movies.withColumn("genre", explode(split(col("genres"), r"\|"))) \
            .groupBy("userId", "genre").count()

        # Lấy Top 2 thể loại
        windowUser = Window.partitionBy("userId").orderBy(desc("count"))
        # Lưu User Profile (UserId -> Top Genre)
        self.user_profiles = user_genre_counts.withColumn("rank", row_number().over(windowUser)) \
            .filter("rank <= 2") \
            .select("userId", col("genre").alias("top_genre"))
            
        # Optimize: Broadcast User Profile nếu cần, nhưng ở đây ta lưu DataFrame để dùng sau
        self.user_profiles = self.user_profiles.cache()
        
        # 2. TẠO MOVIE PROFILE (CANDIDATE SELECTION)
        
        # B2.1: Tính điểm TB và số lượt vote
        movie_stats = df_ratings.groupBy("movieId") \
            .agg(
                avg("rating").alias("avg_rating"),
                count("rating").alias("vote_count")
            ) \
            .filter("avg_rating >= 3.5") \
            .filter("vote_count >= 50")  # Chỉ lấy phim có ít nhất 50 lượt vote (Tránh nhiễu)

        # B2.2: Gán thể loại cho phim và lọc Top 50 per Genre
        movies_exploded = df_movies.join(movie_stats, "movieId") \
            .withColumn("genre", explode(split(col("genres"), r"\|")))
        
        windowGenre = Window.partitionBy("genre").orderBy(desc("avg_rating"), desc("vote_count"))
        
        # Lưu Movie Profile (Genre -> List Top Movies)
        self.movie_profiles = movies_exploded \
            .withColumn("rank_genre", row_number().over(windowGenre)) \
            .filter("rank_genre <= 50") \
            .select("movieId", "genre", "avg_rating")
            
        self.movie_profiles = self.movie_profiles.cache()

        # 3. GENERATE RECOMMENDATIONS (Cho tập User đã biết)
        # Join User thích 'Action' với Top 50 phim 'Action'
        recs = self.user_profiles.join(broadcast(self.movie_profiles), 
                                    self.user_profiles.top_genre == self.movie_profiles.genre) \
            .select("userId", "movieId", "avg_rating") \
            .distinct()

        # 4. LẤY TOP 10 FINAL
        # Lúc này dữ liệu đã rất nhẹ, window function sẽ chạy nhanh
        windowFinal = Window.partitionBy("userId").orderBy(desc("avg_rating"))
        
        self.final_recs = recs.withColumn("rank", row_number().over(windowFinal)) \
            .filter("rank <= 10") \
            .groupBy("userId") \
            .agg(collect_list(struct(col("movieId"), col("avg_rating").alias("rating"))).alias("recommendations"))
        
        print(f"   -> [CBF] Training Done. User Profiles & Movie Profiles Created.")
        return self

    def evaluate(self, test_data):
        from pyspark.ml.evaluation import RegressionEvaluator
        print("   [CBF] Đang đánh giá trên tập Test...")
        
        if self.user_profiles is None or self.movie_profiles is None:
             print("   [CBF] Model chưa được train. Không thể đánh giá.")
             return {"rmse": float('inf'), "mae": float('inf')}

        # Logic Prediction cho CBF: 
        # Nếu Movie thuộc thể loại Top của User -> Predict = Avg Rating của Movie đó
        # Nếu không -> Predict = Global Average (ví dụ 3.0)
        
        # Join User Profile
        test_with_profile = test_data.join(self.user_profiles, "userId", "left")
        
        # Predict = Movie's Avg Rating nếu Movie đó cũng thuộc Top Genre của User.
        predictions = test_with_profile.join(self.movie_profiles, 
            (test_with_profile.movieId == self.movie_profiles.movieId) & 
            (test_with_profile.top_genre == self.movie_profiles.genre), 
            "left") \
            .select(
                test_with_profile["userId"], 
                test_with_profile["movieId"], 
                col("rating").alias("actual"), 
                col("avg_rating").alias("prediction")
            ).na.fill(3.0, subset=["prediction"]) # Fill 3.0 nếu không tìm thấy match
            
        # Do join với profile (1 user có 2 genres) nên 1 dòng rating có thể sinh ra 2 dòng prediction
        predictions = predictions.groupBy("userId", "movieId", "actual") \
            .agg(avg("prediction").alias("prediction"))
            
        evaluator_rmse = RegressionEvaluator(metricName="rmse", labelCol="actual", predictionCol="prediction")
        evaluator_mae = RegressionEvaluator(metricName="mae", labelCol="actual", predictionCol="prediction")
        
        rmse = evaluator_rmse.evaluate(predictions)
        mae = evaluator_mae.evaluate(predictions)
        
        print(f"   [CBF] 📊 Kết quả: RMSE={rmse:.4f}, MAE={mae:.4f}")
        return {"rmse": rmse, "mae": mae}

    def get_recommendations(self, k=10):
        return self.final_recs