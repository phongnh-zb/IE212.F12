# FILE: src/models/content_based_recommender.py
from pyspark.sql.functions import (avg, col, collect_list, desc, explode,
                                   row_number, split, struct)
from pyspark.sql.window import Window


class ContentBasedRecommender:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.final_recs = None

    def train(self, df_ratings, df_movies):
        print("   -> Training Content-Based (Genre Matching)...")
        
        # 1. Tìm "Sở thích" của User (Các thể loại phim User chấm >= 4.0)
        # Join Ratings với Movies
        user_movies = df_ratings.filter("rating >= 4.0") \
            .join(df_movies, "movieId") \
            .select("userId", "genres")

        # Tách genres (Action|Adventure -> [Action, Adventure])
        user_genres = user_movies.withColumn("genre", explode(split(col("genres"), "\|"))) \
            .groupBy("userId", "genre").count() \
            .withColumn("rank", row_number().over(Window.partitionBy("userId").orderBy(desc("count")))) \
            .filter("rank <= 3") \
            .select("userId", col("genre").alias("top_genre"))
            # => Mỗi user lấy Top 3 thể loại họ xem nhiều nhất

        # 2. Tìm phim gợi ý (Phim thuộc Top Genres đó, rating trung bình cao)
        # Tính điểm trung bình cho từng phim
        movie_scores = df_ratings.groupBy("movieId").agg(avg("rating").alias("avg_rating"))
        
        # Phim + Genre + Score
        exploded_movies = df_movies.withColumn("genre", explode(split(col("genres"), "\|"))) \
            .join(movie_scores, "movieId")

        # 3. Join User Profile với Movies
        # Logic: User thích 'Action' -> Gợi ý phim 'Action' có avg_rating cao
        recs = user_genres.join(exploded_movies, user_genres.top_genre == exploded_movies.genre) \
            .select("userId", "movieId", "avg_rating") \
            .distinct()

        # 4. Lấy Top K phim cho mỗi User
        windowSpec = Window.partitionBy("userId").orderBy(desc("avg_rating"))
        self.final_recs = recs.withColumn("rank", row_number().over(windowSpec)) \
            .filter("rank <= 10") \
            .groupBy("userId") \
            .agg(collect_list(struct(col("movieId"), col("avg_rating").alias("rating"))).alias("recommendations"))
        
        print("   -> CBF Training Done.")
        return self

    def get_recommendations(self, k=10):
        return self.final_recs