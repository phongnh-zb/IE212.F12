from pyspark.sql.functions import (avg, col, collect_list, desc, explode,
                                   row_number, split, struct)
from pyspark.sql.window import Window


class ContentBasedRecommender:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.final_recs = None

    def train(self, df_ratings, df_movies):
        print("   -> Training Content-Based (Genre Matching)...")
        
        # 1. User Profile: Lấy Top 3 thể loại yêu thích
        user_movies = df_ratings.filter("rating >= 4.0") \
            .join(df_movies, "movieId") \
            .select("userId", "genres")

        user_genres = user_movies.withColumn("genre", explode(split(col("genres"), "\|"))) \
            .groupBy("userId", "genre").count() \
            .withColumn("rank", row_number().over(Window.partitionBy("userId").orderBy(desc("count")))) \
            .filter("rank <= 3") \
            .select("userId", col("genre").alias("top_genre"))
            
        # Cache lại User Profile vì nó nhỏ và dùng nhiều lần
        user_genres.cache()

        # 2. Movie Profile: Chỉ lấy Top 100 phim hay nhất của TỪNG thể loại
        # Để tránh việc Join ra hàng triệu dòng vô nghĩa
        
        # Bước 2a: Tính điểm trung bình phim
        movie_scores = df_ratings.groupBy("movieId") \
            .agg(avg("rating").alias("avg_rating")) \
            .filter("avg_rating >= 3.0") # Chỉ lấy phim khá trở lên

        # Bước 2b: Gán thể loại và lọc Top 100
        movies_exploded = df_movies.withColumn("genre", explode(split(col("genres"), "\|"))) \
            .join(movie_scores, "movieId")
            
        # Cửa sổ để lấy Top 100 phim per Genre
        windowGenre = Window.partitionBy("genre").orderBy(desc("avg_rating"))
        
        top_movies_per_genre = movies_exploded \
            .withColumn("rank_genre", row_number().over(windowGenre)) \
            .filter("rank_genre <= 100") \
            .select("movieId", "genre", "avg_rating") # Bỏ cột rank đi cho nhẹ
            
        # 3. Join User với Top Movies (Giờ đây dữ liệu nhỏ hơn rất nhiều)
        # Sử dụng Broadcast Join nếu bảng phim nhỏ (Giúp chạy cực nhanh)
        recs = user_genres.join(top_movies_per_genre, 
                                user_genres.top_genre == top_movies_per_genre.genre) \
            .select("userId", "movieId", "avg_rating") \
            .distinct()

        # 4. Lấy Top 10 Final cho mỗi User
        windowUser = Window.partitionBy("userId").orderBy(desc("avg_rating"))
        self.final_recs = recs.withColumn("rank", row_number().over(windowUser)) \
            .filter("rank <= 10") \
            .groupBy("userId") \
            .agg(collect_list(struct(col("movieId"), col("avg_rating").alias("rating"))).alias("recommendations"))
        
        # Trigger action để ép Spark chạy và giải phóng Cache sau khi xong
        count = self.final_recs.count()
        user_genres.unpersist()
        
        print(f"   -> CBF Training Done. Generated recs for {count} users.")
        return self

    def get_recommendations(self, k=10):
        return self.final_recs