from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, coalesce, when


class HybridRecommender:
    def __init__(self, spark, als_model, content_model, alpha=0.7, beta=0.3):
        self.spark = spark
        self.als_model = als_model
        self.content_model = content_model
        self.alpha = alpha
        self.beta = beta
        self.name = f"hybrid (alpha={alpha}, beta={beta})"

    def recommend_for_user(self, user_id, ratings_df, movies_df, n=10):
        als_recs = self.als_model.recommend_for_user(user_id, n * 3)
        als_df = als_recs.withColumnRenamed("score", "als_score")

        als_min = als_df.agg({"als_score": "min"}).collect()[0][0] or 0
        als_max = als_df.agg({"als_score": "max"}).collect()[0][0] or 1
        als_range = als_max - als_min if als_max != als_min else 1

        als_df = als_df.withColumn(
            "als_norm",
            (col("als_score") - lit(als_min)) / lit(als_range),
        )

        cbf_recs = self.content_model.recommend_for_user(user_id, ratings_df, n * 3)
        cbf_df = cbf_recs.withColumnRenamed("score", "cbf_score") \
            .select("movieId", "cbf_score")
        combined = als_df.join(cbf_df, "movieId", "outer")

        combined = combined.withColumn(
            "als_norm",
            coalesce(col("als_norm"), lit(0.0))
        ).withColumn(
            "cbf_score",
            coalesce(col("cbf_score"), lit(0.0))
        )

        cbf_min = combined.agg({"cbf_score": "min"}).collect()[0][0] or 0
        cbf_max = combined.agg({"cbf_score": "max"}).collect()[0][0] or 1
        cbf_range = cbf_max - cbf_min if cbf_max != cbf_min else 1

        combined = combined.withColumn(
            "cbf_norm",
            (col("cbf_score") - lit(cbf_min)) / lit(cbf_range),
        )

        combined = combined.withColumn(
            "hybrid_score",
            col("als_norm") * lit(self.alpha) + col("cbf_norm") * lit(self.beta)
        )

        result = combined.orderBy(col("hybrid_score").desc()) \
            .limit(n) \
            .join(movies_df, "movieId") \
            .select(
            lit(user_id).alias("userId"),
            "movieId",
            "title",
            "genres",
            "als_score",
            "cbf_score",
            col("hybrid_score").alias("score")
        )

        return result

    def predict(self, test_data, rating_df):
        als_predictions = self.als_model.predict(test_data) \
            .withColumnRenamed("prediction", "als_pred")

        cbf_predictions = self.content_model.predict(test_data, rating_df) \
            .select("userId", "movieId", col("prediction").alias("cbf_pred"))

        combined = als_predictions.join(
            cbf_predictions,
            ["userId", "movieId"],
            "left"
        )
        combined = combined.withColumn(
            "cbf_pred",
            coalesce(col("cbf_pred"), lit(3.0))
        )
        combined = combined.withColumn(
            "prediction",
            col("als_pred") * lit(self.alpha) + col("cbf_pred") * lit(self.beta)
        )

        combined = combined.withColumn(
            "prediction",
            when(col("prediction") > 5.0, 5.0)
            .when(col("prediction") < 0.5, 0.5)
            .otherwise(col("prediction"))
        )

        # Defensive column selection
        available_cols = combined.columns
        target_cols = ["userId", "movieId", "rating", "prediction"]
        select_cols = [c for c in target_cols if c in available_cols]

        return combined.select(*select_cols)

    def set_weights(self, alpha, beta):
        if abs(alpha + beta - 1.0) > 0.01:
            total = alpha + beta
            alpha = alpha / total
            beta = beta / total
        self.alpha = alpha
        self.beta = beta
        self.name = f"hybrid (alpha={alpha}, beta={beta})"
        return self
