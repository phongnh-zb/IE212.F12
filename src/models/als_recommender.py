from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql.functions import col, explode
import numpy as np

class ALSRecommender:
    def __init__(self, spark):
        self.spark = spark
        self.model = None
        self.name = "ALS"

    def train(self, train_data, rank=10, maxIter=10, regParam=0.1):
        als = ALS(
            rank=rank,
            maxIter=maxIter,
            regParam=regParam,
            userCol="userId",
            itemCol="movieId",
            ratingCol="rating",
            coldStartStrategy="drop"
        )
        self.model = als.fit(train_data)
        return self

    def train_with_cv(self, train_data):
        als = ALS(
            userCol="userId",
            itemCol="movieId",
            ratingCol="rating",
            coldStartStrategy="drop"
        )
        param_grid = ParamGridBuilder() \
                    .addGrid(als.rank, [10, 20]) \
                    .addGrid(als.regParam, [0.01, 0.1]) \
                    .addGrid(als.maxIter, [10, 15]) \
                    .build()
        evaluator = RegressionEvaluator(
            metricName="rmse",
            labelCol="rating",
            predictionCol="prediction"
        )
        cv = CrossValidator(
            estimator=als,
            estimatorParamMaps=param_grid,
            evaluator=evaluator,
            numFolds=3,
            seed=42
        )
        cv_model = cv.fit(train_data)
        self.model = cv_model.bestModel
        print(f"Best Rank: {self.model.rank}")
        print(f"Best RegParam: {self.model._java_obj.parent().getRegParam()}")

        return self

    def predict(self, test_data):
        return self.model.transform(test_data)

    def recommend_for_user(self, user_id, n= 10):
        user_df = self.spark.createDataFrame([(user_id,)], ["userId"])
        recommendations = self.model.recommendForUserSubset(user_df, n)
        return recommendations.select(
            "userId",
            explode("recommendations").alias("rec")
        ).select(
            "userId",
            col("rec.movieId").alias("movieId"),
            col("rec.rating").alias("score")
        )

    def get_item_factors(self):
        return self.model.itemFactors

    def get_user_factors(self):
        return self.model.userFactors

