from pyspark.sql import SparkSession
from models import ALSRecommender, ContentBasedRecommender, HybridRecommender, ModelComparator


def create_spark_session():
    return SparkSession.builder \
        .appName("Movie_recommender") \
        .master("local[*]") \
        .config("spark.driver.host", "localhost") \
        .getOrCreate()
def load_data(spark, data_path="./data"):
    rating = spark.read.csv(f"{data_path}/ratings.csv", header=True, inferSchema=True)
    movies = spark.read.csv(f"{data_path}/movies.csv", header=True, inferSchema=True)
    tags = spark.read.csv(f"{data_path}/tags.csv", header=True, inferSchema=True)

    return rating, movies, tags

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    ratings, movies, tags = load_data(spark)
    train_data, test_data = ratings.randomSplit([.8, .2], seed=42)
    train_data.cache()
    test_data.cache()

    print(f"\nTrain: {train_data.count()}, Test: {test_data.count()}")

    comparator = ModelComparator(spark)
    print("\n" + "=" * 60)
    print("TRAINING ALS MODEL")
    print("=" * 60)

    als_model = ALSRecommender(spark)
    als_model.train(train_data, rank=15, maxIter=10, regParam=0.1)
    als_predictions = als_model.predict(test_data)
    comparator.evaluate(als_predictions, als_model.name)

    print("\n" + "=" * 60)
    print("TRAINING CONTENT_BASED MODEL")
    print("=" * 60)

    cbf_model = ContentBasedRecommender(spark)
    cbf_model.train(movies, tags)
    cbf_predictions = cbf_model.predict(test_data, train_data)
    comparator.evaluate(cbf_predictions, cbf_model.name)

    print("\n" + "=" * 60)
    print("TRAINING HYBRID MODEL")
    print("=" * 60)

    hybrid_model = HybridRecommender(spark, als_model, cbf_model, alpha=0.7, beta=0.3)
    hybrid_predictions = hybrid_model.predict(test_data, train_data)
    comparator.evaluate(hybrid_predictions, hybrid_model.name)

    comparison_df = comparator.compare_all()
    print("\n" + "=" * 60)
    print("FINAL MODEL COMPARISON SUMMARY")
    print("=" * 60)
    print(comparison_df)

    spark.stop()

if __name__ == "__main__":
    main()




