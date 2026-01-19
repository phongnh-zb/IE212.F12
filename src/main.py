import sys
import os
sys.path.append(os.path.join(os.getcwd(), 'src'))

from pyspark.sql import SparkSession
from models import ALSRecommender, ContentBasedRecommender, HybridRecommender, ModelComparator
from utils.hbase_utils import get_all_data_from_hbase


def create_spark_session():
    return SparkSession.builder \
        .appName("Movie_recommender") \
        .master("local[*]") \
        .config("spark.driver.host", "localhost") \
        .getOrCreate()

def load_from_csv(spark, data_path="./data"):
    rating = spark.read.csv(f"{data_path}/ratings.csv", header=True, inferSchema=True)
    movies = spark.read.csv(f"{data_path}/movies.csv", header=True, inferSchema=True)
    tags = spark.read.csv(f"{data_path}/tags.csv", header=True, inferSchema=True)
    return rating, movies, tags

def load_data(spark):
    try:
        ratings, movies, tags = get_all_data_from_hbase(spark)
        print("read data from HBase")

        return ratings, movies, tags
    except Exception as e:
        print("read data from csv")
        return load_from_csv(spark)

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    ratings, movies, tags = load_data(spark)
    train_data, test_data = ratings.randomSplit([.8, .2], seed=42)
    
    comparator = ModelComparator(spark)
    
    # ALS
    als_model = ALSRecommender(spark)
    als_model.train(train_data, rank=15, maxIter=10, regParam=0.1)
    comparator.evaluate(als_model.predict(test_data), "ALS")
    
    # Content-Based
    cbf_model = ContentBasedRecommender(spark)
    cbf_model.train(movies, tags)
    comparator.evaluate(cbf_model.predict(test_data, train_data), "Content-Based")
    
    # Hybrid
    hybrid_model = HybridRecommender(spark, als_model, cbf_model, alpha=0.7, beta=0.3)
    comparator.evaluate(hybrid_model.predict(test_data, train_data), "Hybrid")
    
    print("\n--- FINAL MODEL COMPARISON SUMMARY ---")
    print(comparator.compare_all())
    spark.stop()

if __name__ == "__main__":
    main()
