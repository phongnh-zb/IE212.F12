import happybase
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def load_hbase_table(spark: SparkSession, table_name: str, columns: list, column_family: str = 'cf'):
    try:
        connection = happybase.Connection('localhost', port=9090)
        table = connection.table(table_name)
        
        data = []
        for key, row in table.scan():
            item = {}
            for c in columns:
                qualifier = f"{column_family}:{c}".encode()
                if qualifier in row:
                    item[c] = row[qualifier].decode('utf-8')
                else:
                    item[c] = None
            data.append(item)
        
        connection.close()
        
        if not data:
            return spark.createDataFrame([], schema=", ".join([f"{c} string" for c in columns]))
            
        pdf = pd.DataFrame(data)
        df = spark.createDataFrame(pdf)
        
        return df
    except Exception as e:
        print(f"Error loading from HBase table {table_name}: {e}")
        raise e

def get_all_data_from_hbase(spark: SparkSession):
    ratings_raw = load_hbase_table(spark, 'ratings', ['userId', 'movieId', 'rating'])
    ratings = ratings_raw.withColumn("userId", col("userId").cast("int")) \
                         .withColumn("movieId", col("movieId").cast("int")) \
                         .withColumn("rating", col("rating").cast("double"))
    
    movies_raw = load_hbase_table(spark, 'movies', ['movieId', 'title', 'genres'])
    movies = movies_raw.withColumn("movieId", col("movieId").cast("int"))
    
    tags_raw = load_hbase_table(spark, 'tags', ['userId', 'movieId', 'tag'])
    tags = tags_raw.withColumn("userId", col("userId").cast("int")) \
                   .withColumn("movieId", col("movieId").cast("int"))
                   
    return ratings, movies, tags
