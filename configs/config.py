import os

# Đường dẫn gốc của Project (để dùng cho các file local)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR_LOCAL = os.path.join(PROJECT_ROOT, 'data')

# HADOOP / HDFS
HDFS_NAMENODE = "hdfs://localhost:9000"
HDFS_BASE_PATH = f"{HDFS_NAMENODE}/user/ie212/movielens/"

# Output paths của MapReduce jobs
HDFS_OUTPUT_RATINGS = f"{HDFS_BASE_PATH}output_rating_counts"
HDFS_OUTPUT_AVG = f"{HDFS_BASE_PATH}output_average_ratings"
HDFS_OUTPUT_GENRES = f"{HDFS_BASE_PATH}output_genre_counts"

# HBASE
HBASE_HOST = '127.0.0.1'
HBASE_TABLE_MOVIES = 'movies'
HBASE_TABLE_MOVIES = 'movies'
HBASE_TABLE_RATINGS = 'ratings'
HBASE_TABLE_TAGS = 'tags'
HBASE_TABLE_RECS = 'recommendations'

# FILES (CSV)
LINKS_FILE = "links.csv"
MOVIES_FILE = "movies.csv"
RATINGS_FILE = "ratings.csv"
TAGS_FILE = "tags.csv"

# SPARK CONFIGS
SPARK_APP_NAME = "MovieLens_ALS_Training"
SPARK_MASTER = "local[*]"