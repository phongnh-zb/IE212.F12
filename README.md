# Hệ thống gợi ý phim thông minh sử dụng Big Data

## Giới thiệu

Dự án triển khai một hệ thống gợi ý phim sử dụng nền tảng công nghệ Big Data bao gồm **Hadoop HDFS**, **HBase**, **MapReduce** và **Apache Spark**. Hệ thống xử lý dữ liệu từ bộ dataset **MovieLens 10M** để cung cấp các gợi ý phim được cá nhân hóa cho người dùng.

**Mục tiêu:**

- Xây dựng pipeline xử lý dữ liệu hoàn chỉnh từ ETL đến huấn luyện mô hình
- Kết hợp nhiều phương pháp gợi ý (Collaborative Filtering, Content-Based, Hybrid)
- Tạo giao diện web trực quan để người dùng tương tác với hệ thống

**Dataset:** MovieLens 10M Dataset

---

## Kiến trúc Hệ thống & Luồng Xử lý

Luồng dữ liệu hoạt động qua 4 giai đoạn chính:

### 1. Lưu trữ & Quản lý Dữ liệu (Hadoop & HBase)

**HDFS (Hadoop Distributed File System):**

- Lưu kết quả trung gian từ các job MapReduce
- Lưu trữ phân tán, có khả năng chịu lỗi cao

**HBase (NoSQL Database):**

- Đóng vai trò là cơ sở dữ liệu NoSQL truy xuất thời gian thực cho ứng dụng
- Cấu trúc bảng:

| Tên bảng          | Mô tả                     | Column Families                                            |
| ----------------- | ------------------------- | ---------------------------------------------------------- |
| `movies`          | Metadata phim và thống kê | `info` (title, genres), `stats` (avg_rating, rating_count) |
| `ratings`         | Lịch sử đánh giá          | `rating` (userId, movieId, rating, timestamp)              |
| `tags`            | Thẻ gắn cho phim          | `tag` (userId, movieId, tag, timestamp)                    |
| `recommendations` | Gợi ý đã tính toán        | `rec` (movieId, score, rank)                               |
| `genre_stats`     | Thống kê theo thể loại    | `stats` (genre, count, avg_rating)                         |

### 2. Xử lý Lô (Batch Processing - MapReduce)

**Công nghệ:** Python với Hadoop Streaming

**Các job MapReduce:**

1. **Rating Count Job:** Đếm số lượng đánh giá cho mỗi bộ phim
2. **Average Rating Job:** Tính điểm đánh giá trung bình cho mỗi bộ phim
3. **Genre Statistics Job:** Thống kê sự phân bố phim theo thể loại

**Luồng xử lý:**

```
CSV Data → HDFS → MapReduce Jobs → Intermediate Results (HDFS) → Load to HBase
```

### 3. Học Máy (Machine Learning - Apache Spark)

**Các mô hình:**

1. **ALS (Alternating Least Squares) - Collaborative Filtering**
   - Học từ lịch sử tương tác của người dùng
   - Dự đoán rating dựa trên hành vi người dùng tương tự
   - Phù hợp cho cold-start problem ở mức độ nhất định

2. **Content-Based Filtering**
   - Gợi ý dựa trên sự tương đồng về thể loại và metadata
   - Phân tích đặc trưng của phim (genres, tags)
   - Không phụ thuộc vào dữ liệu người dùng khác

3. **Hybrid Model (Mô hình Lai)**
   - Kết hợp điểm số từ ALS và Content-Based
   - Sử dụng chiến lược trung bình trọng số
   - Áp dụng "History Filter" để loại bỏ phim đã xem
   - Cải thiện độ chính xác và đa dạng của gợi ý

**Output:** Top-N recommendations cho mỗi user được lưu vào bảng `recommendations` trong HBase

### 4. Tầng Ứng dụng (Streamlit Web App)

**Kết nối:**

- Sử dụng HBase Thrift Server để giao tiếp với HBase
- Truy vấn real-time để lấy dữ liệu gợi ý và thống kê

**Tính năng:**

- **Gợi ý cá nhân:** Xem danh sách phim được gợi ý cho từng user cụ thể
- **Phân tích người dùng:** Lịch sử đánh giá, thể loại yêu thích
- **Thống kê hệ thống:** Biểu đồ phân bố thể loại, rating distribution, trending movies
- **Tìm kiếm & Lọc:** Tìm phim theo mã người dùng, tên phim

---

## Cấu trúc Dự án

```text
IE212.F12/
├── configs/
│   └── config.py                      # Cấu hình HBase host, HDFS paths, Spark configs
├── data/
│   ├── movies.csv                     # Dữ liệu phim (movieId, title, genres)
│   ├── ratings.csv                    # Dữ liệu đánh giá (userId, movieId, rating, timestamp)
│   └── tags.csv                       # Dữ liệu tags (userId, movieId, tag, timestamp)
├── lib/
│   └── hadoop-streaming-3.2.3.jar     # Thư viện Hadoop Streaming
├── scripts/
│   ├── start_services.sh              # Khởi động Hadoop, HBase, Thrift Server
│   ├── run_hbase.sh                   # Nạp dữ liệu thô vào HBase
│   └── run_mapreduce.sh               # Submit các MapReduce jobs
├── src/
│   ├── app.py                         # Entry point - Streamlit Web Application
│   ├── hbase/
│   │   ├── init_tables.py             # Khởi tạo/reset schema HBase
│   │   ├── load_movies.py             # Load movies.csv → HBase
│   │   ├── load_ratings.py            # Load ratings.csv → HBase
│   │   ├── load_tags.py               # Load tags.csv → HBase
│   │   └── load_stats.py              # Load MapReduce results → HBase
│   ├── mapreduce/
│   │   ├── count_movie_ratings/
│   │   │   ├── mapper.py              # Mapper đếm số lượng ratings
│   │   │   └── reducer.py             # Reducer đếm số lượng ratings
│   │   ├── calc_average_ratings/
│   │   │   ├── mapper.py              # Mapper tính rating trung bình
│   │   │   └── reducer.py             # Reducer tính rating trung bình
│   │   └── count_genres/
│   │       ├── mapper.py              # Mapper thống kê theo thể loại
│   │       └── reducer.py             # Reducer thống kê theo thể loại
│   ├── models/
│   │   ├── als_recommender.py         # Mô hình ALS
│   │   ├── content_based_recommender.py   # Mô hình Content-Based
│   │   └── hybrid_recommender.py      # Mô hình Hybrid
│   ├── utils/
│   │   └── hbase_utils.py             # Class kết nối và truy vấn HBase
│   └── run_training.py                # Entry point - Huấn luyện mô hình Spark
├── run_pipeline.sh                    # Script master - chạy toàn bộ pipeline
└── requirements.txt                   # Python dependencies
```

---

## Yêu cầu Hệ thống

### Phần mềm cần thiết:

| Thành phần       | Phiên bản     | Ghi chú                            |
| ---------------- | ------------- | ---------------------------------- |
| **Hệ điều hành** | Linux / macOS | Windows cần WSL                    |
| **Java JDK**     | 8 hoặc 11     | Cần thiết cho Hadoop & Spark       |
| **Hadoop**       | 3.x           | HDFS + YARN/MapReduce              |
| **HBase**        | 2.x           | Pseudo-Distributed hoặc Standalone |
| **Apache Spark** | 3.x           | Với PySpark                        |
| **Python**       | 3.9+          | Khuyến nghị 3.9 - 3.11             |

### Phần cứng khuyến nghị:

- **RAM:** Tối thiểu 8GB (khuyến nghị 16GB)
- **CPU:** 4 cores trở lên
- **Ổ cứng:** 50GB dung lượng trống (để chứa MovieLens 10M Dataset)

---

## Cài đặt & Thiết lập

### Bước 1: Clone Repository

```bash
git clone git@github.com:phongnh-zb/IE212.F12.git
cd IE212.F12
```

### Bước 2: Cài đặt Python Dependencies

```bash
pip install -r requirements.txt
```

### Bước 3: Cấu hình Biến Môi trường

Đảm bảo các biến môi trường sau đã được thiết lập:

```bash
# Thêm vào ~/.bashrc hoặc ~/.zshrc
export HADOOP_HOME=/path/to/hadoop
export HBASE_HOME=/path/to/hbase
export SPARK_HOME=/path/to/spark
export PATH=$PATH:$HADOOP_HOME/bin:$HBASE_HOME/bin:$SPARK_HOME/bin
```

Sau đó reload:

```bash
source ~/.bashrc  # hoặc source ~/.zshrc
```

### Bước 4: Kiểm tra Cấu hình

Mở file `configs/config.py` và kiểm tra các thông số:

```python
# HBase Configuration
HBASE_HOST = 'localhost'  # Thay đổi nếu HBase chạy trên remote server
HBASE_PORT = 9090

# HDFS Configuration
HDFS_URI = 'hdfs://localhost:9000'
HDFS_DATA_DIR = '/user/ie212/movielens'
```

### Bước 5: Chuẩn bị Dataset

**Cấu trúc dữ liệu:**

```bash
data/
├── movies.csv      # Format: movieId,title,genres
├── ratings.csv     # Format: userId,movieId,rating,timestamp
└── tags.csv        # Format: userId,movieId,tag,timestamp
```

---

## Hướng dẫn Sử dụng

### Chạy Pipeline Tự động

Cách đơn giản nhất để khởi động toàn bộ hệ thống:

```bash
./run_pipeline.sh [model_type]
```

**Tham số:**

- `all` - Huấn luyện tất cả các mô hình (mặc định)
- `als` - Chỉ mô hình ALS
- `cbf` - Chỉ mô hình Content-Based
- `hybrid` - Chỉ mô hình Hybrid

**Ví dụ:**

```bash
# Chạy toàn bộ pipeline với mô hình hybrid
./run_pipeline.sh hybrid

# Chạy tất cả các mô hình
./run_pipeline.sh
```

**Thời gian ước tính:** 30 --> 60 giây (tùy thuộc vào cấu hình phần cứng)

### Chạy Ứng dụng Web

Sau khi pipeline hoàn tất, khởi động giao diện web:

```bash
streamlit run src/app.py
```

**Truy cập:** `http://localhost:8501`

---

## Troubleshooting

### 1. Lỗi Broken Pipe / Thrift Connection Error

**Nguyên nhân:** HBase Thrift Server chưa khởi động hoặc bị treo

**Giải pháp:**

```bash
# Kiểm tra Thrift Server
jps | grep ThriftServer

# Nếu không chạy, khởi động thủ công
hbase thrift start -p 9090 &

# Hoặc restart qua script
./scripts/start_services.sh
```

### 2. Biểu đồ trên Dashboard bị trống

**Nguyên nhân:** MapReduce jobs chưa chạy hoặc dữ liệu chưa được load vào HBase

**Giải pháp:**

```bash
# Kiểm tra dữ liệu trong HBase
hbase shell
> scan 'genre_stats', {LIMIT => 10}
> scan 'movies', {LIMIT => 10}

# Nếu trống, chạy lại MapReduce và load stats
./scripts/run_mapreduce.sh
python3 src/hbase/load_stats.py
```

### 3. Lỗi Kết nối HBase

**Giải pháp:**

```bash
# Kiểm tra HBase Master đang chạy:
jps | grep HMaster

# Kiểm tra Zookeeper:
jps | grep QuorumPeerMain

# Kiểm tra cấu hình trong `configs/config.py`
# Phải khớp với hbase-site.xml
HBASE_HOST = '127.0.0.1'
```

### 4. Lỗi Out of Memory khi chạy Spark

**Giải pháp:**
Điều chỉnh memory trong `src/run_training.py`:

```python
spark = SparkSession.builder \
    .appName("MovieLens_10M_Pipeline") \
    .master("local[*]") \
    .config("spark.driver.memory", "6g") \
    .config("spark.executor.memory", "6g") \
    .config("spark.driver.maxResultSize", "2g") \
    .config("spark.sql.shuffle.partitions", "500") \
    .config("spark.default.parallelism", "500") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "2g") \
    .getOrCreate()
```

### 5. MapReduce Job bị Failed

**Kiểm tra logs:**

```bash
# Xem logs của job
yarn logs -applicationId <application_id>

# Hoặc kiểm tra trong HDFS
hdfs dfs -cat /user/ie212/movielens/output_rating_counts/_logs/*
```
