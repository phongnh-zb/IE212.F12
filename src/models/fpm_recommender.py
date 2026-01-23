from pyspark.ml.fpm import FPGrowth
from pyspark.sql.functions import collect_list


class FPGrowthRecommender:
    def __init__(self, spark):
        self.spark = spark
        self.model = None

    def train(self, df_ratings):
        # FP-Growth cần input là: UserID | [List các phim đã xem]
        # Gom nhóm rating theo User
        df_basket = df_ratings.groupBy("userId") \
            .agg(collect_list("movieId").alias("items"))

        # Khởi tạo thuật toán
        fp = FPGrowth(itemsCol="items", minSupport=0.01, minConfidence=0.1)
        self.model = fp.fit(df_basket)
        print("✅ FP-Growth Model Trained!")

    def get_recommendations(self, k=10):
        # Lấy các luật kết hợp (Association Rules)
        # Antecedent: Nếu xem cái này -> Consequent: Thì xem cái kia
        if not self.model: return None
        
        # Transform để gợi ý cho user dựa trên lịch sử của họ
        # Lưu ý: FP-Growth output hơi khác format chuẩn, cần xử lý thêm để khớp với HBase
        return self.model.associationRules