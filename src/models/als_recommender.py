import time

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder


class ALSRecommender:
    def __init__(self, spark):
        self.spark = spark
        self.best_model = None  # Lưu model tốt nhất sau khi CV

    def train(self, train_data):
        print("   [ALS] Bắt đầu quá trình Cross Validation (Hyperparameter Tuning)...")
        start_time = time.time()

        # 2. Khởi tạo ALS Estimator
        # coldStartStrategy="drop": Bỏ qua các user/movie chưa từng xuất hiện trong tập train để tránh lỗi NaN
        als = ALS(
            userCol="userId", 
            itemCol="movieId", 
            ratingCol="rating", 
            coldStartStrategy="drop",
            nonnegative=True
        )

        # 3. Xây dựng lưới tham số (Parameter Grid) để thử nghiệm
        # Lưu ý: Càng nhiều tham số thì chạy càng lâu
        param_grid = ParamGridBuilder() \
            .addGrid(als.rank, [10]) \
            .addGrid(als.regParam, [0.1]) \
            .build()

        # 4. Định nghĩa thước đo đánh giá (RMSE - Root Mean Squared Error)
        evaluator = RegressionEvaluator(
            metricName="rmse", 
            labelCol="rating", 
            predictionCol="prediction"
        )

        # 5. Thiết lập Cross Validator
        # numFolds=2: Chia dữ liệu làm 3 phần, train 1 phần test 1 phần (xoay vòng)
        cv = CrossValidator(
            estimator=als,
            estimatorParamMaps=param_grid,
            evaluator=evaluator,
            numFolds=2 
        )

        # 6. Bắt đầu chạy (FIT)
        print("   [ALS] Đang chạy CrossValidator (có thể mất vài phút)...")
        cv_model = cv.fit(train_data)

        # 7. Lấy ra model tốt nhất
        self.best_model = cv_model.bestModel
        
        # In kết quả tối ưu
        best_rank = self.best_model.rank
        best_reg = self.best_model._java_obj.parent().getRegParam()
        print(f"   [ALS] ✅ Tìm thấy tham số tốt nhất: Rank={best_rank}, RegParam={best_reg}")
        print(f"   [ALS] Thời gian training: {time.time() - start_time:.2f}s")
        return self

    def evaluate(self, test_data):
        if not self.best_model:
            print("   [ALS] ❌ Model chưa (hoặc thất bại) training.")
            return {"rmse": float('inf'), "mae": float('inf')}
            
        print("   [ALS] Đang đánh giá trên tập Test...")
        predictions = self.best_model.transform(test_data)
        
        evaluator_rmse = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
        evaluator_mae = RegressionEvaluator(metricName="mae", labelCol="rating", predictionCol="prediction")
        
        rmse = evaluator_rmse.evaluate(predictions)
        mae = evaluator_mae.evaluate(predictions)
        
        print(f"   [ALS] 📊 Kết quả: RMSE={rmse:.4f}, MAE={mae:.4f}")
        return {"rmse": rmse, "mae": mae}

    def get_recommendations(self, k=10):
        # recommendForAllUsers(k) trả về cột 'recommendations' chứa mảng các struct (movieId, rating)
        return self.best_model.recommendForAllUsers(k)