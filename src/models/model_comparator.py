from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col
import pandas as pd


class ModelComparator:
    def __init__(self, spark):
        self.spark = spark
        self.results = {}

    def evaluate(self, predictions, model_name):
        predictions_clean = predictions.filter(col('prediction').isNotNull())

        rmse_eval = RegressionEvaluator(
            metricName='rmse',
            labelCol="rating",
            predictionCol="prediction"
        )
        rmse = rmse_eval.evaluate(predictions_clean)
        mae_eval = RegressionEvaluator(
            metricName='mae',
            labelCol="rating",
            predictionCol="prediction"
        )
        mae = mae_eval.evaluate(predictions_clean)

        self.results[model_name] = {
            "RMSE": round(rmse, 4),
            "MAE": round(mae, 4),
        }

        print(f"\n{'=' * 50}")
        print(f"{model_name}")
        print(f"{'=' * 50}")
        print(f"    RMSE: {rmse:.4f}")
        print(f"    MAE: {mae:.4f}")
        return self.results[model_name]

    def evaluate_ranking(self, model, test_data, k=10):
        model_name = model.name
        users = [row.userId for row in test_data.select("userId").distinct().limit(100).collect()]

        precisions = []
        recalls = []
        for user_id in users:
            actual = test_data.filter(
                (col("userId") == user_id) & (col("rating") >= 4.0)
            ).select("movieId").collect()
            actual_set = set([row.movieId for row in actual])

            if not actual_set:
                continue
            try:
                recs = model.recommend_for_user(user_id, k)
                if hasattr(recs, 'collect'):
                    rec_ids = set([row.movieId for row in recs.collect()])
                else:
                    continue
            except:
                continue

            hits = len(actual_set & rec_ids)
            precision = hits / k if k > 0 else 0
            recall = hits / len(actual_set) if actual_set else 0

            precisions.append(precision)
            recalls.append(recall)

        avg_precision = sum(precisions) / len(precisions) if precisions else 0
        avg_recall = sum(recalls) / len(recalls) if recalls else 0

        if model_name not in self.results:
            self.results[model_name] = {}

        self.results[model_name][f"Precision@{k}"] = round(avg_precision, 4)
        self.results[model_name][f"Recall@{k}"] = round(avg_recall, 4)

        print(f"    Precision@{k}: {avg_precision:.4f}")
        print(f"    Recall@{k}: {avg_recall:.4f}")
        return avg_precision, avg_recall

    def compare_all(self):

        if not self.results:
            print("No results to compare")
            return None

        df = pd.DataFrame(self.results).T
        df = df.sort_values("RMSE", ascending=True)

        print("\n" + "=" * 70)
        print("MODEL COMPARISON RESULTS")
        print("=" * 70)
        print(df.to_string())
        print("=" * 70)

        best_model = df.index[0]
        print(f"Best Model (lowest RMSE): {best_model}")
        return df

    def export_results(self, filepath="results/model_comparison.csv"):
        if not self.results:
            return
        df = pd.DataFrame(self.results).T
        df.to_csv(filepath)
