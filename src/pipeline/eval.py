import os
import json
import yaml
from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from src.logging import Log
from src.pipeline.ingestion import SparkLoader

# Set up logging
logger = Log.setup_logging()

class ModelEvaluator:
    """
    Class for evaluating the trained model and generating performance metrics.
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize evaluator with SparkSession.
        Args:
            spark (SparkSession): Existing SparkSession to use
        """
        self.spark = spark
        logger.info("ModelEvaluator initialized with existing SparkSession")

    def load_model(self, model_path: str):
        """Load the trained model from disk."""
        try:
            model = RandomForestClassificationModel.load(model_path)
            logger.info(f"Model loaded successfully from {model_path}")
            return model
        except Exception as e:
            logger.error(f"Error loading model: {str(e)}")
            raise

    def load_test_data(self, test_data_path: str):
        """Load test data from disk."""
        try:
            test_data = self.spark.read.parquet(test_data_path)
            logger.info(f"Test data loaded successfully from {test_data_path}")
            return test_data
        except Exception as e:
            logger.error(f"Error loading test data: {str(e)}")
            raise

    def calculate_metrics(self, model, test_data):
        """Calculate various classification metrics."""
        try:
            # Make predictions
            predictions = model.transform(test_data)
            
            # Initialize evaluators
            binary_evaluator = BinaryClassificationEvaluator(
                labelCol="label", rawPredictionCol="rawPrediction"
            )
            multi_evaluator = MulticlassClassificationEvaluator(
                labelCol="label", predictionCol="prediction"
            )

            # Calculate metrics
            metrics = {
                "accuracy": multi_evaluator.setMetricName("accuracy").evaluate(predictions),
                "precision": multi_evaluator.setMetricName("weightedPrecision").evaluate(predictions),
                "recall": multi_evaluator.setMetricName("weightedRecall").evaluate(predictions),
                "f1": multi_evaluator.setMetricName("f1").evaluate(predictions),
                "auc_roc": binary_evaluator.setMetricName("areaUnderROC").evaluate(predictions)
            }

            # Calculate confusion matrix
            confusion_matrix = predictions.groupBy("label", "prediction").count().collect()
            metrics["confusion_matrix"] = [
                {"actual": row["label"], 
                 "predicted": row["prediction"], 
                 "count": row["count"]} for row in confusion_matrix
            ]

            logger.info("Model evaluation metrics calculated successfully")
            return metrics

        except Exception as e:
            logger.error(f"Error calculating metrics: {str(e)}")
            raise

    def save_metrics(self, metrics: dict, output_path: str):
        """Save metrics to JSON file."""
        try:
            # Create metrics directory if it doesn't exist
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Save metrics to JSON file
            with open(output_path, 'w') as f:
                json.dump(metrics, f, indent=4)
            
            logger.info(f"Metrics saved successfully to {output_path}")
        except Exception as e:
            logger.error(f"Error saving metrics: {str(e)}")
            raise

def main():
    """Main function to run model evaluation."""
    try:
        # Load parameters
        with open("params.yaml", 'r') as f:
            params = yaml.safe_load(f)

        # Initialize Spark session using SparkLoader
        spark_loader = SparkLoader()

        # Initialize evaluator
        evaluator = ModelEvaluator(spark_loader.spark)

        # Load model
        model_path = os.path.join(
            params['model_training']['artifacts_path'],
            params['model_training']['model_name']
        )
        model = evaluator.load_model(model_path)

        # Load test data
        test_data_path = os.path.join(
            params['model_training']['input_path'],
            params['evaluation']['test_data']
        )
        test_data = evaluator.load_test_data(test_data_path)

        # Calculate metrics
        metrics = evaluator.calculate_metrics(model, test_data)

        # Save metrics
        metrics_path = os.path.join(
            params['evaluation']['metrics_path'],
            params['evaluation']['metrics_file']
        )
        evaluator.save_metrics(metrics, metrics_path)

        logger.info("Model evaluation completed successfully")
        spark_loader.spark.stop()

    except Exception as e:
        logger.error(f"Error in model evaluation: {str(e)}")
        raise

if __name__ == "__main__":
    main()
