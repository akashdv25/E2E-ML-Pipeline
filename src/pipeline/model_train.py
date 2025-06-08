import os
import yaml
from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassifier
from src.logging import Log
from src.pipeline.ingestion import SparkLoader

# Set up logging
logger = Log.setup_logging()

class ModelTrainer:
    """
    This class handles model training operations.
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize model trainer with SparkSession.
        Args:
            spark (SparkSession): Existing SparkSession to use
        """
        self.spark = spark
        logger.info("ModelTrainer initialized with existing SparkSession", stacklevel=2)

    def train_random_forest(self, train_data, params: dict):
        """Train Random Forest model with specified parameters."""
        try:
            # Initialize Random Forest classifier with parameters from yaml
            rf = RandomForestClassifier(
                labelCol="label",
                featuresCol="features",
                numTrees=params['num_trees'],
                maxDepth=params['max_depth'],
                seed=params['seed'],
                featureSubsetStrategy=params['feature_subset_strategy'],
                impurity=params['impurity']
            )
            
            # Train the model
            logger.info("Starting Random Forest training...")
            model = rf.fit(train_data)
            logger.info("Random Forest training completed successfully")
            
            return model
            
        except Exception as e:
            logger.error(f"Error training Random Forest model: {str(e)}")
            raise

    def save_model(self, model, output_path: str):
        """Save the trained model."""
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Save the model
            model.save(output_path)
            logger.info(f"Model saved successfully to {output_path}")
            
        except Exception as e:
            logger.error(f"Error saving model: {str(e)}")
            raise

def main():
    """Main function to run the model training pipeline."""
    try:
        logger.info("Starting model training pipeline")
        
        # Load parameters
        with open('params.yaml', 'r') as file:
            params = yaml.safe_load(file)
        
        # Get paths
        train_data_path = os.path.join(
            params['model_training']['input_path'],
            params['model_training']['train_data']
        )
        model_output_path = os.path.join(
            params['model_training']['artifacts_path'],
            params['model_training']['model_name']
        )
        
        # Get the SparkSession
        spark_loader = SparkLoader()
        
        # Initialize model trainer
        trainer = ModelTrainer(spark_loader.spark)
        
        # Load training data
        train_data = spark_loader.spark.read.parquet(train_data_path)
        logger.info("Training data loaded successfully")
        
        # Train Random Forest model
        rf_model = trainer.train_random_forest(
            train_data,
            params['model_training']['random_forest']
        )
        
        # Save the model
        trainer.save_model(rf_model, model_output_path)
        
        logger.info("Model training pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Model training pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
