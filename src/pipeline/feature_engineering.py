import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml import Pipeline
import yaml
from src.logging import Log
from src.pipeline.ingestion import SparkLoader

# Set up logging
logger = Log.setup_logging()

class FeatureEngineer:
    """
    This class handles feature engineering operations for the bank churn prediction model.
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize feature engineer with SparkSession.
        Args:
            spark (SparkSession): Existing SparkSession to use
        """
        self.spark = spark
        logger.info("FeatureEngineer initialized with existing SparkSession", stacklevel=2)

    def create_age_features(self, df: DataFrame) -> DataFrame:
        """Create age-related features."""
        try:
            df = df.withColumn(
                'age_group',
                F.when(F.col('age') < 30, 'Young')
                .when((F.col('age') >= 30) & (F.col('age') < 50), 'Middle-Aged')
                .otherwise('Senior')
            )
            df = df.withColumn('is_senior', F.when(F.col('age') >= 60, 1).otherwise(0))
            logger.info("Age-related features created successfully")
            return df
        except Exception as e:
            logger.error(f"Error creating age features: {str(e)}")
            raise

    def create_balance_features(self, df: DataFrame) -> DataFrame:
        """Create balance-related features."""
        try:
            df = df.withColumn(
                'balance_category',
                F.when(F.col('balance') == 0, 'Zero')
                .when(F.col('balance') < 50000, 'Low')
                .when(F.col('balance') < 100000, 'Medium')
                .otherwise('High')
            )
            df = df.withColumn(
                'balance_salary_ratio',
                F.col('balance') / F.when(F.col('estimated_salary') == 0, 1)
                .otherwise(F.col('estimated_salary'))
            )
            logger.info("Balance-related features created successfully")
            return df
        except Exception as e:
            logger.error(f"Error creating balance features: {str(e)}")
            raise

    def create_product_features(self, df: DataFrame) -> DataFrame:
        """Create product-related features."""
        try:
            df = df.withColumn(
                'product_engagement_score',
                F.col('products_number') * F.col('active_member')
            )
            df = df.withColumn(
                'is_multi_product',
                F.when(F.col('products_number') > 1, 1).otherwise(0)
            )
            logger.info("Product-related features created successfully")
            return df
        except Exception as e:
            logger.error(f"Error creating product features: {str(e)}")
            raise

    def create_tenure_features(self, df: DataFrame) -> DataFrame:
        """Create tenure-related features."""
        try:
            df = df.withColumn(
                'tenure_group',
                F.when(F.col('tenure') <= 2, 'New')
                .when((F.col('tenure') > 2) & (F.col('tenure') <= 5), 'Established')
                .otherwise('Loyal')
            )
            df = df.withColumn(
                'avg_balance_per_tenure',
                F.col('balance') / (F.col('tenure') + 1)
            )
            logger.info("Tenure-related features created successfully")
            return df
        except Exception as e:
            logger.error(f"Error creating tenure features: {str(e)}")
            raise

    def create_credit_features(self, df: DataFrame) -> DataFrame:
        """Create credit-related features."""
        try:
            df = df.withColumn(
                'credit_score_category',
                F.when(F.col('credit_score') < 600, 'Poor')
                .when((F.col('credit_score') >= 600) & (F.col('credit_score') < 700), 'Fair')
                .when((F.col('credit_score') >= 700) & (F.col('credit_score') < 800), 'Good')
                .otherwise('Excellent')
            )
            df = df.withColumn(
                'credit_balance_ratio',
                F.col('balance') / F.when(F.col('credit_score') == 0, 1)
                .otherwise(F.col('credit_score'))
            )
            logger.info("Credit-related features created successfully")
            return df
        except Exception as e:
            logger.error(f"Error creating credit features: {str(e)}")
            raise

    def create_customer_value_features(self, df: DataFrame) -> DataFrame:
        """Create customer value and risk-related features."""
        try:
            df = df.withColumn(
                'customer_value_score',
                (F.col('balance') * 0.4 +
                 F.col('estimated_salary') * 0.3 +
                 F.col('credit_score') * 0.3) / 1000
            )
            df = df.withColumn(
                'risk_score',
                (1000 - F.col('credit_score')) * 0.5 +
                (F.when(F.col('balance') == 0, 100).otherwise(0)) * 0.3 +
                (F.when(F.col('active_member') == 0, 100).otherwise(0)) * 0.2
            )
            df = df.withColumn(
                'activity_level',
                F.when(
                    (F.col('active_member') == 1) &
                    (F.col('products_number') > 1) &
                    (F.col('balance') > 0),
                    'High'
                ).when(
                    (F.col('active_member') == 1) &
                    (F.col('products_number') == 1),
                    'Medium'
                ).otherwise('Low')
            )
            logger.info("Customer value features created successfully")
            return df
        except Exception as e:
            logger.error(f"Error creating customer value features: {str(e)}")
            raise

    def create_ml_features(self, df: DataFrame, categorical_cols: list) -> DataFrame:
        """Create machine learning ready features."""
        try:
            # Get numerical columns (excluding categorical and target)
            numerical_cols = [col for col in df.columns 
                            if col not in categorical_cols + ['churn']
                            and df.schema[col].dataType.typeName() in ('integer', 'double')]

            # Create pipeline stages
            stages = []

            # Handle categorical features
            for col in categorical_cols:
                indexer = StringIndexer(
                    inputCol=col,
                    outputCol=f"{col}_index",
                    handleInvalid="keep"
                )
                encoder = OneHotEncoder(
                    inputCols=[f"{col}_index"],
                    outputCols=[f"{col}_vec"]
                )
                stages.extend([indexer, encoder])

            # Combine all features
            assembler_inputs = [f"{col}_vec" for col in categorical_cols] + numerical_cols
            assembler = VectorAssembler(
                inputCols=assembler_inputs,
                outputCol="features_unscaled",
                handleInvalid="keep"
            )
            stages.append(assembler)

            # Scale features
            scaler = StandardScaler(
                inputCol="features_unscaled",
                outputCol="features",
                withStd=True,
                withMean=True
            )
            stages.append(scaler)

            # Create and apply pipeline
            pipeline = Pipeline(stages=stages)
            model = pipeline.fit(df)
            processed_df = model.transform(df)

            # Select only needed columns
            final_df = processed_df.select(
                "features",
                F.col("churn").cast("double").alias("label")
            )

            logger.info("ML features created successfully")
            return final_df
        except Exception as e:
            logger.error(f"Error creating ML features: {str(e)}")
            raise

    def save_feature_data(self, train_df: DataFrame, test_df: DataFrame, 
                         train_path: str, test_path: str) -> None:
        """Save train and test DataFrames as single Parquet files."""
        try:
            # Save train data as a single parquet file
            (train_df.coalesce(1)
                    .write
                    .mode("overwrite")
                    .format("parquet")
                    .save(train_path))
            
            # Save test data as a single parquet file
            (test_df.coalesce(1)
                   .write
                   .mode("overwrite")
                   .format("parquet")
                   .save(test_path))
            
            # Clean up and rename files to have a single clean parquet file
            for path in [train_path, test_path]:
                part_file = os.path.join(path, [f for f in os.listdir(path) 
                                              if f.startswith("part-") and f.endswith(".parquet")][0])
                final_path = path + ".parquet"
                os.rename(part_file, final_path)
                
                # Remove the directory with extra files
                import shutil
                shutil.rmtree(path)
            
            logger.info(f"Train data saved to {train_path}.parquet")
            logger.info(f"Test data saved to {test_path}.parquet")
        except Exception as e:
            logger.error(f"Error saving feature data: {str(e)}")
            raise

def main():
    """Main function to run the feature engineering pipeline."""
    try:
        logger.info("Starting feature engineering pipeline")
        
        # Load parameters
        with open('params.yaml', 'r') as file:
            params = yaml.safe_load(file)
        
        # Get paths
        input_path = os.path.join(
            params['feature_engineering']['input_path'],
            params['feature_engineering']['input_file']
        )
        train_path = os.path.join(
            params['feature_engineering']['output_path'],
            params['feature_engineering']['train_file']
        )
        test_path = os.path.join(
            params['feature_engineering']['output_path'],
            params['feature_engineering']['test_file']
        )
        
        # Get the SparkSession
        spark_loader = SparkLoader()
        
        # Initialize feature engineer
        feature_engineer = FeatureEngineer(spark_loader.spark)
        
        # Load processed data
        df = spark_loader.spark.read.csv(input_path, header=True, inferSchema=True)
        logger.info("Processed data loaded successfully")
        
        # Create features
        df = feature_engineer.create_age_features(df)
        df = feature_engineer.create_balance_features(df)
        df = feature_engineer.create_product_features(df)
        df = feature_engineer.create_tenure_features(df)
        df = feature_engineer.create_credit_features(df)
        df = feature_engineer.create_customer_value_features(df)
        
        # Create ML features
        final_df = feature_engineer.create_ml_features(
            df, 
            params['feature_engineering']['categorical_columns']
        )
        
        # Split data
        train_df, test_df = final_df.randomSplit(
            [1 - params['feature_engineering']['test_size'],
             params['feature_engineering']['test_size']],
            seed=params['feature_engineering']['random_seed']
        )
        
        # Save train and test data
        feature_engineer.save_feature_data(train_df, test_df, train_path, test_path)
        
        logger.info("Feature engineering pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Feature engineering pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
