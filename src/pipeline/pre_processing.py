import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, isnan, when
import yaml
from src.logging import Log
from src.pipeline.ingestion import SparkLoader

# Set up logging
logger = Log.setup_logging()

class DataPreprocessor:
    """
    This class handles data preprocessing and cleaning operations.
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize preprocessor with SparkSession.
        Args:
            spark (SparkSession): Existing SparkSession to use
        """
        self.spark = spark
        logger.info("DataPreprocessor initialized with existing SparkSession", stacklevel=2)

    def load_data(self, data_path: str) -> DataFrame:
        """Load data from local CSV file."""
        try:
            df = self.spark.read.csv(data_path, header=True, inferSchema=True)
            logger.info(f"Data loaded successfully from {data_path}", stacklevel=2)
            return df
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}", stacklevel=2)
            raise

    def analyze_null_values(self, df: DataFrame) -> None:
        """Analyze and log null values in the dataset."""
        try:
            total_count = float(df.count())
            
            # Calculate null percentages for each column
            for column in df.columns:
                # Get column type
                col_type = dict(df.dtypes)[column]
                
                # Build null condition based on column type
                if col_type in ('int', 'double', 'float', 'long'):
                    null_condition = col(column).isNull()
                    if col_type != 'int':  # Only check isnan for floating point types
                        null_condition = null_condition | isnan(col(column))
                else:
                    null_condition = (
                        col(column).isNull() |
                        (col(column) == '') |
                        (col(column) == 'NULL') |
                        (col(column) == 'null')
                    )
                
                null_count = df.filter(null_condition).count()
                percentage = (null_count / total_count) * 100
                
                logger.info(f"Column: {column}, Null Count: {null_count}, Percentage: {percentage:.2f}%")
            
        except Exception as e:
            logger.error(f"Error analyzing null values: {str(e)}", stacklevel=2)
            raise

    def clean_data(self, df: DataFrame, params: dict) -> DataFrame:
        """
        Clean the dataset based on specified parameters.
        Args:
            df (DataFrame): Input DataFrame
            params (dict): Cleaning parameters from yaml
        """
        try:
            # Drop specified columns
            columns_to_drop = params.get('columns_to_drop', ['customer_id'])
            df_cleaned = df.drop(*columns_to_drop)
            logger.info(f"Dropped columns: {columns_to_drop}")

            # Remove records with specific values if specified in params
            if 'filter_conditions' in params:
                for condition in params['filter_conditions']:
                    column = condition['column']
                    value = condition['value']
                    operator = condition.get('operator', '!=')
                    
                    if operator == '!=':
                        df_cleaned = df_cleaned.filter(col(column) != value)
                    elif operator == '>':
                        df_cleaned = df_cleaned.filter(col(column) > value)
                    elif operator == '<':
                        df_cleaned = df_cleaned.filter(col(column) < value)
                    
                    logger.info(f"Applied filter: {column} {operator} {value}")

            # Handle missing values
            df_cleaned = df_cleaned.dropna()
            logger.info("Dropped rows with missing values")

            return df_cleaned

        except Exception as e:
            logger.error(f"Error cleaning data: {str(e)}", stacklevel=2)
            raise

    def save_processed_data(self, df: DataFrame, output_path: str) -> None:
        """Save processed DataFrame as a single CSV file."""
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Create a temporary directory for saving
            temp_dir = output_path + '_temp'
            
            # Save as single CSV file
            (df.coalesce(1)
               .write
               .mode("overwrite")
               .option("header", "true")
               .csv(temp_dir))
            
            # Rename the part file to the final name
            part_files = [f for f in os.listdir(temp_dir) if f.startswith('part-') and f.endswith('.csv')]
            if part_files:
                os.rename(
                    os.path.join(temp_dir, part_files[0]),
                    output_path
                )
                
                # Clean up temp directory
                import shutil
                shutil.rmtree(temp_dir)
                
            logger.info(f"Processed data saved to {output_path}", stacklevel=2)
            
        except Exception as e:
            logger.error(f"Error saving processed data: {str(e)}", stacklevel=2)
            raise

class LoadYamlParams:
    """Load parameters from YAML file."""
    
    def load_params(self, params_path: str = 'params.yaml') -> dict:
        try:
            with open(params_path, 'r') as file:
                params = yaml.safe_load(file)
            logger.info(f"Parameters loaded successfully from {params_path}", stacklevel=2)
            return params
        except Exception as e:
            logger.error(f"Error loading parameters: {str(e)}", stacklevel=2)
            raise

def main():
    """Main function to run the preprocessing pipeline."""
    try:
        logger.info("Starting preprocessing pipeline", stacklevel=2)
        
        # Load parameters
        params_obj = LoadYamlParams()
        params = params_obj.load_params()
        
        # Get paths from params
        raw_data_path = os.path.join(
            params['ingestion']['raw_data_path'],
            params['ingestion']['raw_data_file']
        )
        processed_data_path = os.path.join(
            params['pre_processing']['processed_data_path'],
            params['pre_processing']['processed_data_file']
        )
        
        # Get the SparkSession from SparkLoader
        spark_loader = SparkLoader()
        
        # Initialize preprocessor with existing SparkSession
        preprocessor = DataPreprocessor(spark_loader.spark)
        
        # Load data
        df = preprocessor.load_data(raw_data_path)
        
        # Analyze null values
        preprocessor.analyze_null_values(df)
        
        # Clean data based on params
        df_cleaned = preprocessor.clean_data(df, params['pre_processing'])
        
        # Save processed data
        preprocessor.save_processed_data(df_cleaned, processed_data_path)
        
        logger.info("Preprocessing pipeline completed successfully", stacklevel=2)
        
    except Exception as e:
        logger.error(f"Preprocessing pipeline failed: {str(e)}", stacklevel=2)
        raise

if __name__ == "__main__":
    main() 


