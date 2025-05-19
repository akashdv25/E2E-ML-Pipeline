import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

from config import setup_logging

# Set up logging at the module level
logger = setup_logging()

class LoadCreds:

    """
    This class is used to load the AWS credentials from the .env file.

    Args: None

    Attributes:
        AWS_ACCESS_KEY_ID: str
        AWS_SECRET_ACCESS_KEY: str
        AWS_REGION: str
        S3_BUCKET_NAME: str    
        
    """

    # Initialize the class
    def __init__(self):
        logger.info("Initializing LoadCreds Class and loading environment variables")
        load_dotenv()
        self.AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
        self.AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.AWS_REGION = os.getenv("AWS_REGION")
        self.S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
        logger.info("Environment variables loaded successfully")

        if not all([self.AWS_ACCESS_KEY_ID, self.AWS_SECRET_ACCESS_KEY, self.AWS_REGION, self.S3_BUCKET_NAME]):
            logger.error("Missing required AWS credentials in environment variables")
        else:
            logger.debug("AWS credentials loaded successfully")



class SparkLoader:

    """
    This class is used to load the Spark session and the dataframes from the S3 bucket.

    Args:
        app_name: str (default: ' Bank Churn Prediction')

    Attributes:
        spark: SparkSession
        bucket_name: str

    Methods:
        load_dataframes: Load  CSVs from S3 into DataFrames and return them .
 
    """

    # Initialize the class
    def __init__(self, app_name=' Bank Churn Prediction'):

        logger.info(f"Initializing SparkLoader Class with app_name : {app_name} and creating Spark session")

        # Load the credentials from the .env file
        creds = LoadCreds()

        try:
            logger.debug("Creating Spark session...")
        # Create the Spark session
            self.spark = SparkSession.builder \
                .appName(app_name) \
                .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2") \
                .config("spark.hadoop.fs.s3a.access.key", creds.AWS_ACCESS_KEY_ID) \
                .config("spark.hadoop.fs.s3a.secret.key", creds.AWS_SECRET_ACCESS_KEY) \
                .config("spark.hadoop.fs.s3a.endpoint", f"s3.{creds.AWS_REGION}.amazonaws.com") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .getOrCreate()
            self.bucket_name = creds.S3_BUCKET_NAME
            logger.info("Spark Session created successfully")        
        except Exception as e:
            logger.error(f"Error creating Spark session: {str(e)}")
            raise e



        
    # Load the dataframes from the S3 bucket
    def load_dataframes(self):
       
        """
        Load Bank Data CSV from S3 into Pyspark DataFrame.

        Args:
            None

        Returns:
            bank_df: Pyspark DataFrame
        
        """
        try:
        # Load the flights data from the S3 bucket
            bank_path = f"s3a://{self.bucket_name}/bank_churn.csv"        
            logger.info(f"Loading bank data from {bank_path}")
           
            # Load the dataframes from the S3 bucket
            bank_df = self.spark.read.csv(bank_path, header=True, inferSchema=True)
            logger.info(f"Bank data loaded successfully")

            return bank_df
        
        except Exception as e:
            logger.error(f"Error loading dataframes: {str(e)}")
            raise e

if __name__ == "__main__":
    try:
        logger = setup_logging()
        logger.info("Starting data ingestion process")
        spark = SparkLoader()
        bank_df = spark.load_dataframes()
        logger.info("Data ingestion process completed")
    except Exception as e:
        logger.error(f"Main execution failed: {str(e)}")