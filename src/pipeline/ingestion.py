import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError
import yaml
from src.logging import Log

# Set up logging at the module level
logger = Log.setup_logging()

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
        
        logger.info("Initializing LoadCreds Class and loading environment variables",stacklevel=2)
        load_dotenv()
        
        self.AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
        self.AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.AWS_REGION = os.getenv("AWS_REGION")
        self.S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
        
        logger.info("Environment variables loaded successfully",stacklevel=2)

        if not all([self.AWS_ACCESS_KEY_ID, self.AWS_SECRET_ACCESS_KEY, self.AWS_REGION, self.S3_BUCKET_NAME]):
            logger.error("Missing required AWS credentials in environment variables",stacklevel=2)
        
        else:
            logger.debug("AWS credentials loaded successfully",stacklevel=2)



class SparkLoader:

    """
    This class is used to load the Spark session .

    Args:
        app_name: str (default: ' Bank Churn Prediction')

    Attributes:
        spark: SparkSession
        bucket_name: str


    """

    # Initialize the class
    def __init__(self, app_name=' Bank Churn Prediction'):

        logger.info(f"Initializing SparkLoader Class with app_name : {app_name} and creating Spark session",stacklevel=2)

        # Load the credentials from the .env file
        self.creds = LoadCreds()
        
        # Initialize AWS S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.creds.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=self.creds.AWS_SECRET_ACCESS_KEY,
            region_name=self.creds.AWS_REGION
        )

        try:
            logger.debug("Creating Spark session...",stacklevel=2)
        
            # Create the Spark session
            self.spark = SparkSession.builder \
                .appName(app_name) \
                .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2") \
                .config("spark.hadoop.fs.s3a.access.key", self.creds.AWS_ACCESS_KEY_ID) \
                .config("spark.hadoop.fs.s3a.secret.key", self.creds.AWS_SECRET_ACCESS_KEY) \
                .config("spark.hadoop.fs.s3a.endpoint", f"s3.{self.creds.AWS_REGION}.amazonaws.com") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .getOrCreate()
            
            self.bucket_name = self.creds.S3_BUCKET_NAME
            
            logger.info("Spark Session created successfully",stacklevel=2)        
        
        except Exception as e:
            logger.error(f"Error creating Spark session: {str(e)}",stacklevel=2)
            raise e


class LoadData:

    def __init__(self):

        self.creds = LoadCreds()
        self.bucket_name = self.creds.S3_BUCKET_NAME
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.creds.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=self.creds.AWS_SECRET_ACCESS_KEY,
            region_name=self.creds.AWS_REGION
        )

    def load_data_from_s3_and_save_locally(self, path_to_save: str):
        """
        Load data from S3 and save locally.

        Args:
            path_to_save (str): Local path where data should be saved
        """
        try:
            # Create directory if it doesn't exist
            os.makedirs(path_to_save, exist_ok=True)
            logger.info(f"Created directory {path_to_save} if it didn't exist")

            # Define local file path
            local_file_path = os.path.join(path_to_save, "Bank_churn.csv")
            
            # Download file from S3
            logger.info(f"Downloading data from S3 bucket {self.bucket_name}")
            try:
                self.s3_client.download_file(
                    self.bucket_name,
                    "Bank_churn.csv",
                    local_file_path
                )
                logger.info(f"Bank data saved successfully to {local_file_path}")
            
            except ClientError as e:
                logger.error(f"Error downloading from S3: {str(e)}")
                raise
            
        except Exception as e:
            logger.error(f"Error saving bank data: {str(e)}")
            raise e


    

class LoadYamlParams:

    """
    This class is used to load the parameters from the YAML file.
    """


    def load_params(self,params_path: str = 'params.yaml') -> dict:
        """
        Load parameters from YAML file.

        Args:
            params_path (str): Path to the parameters YAML file

        Returns:
            dict: Loaded parameters
        """
        try:
            with open(params_path, 'r') as file:
                params = yaml.safe_load(file)
            logger.info(f"Parameters loaded successfully from {params_path}")
            return params
        
        except Exception as e:
            logger.error(f"Error loading parameters: {str(e)}")
            raise




def main():
    """Main function to run the data ingestion process."""
    
    try:
        logger = Log.setup_logging()
        logger.info("Starting data ingestion process")
        
        # Load parameters
        params_obj = LoadYamlParams()
        params = params_obj.load_params()
        raw_data_path = params['ingestion']['raw_data_path']
        
        # Initialize SparkLoader
        spark_loader = LoadData()
        
        # Step 1: Save data locally from S3
        spark_loader.load_data_from_s3_and_save_locally(raw_data_path)
        logger.info("Data saved locally successfully")


    except Exception as e:
        logger.error(f"Main execution failed: {str(e)}")

if __name__ == "__main__":
    main()
