import logging
import os


class Log:
    def __init__(self):
        pass

    def setup_logging(log_dir="logging-info"):
        """
        Set up logging configuration and create log directory if it doesn't exist.
        
        Args:
            log_dir (str): Directory name for storing log files. Defaults to 'logging-info'
        
        Returns:
            logging.Logger: Configured logger instance
        """
        # Create log directory if it doesn't exist
        os.makedirs(log_dir, exist_ok=True)
        
        # Configure logger
        logger = logging.getLogger(__name__)
        logging.basicConfig(
            filename=os.path.join(log_dir, "logs.log"),
            encoding="utf-8",
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s"
        )
        
        return logger

if __name__ == "__main__":
    logger = Log.setup_logging()
    logger.info("Logging setup completed successfully")




