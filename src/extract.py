"""
Sales Returns Processing - Extract Module
Extracts sales returns data from source systems including orders, returns, and customer data.
"""

import logging
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SalesReturnsExtractor:
    """Handles extraction of sales returns data from various sources."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize the extractor.
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.source_config = config.get('sources', {})
        
    def extract_customers(self) -> Optional[DataFrame]:
        """
        Extract customer master data.
        
        Returns:
            DataFrame containing customer data or None on failure
        """
        try:
            customer_config = self.source_config.get('customers', {})
            source_path = customer_config.get('path')
            source_format = customer_config.get('format', 'csv')
            
            logger.info(f"Extracting customers from {source_path}")
            
            df = self.spark.read \
                .format(source_format) \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(source_path)
            
            logger.info(f"Extracted {df.count()} customer records")
            return df
            
        except AnalysisException as e:
            logger.error(f"Failed to extract customers: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error extracting customers: {str(e)}")
            return None
    
    def extract_orders(self) -> Optional[DataFrame]:
        """
        Extract order data including order details.
        
        Returns:
            DataFrame containing order data or None on failure
        """
        try:
            order_config = self.source_config.get('orders', {})
            source_path = order_config.get('path')
            source_format = order_config.get('format', 'csv')
            
            logger.info(f"Extracting orders from {source_path}")
            
            df = self.spark.read \
                .format(source_format) \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(source_path)
            
            logger.info(f"Extracted {df.count()} order records")
            return df
            
        except AnalysisException as e:
            logger.error(f"Failed to extract orders: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error extracting orders: {str(e)}")
            return None
    
    def extract_returns(self) -> Optional[DataFrame]:
        """
        Extract sales returns data.
        
        Returns:
            DataFrame containing returns data or None on failure
        """
        try:
            returns_config = self.source_config.get('returns', {})
            source_path = returns_config.get('path')
            source_format = returns_config.get('format', 'csv')
            
            logger.info(f"Extracting returns from {source_path}")
            
            df = self.spark.read \
                .format(source_format) \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(source_path)
            
            logger.info(f"Extracted {df.count()} return records")
            return df
            
        except AnalysisException as e:
            logger.error(f"Failed to extract returns: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error extracting returns: {str(e)}")
            return None
    
    def extract_return_items(self) -> Optional[DataFrame]:
        """
        Extract return line items data.
        
        Returns:
            DataFrame containing return items or None on failure
        """
        try:
            items_config = self.source_config.get('return_items', {})
            source_path = items_config.get('path')
            source_format = items_config.get('format', 'csv')
            
            logger.info(f"Extracting return items from {source_path}")
            
            df = self.spark.read \
                .format(source_format) \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(source_path)
            
            logger.info(f"Extracted {df.count()} return item records")
            return df
            
        except AnalysisException as e:
            logger.error(f"Failed to extract return items: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error extracting return items: {str(e)}")
            return None
    
    def extract_products(self) -> Optional[DataFrame]:
        """
        Extract product master data for price lookups.
        
        Returns:
            DataFrame containing product data or None on failure
        """
        try:
            product_config = self.source_config.get('products', {})
            source_path = product_config.get('path')
            source_format = product_config.get('format', 'csv')
            
            logger.info(f"Extracting products from {source_path}")
            
            df = self.spark.read \
                .format(source_format) \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(source_path)
            
            logger.info(f"Extracted {df.count()} product records")
            return df
            
        except AnalysisException as e:
            logger.error(f"Failed to extract products: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error extracting products: {str(e)}")
            return None
    
    def extract_all(self) -> Dict[str, Optional[DataFrame]]:
        """
        Extract all required datasets for returns processing.
        
        Returns:
            Dictionary mapping dataset names to DataFrames
        """
        logger.info("Starting extraction of all datasets")
        
        datasets = {
            'customers': self.extract_customers(),
            'orders': self.extract_orders(),
            'returns': self.extract_returns(),
            'return_items': self.extract_return_items(),
            'products': self.extract_products()
        }
        
        successful = sum(1 for df in datasets.values() if df is not None)
        logger.info(f"Extraction complete: {successful}/{len(datasets)} datasets successful")
        
        return datasets


def load_config(config_path: str = 'config.yaml') -> Dict[str, Any]:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        Configuration dictionary
    """
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        logger.info(f"Configuration loaded from {config_path}")
        return config
    except Exception as e:
        logger.error(f"Failed to load configuration: {str(e)}")
        raise


if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("SalesReturnsExtract") \
        .getOrCreate()
    
    try:
        # Load configuration
        config = load_config()
        
        # Initialize extractor
        extractor = SalesReturnsExtractor(spark, config)
        
        # Extract all datasets
        datasets = extractor.extract_all()
        
        # Validate extraction
        failed_datasets = [name for name, df in datasets.items() if df is None]
        if failed_datasets:
            logger.error(f"Failed to extract: {', '.join(failed_datasets)}")
        else:
            logger.info("All datasets extracted successfully")
            
    except Exception as e:
        logger.error(f"Extraction job failed: {str(e)}")
        raise
    finally:
        spark.stop()