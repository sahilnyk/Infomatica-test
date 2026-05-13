"""
Sales Order Processing - Extract Module
Extracts sales order data from source systems
"""
import logging
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException

logger = logging.getLogger(__name__)


class SalesOrderExtractor:
    """Handles extraction of sales order data from various sources"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize the extractor
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.source_config = config.get('source', {})
        
    def extract_sales_orders(self) -> DataFrame:
        """
        Extract sales orders from source
        
        Returns:
            DataFrame containing sales order data
        """
        try:
            source_path = self.source_config.get('sales_orders_path')
            source_format = self.source_config.get('format', 'parquet')
            
            logger.info(f"Extracting sales orders from {source_path}")
            
            df = self.spark.read.format(source_format).load(source_path)
            
            logger.info(f"Extracted {df.count()} sales order records")
            return df
            
        except AnalysisException as e:
            logger.error(f"Failed to extract sales orders: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during extraction: {str(e)}")
            raise
    
    def extract_order_items(self) -> DataFrame:
        """
        Extract order line items from source
        
        Returns:
            DataFrame containing order items
        """
        try:
            source_path = self.source_config.get('order_items_path')
            source_format = self.source_config.get('format', 'parquet')
            
            logger.info(f"Extracting order items from {source_path}")
            
            df = self.spark.read.format(source_format).load(source_path)
            
            logger.info(f"Extracted {df.count()} order item records")
            return df
            
        except AnalysisException as e:
            logger.error(f"Failed to extract order items: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during extraction: {str(e)}")
            raise
    
    def extract_customers(self) -> DataFrame:
        """
        Extract customer data from source
        
        Returns:
            DataFrame containing customer data
        """
        try:
            source_path = self.source_config.get('customers_path')
            source_format = self.source_config.get('format', 'parquet')
            
            logger.info(f"Extracting customers from {source_path}")
            
            df = self.spark.read.format(source_format).load(source_path)
            
            logger.info(f"Extracted {df.count()} customer records")
            return df
            
        except AnalysisException as e:
            logger.error(f"Failed to extract customers: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during extraction: {str(e)}")
            raise
    
    def extract_region_mapping(self) -> DataFrame:
        """
        Extract region mapping reference data
        
        Returns:
            DataFrame containing region mappings
        """
        try:
            source_path = self.source_config.get('region_mapping_path')
            source_format = self.source_config.get('format', 'parquet')
            
            logger.info(f"Extracting region mapping from {source_path}")
            
            df = self.spark.read.format(source_format).load(source_path)
            
            logger.info(f"Extracted {df.count()} region mapping records")
            return df
            
        except AnalysisException as e:
            logger.error(f"Failed to extract region mapping: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during extraction: {str(e)}")
            raise
    
    def validate_extracted_data(self, df: DataFrame, dataset_name: str) -> bool:
        """
        Validate extracted data meets basic quality checks
        
        Args:
            df: DataFrame to validate
            dataset_name: Name of the dataset for logging
            
        Returns:
            True if validation passes, False otherwise
        """
        try:
            if df is None:
                logger.error(f"{dataset_name}: DataFrame is None")
                return False
            
            row_count = df.count()
            if row_count == 0:
                logger.warning(f"{dataset_name}: No records extracted")
                return False
            
            null_counts = df.select([
                sum(col(c).isNull().cast("int")).alias(c) 
                for c in df.columns
            ]).collect()[0].asDict()
            
            for col_name, null_count in null_counts.items():
                if null_count > 0:
                    logger.info(f"{dataset_name}.{col_name}: {null_count} null values")
            
            logger.info(f"{dataset_name}: Validation passed - {row_count} records")
            return True
            
        except Exception as e:
            logger.error(f"Validation failed for {dataset_name}: {str(e)}")
            return False