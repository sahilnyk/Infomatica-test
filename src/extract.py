"""
Extract module for Sales Line Item Processing
Reads sales line items from source systems
"""
import logging
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType, TimestampType
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SalesLineItemExtractor:
    """Extracts sales line item data from source systems"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize extractor with Spark session and configuration
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.source_config = config.get('source', {})
        
    def get_line_item_schema(self) -> StructType:
        """
        Define schema for sales line items
        
        Returns:
            StructType: Schema definition
        """
        return StructType([
            StructField("line_item_id", StringType(), nullable=False),
            StructField("order_id", StringType(), nullable=False),
            StructField("product_id", StringType(), nullable=False),
            StructField("quantity", IntegerType(), nullable=False),
            StructField("unit_price", DecimalType(18, 2), nullable=False),
            StructField("discount_percentage", DecimalType(5, 2), nullable=True),
            StructField("tax_rate", DecimalType(5, 2), nullable=True),
            StructField("line_status", StringType(), nullable=True),
            StructField("created_date", TimestampType(), nullable=True),
            StructField("modified_date", TimestampType(), nullable=True),
            StructField("source_system", StringType(), nullable=True)
        ])
    
    def extract_from_file(self, file_path: Optional[str] = None) -> DataFrame:
        """
        Extract line items from flat file source
        
        Args:
            file_path: Optional override for file path
            
        Returns:
            DataFrame: Extracted line items
        """
        path = file_path or self.source_config.get('file_path')
        file_format = self.source_config.get('file_format', 'csv')
        
        logger.info(f"Extracting line items from {file_format} file: {path}")
        
        try:
            if file_format.lower() == 'csv':
                df = self.spark.read.csv(
                    path,
                    header=self.source_config.get('header', True),
                    schema=self.get_line_item_schema(),
                    sep=self.source_config.get('delimiter', ','),
                    quote=self.source_config.get('quote_char', '"'),
                    escape=self.source_config.get('escape_char', '\\'),
                    nullValue=self.source_config.get('null_value', ''),
                    dateFormat=self.source_config.get('date_format', 'yyyy-MM-dd'),
                    timestampFormat=self.source_config.get('timestamp_format', 'yyyy-MM-dd HH:mm:ss')
                )
            elif file_format.lower() == 'parquet':
                df = self.spark.read.parquet(path)
            elif file_format.lower() == 'json':
                df = self.spark.read.json(path, schema=self.get_line_item_schema())
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
            
            record_count = df.count()
            logger.info(f"Successfully extracted {record_count} line items")
            
            return df
            
        except Exception as e:
            logger.error(f"Error extracting line items from file: {str(e)}")
            raise
    
    def extract_from_jdbc(self, table_name: Optional[str] = None) -> DataFrame:
        """
        Extract line items from JDBC source
        
        Args:
            table_name: Optional override for table name
            
        Returns:
            DataFrame: Extracted line items
        """
        jdbc_config = self.source_config.get('jdbc', {})
        table = table_name or jdbc_config.get('table_name')
        
        logger.info(f"Extracting line items from JDBC table: {table}")
        
        try:
            df = self.spark.read.jdbc(
                url=jdbc_config.get('url'),
                table=table,
                properties={
                    'user': jdbc_config.get('user'),
                    'password': jdbc_config.get('password'),
                    'driver': jdbc_config.get('driver'),
                    'fetchsize': str(jdbc_config.get('fetch_size', 10000))
                }
            )
            
            record_count = df.count()
            logger.info(f"Successfully extracted {record_count} line items from JDBC")
            
            return df
            
        except Exception as e:
            logger.error(f"Error extracting line items from JDBC: {str(e)}")
            raise
    
    def extract_from_delta(self, table_path: Optional[str] = None) -> DataFrame:
        """
        Extract line items from Delta Lake table
        
        Args:
            table_path: Optional override for Delta table path
            
        Returns:
            DataFrame: Extracted line items
        """
        path = table_path or self.source_config.get('delta_path')
        
        logger.info(f"Extracting line items from Delta table: {path}")
        
        try:
            df = self.spark.read.format("delta").load(path)
            
            record_count = df.count()
            logger.info(f"Successfully extracted {record_count} line items from Delta")
            
            return df
            
        except Exception as e:
            logger.error(f"Error extracting line items from Delta: {str(e)}")
            raise
    
    def extract(self) -> DataFrame:
        """
        Main extraction method - routes to appropriate extractor
        
        Returns:
            DataFrame: Extracted line items
        """
        source_type = self.source_config.get('type', 'file')
        
        logger.info(f"Starting extraction with source type: {source_type}")
        
        if source_type == 'file':
            return self.extract_from_file()
        elif source_type == 'jdbc':
            return self.extract_from_jdbc()
        elif source_type == 'delta':
            return self.extract_from_delta()
        else:
            raise ValueError(f"Unsupported source type: {source_type}")


def load_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    """
    Load configuration from YAML file
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        Dict: Configuration dictionary
    """
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        logger.info(f"Configuration loaded from {config_path}")
        return config
    except Exception as e:
        logger.error(f"Error loading configuration: {str(e)}")
        raise


def main():
    """Main execution function"""
    config = load_config()
    
    spark = SparkSession.builder \
        .appName("SalesLineItemExtract") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    try:
        extractor = SalesLineItemExtractor(spark, config)
        df = extractor.extract()
        
        df.show(10, truncate=False)
        df.printSchema()
        
        logger.info("Extraction completed successfully")
        
    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()