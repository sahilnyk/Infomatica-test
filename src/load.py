"""
Sales Order Processing - Load Module
Loads transformed data to target destinations
"""
import logging
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException

logger = logging.getLogger(__name__)


class SalesOrderLoader:
    """Handles loading of transformed sales order data"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize the loader
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.target_config = config.get('target', {})
        
    def load_processed_orders(self, df: DataFrame) -> bool:
        """
        Load processed orders to target
        
        Args:
            df: Processed orders DataFrame
            
        Returns:
            True if successful, False otherwise
        """
        try:
            target_path = self.target_config.get('processed_orders_path')
            target_format = self.target_config.get('format', 'delta')
            write_mode = self.target_config.get('write_mode', 'append')
            partition_cols = self.target_config.get('partition_columns', ['processed_date'])
            
            logger.info(f"Loading {df.count()} processed orders to {target_path}")
            
            writer = df.write.format(target_format).mode(write_mode)
            
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            
            if target_format == 'delta':
                writer = writer.option("mergeSchema", "true")
                writer = writer.option("overwriteSchema", "false")
            
            writer.save(target_path)
            
            logger.info("Processed orders loaded successfully")
            return True
            
        except AnalysisException as e:
            logger.error(f"Failed to load processed orders: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during load: {str(e)}")
            return False
    
    def load_invalid_orders(self, df: DataFrame) -> bool:
        """
        Load invalid orders to error table
        
        Args:
            df: Invalid orders DataFrame
            
        Returns:
            True if successful, False otherwise
        """
        try:
            invalid_df = df.filter(df.is_valid_order == False)
            
            if invalid_df.count() == 0:
                logger.info("No invalid orders to load")
                return True
            
            target_path = self.target_config.get('invalid_orders_path')
            target_format = self.target_config.get('format', 'delta')
            
            logger.info(f"Loading {invalid_df.count()} invalid orders to {target_path}")
            
            invalid_df.write.format(target_format).mode('append').save(target_path)
            
            logger.info("Invalid orders loaded successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load invalid orders: {str(e)}")
            return False
    
    def load_order_metrics(self, df: DataFrame) -> bool:
        """
        Load aggregated order metrics to analytics table
        
        Args:
            df: Orders DataFrame with metrics
            
        Returns:
            True if successful, False otherwise
        """
        try:
            from pyspark.sql import functions as F
            
            # Aggregate metrics by date and region
            metrics_df = df.groupBy(
                "processed_date",
                "region_code",
                "region_name",
                "status"
            ).agg(
                F.count("*").alias("order_count"),
                F.sum("net_amount").alias("total_revenue"),
                F.avg("net_amount").alias("avg_order_value"),
                F.sum("total_discount").alias("total_discounts"),
                F.avg("discount_percentage").alias("avg_discount_pct"),
                F.sum("line_item_count").alias("total_items"),
                F.countDistinct("customer_id").alias("unique_customers"),
                F.sum(F.when(F.col("is_new_customer"), 1).otherwise(0)).alias("new_customer_orders"),
                F.sum(F.when(F.col("is_rush_order"), 1).otherwise(0)).alias("rush_orders"),
                F.sum(F.when(F.col("requires_review"), 1).otherwise(0)).alias("orders_requiring_review")
            )
            
            target_path = self.target_config.get('metrics_path')
            target_format = self.target_config.get('format', 'delta')
            
            logger.info(f"Loading order metrics to {target_path}")
            
            metrics_df.write.format(target_format).mode('append').save(target_path)
            
            logger.info("Order metrics loaded successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load order metrics: {str(e)}")
            return False
    
    def create_or_update_table(self, df: DataFrame, table_name: str, 
                               database: Optional[str] = None) -> bool:
        """
        Create or update a Delta table
        
        Args:
            df: DataFrame to save
            table_name: Name of the table
            database: Optional database name
            
        Returns:
            True if successful, False otherwise
        """
        try:
            full_table_name = f"{database}.{table_name}" if database else table_name
            
            logger.info(f"Creating/updating table {full_table_name}")
            
            df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)
            
            logger.info(f"Table {full_table_name} created/updated successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create/update table {full_table_name}: {str(e)}")
            return False
    
    def optimize_target_tables(self) -> bool:
        """
        Optimize target Delta tables
        
        Returns:
            True if successful, False otherwise
        """
        try:
            tables_to_optimize = self.target_config.get('tables_to_optimize', [])
            
            for table_info in tables_to_optimize:
                table_name = table_info.get('name')
                z_order_cols = table_info.get('z_order_columns', [])
                
                logger.info(f"Optimizing table {table_name}")
                
                self.spark.sql(f"OPTIMIZE {table_name}")
                
                if z_order_cols:
                    z_order_clause = ", ".join(z_order_cols)
                    self.spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({z_order_clause})")
                    logger.info(f"Applied Z-ORDER on {z_order_clause}")
                
                logger.info(f"Table {table_name} optimized successfully")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to optimize tables: {str(e)}")
            return False
    
    def generate_load_summary(self, df: DataFrame) -> Dict[str, Any]:
        """
        Generate summary statistics for loaded data
        
        Args:
            df: Loaded DataFrame
            
        Returns:
            Dictionary with summary statistics
        """
        try:
            from pyspark.sql import functions as F
            
            summary = df.agg(
                F.count("*").alias("total_records"),
                F.sum(F.when(F.col("is_valid_order"), 1).otherwise(0)).alias("valid_orders"),
                F.sum(F.when(~F.col("is_valid_order"), 1).otherwise(0)).alias("invalid_orders"),
                F.sum("net_amount").alias("total_revenue"),
                F.avg("net_amount").alias("avg_order_value"),
                F.min("order_date").alias("earliest_order"),
                F.max("order_date").alias("latest_order"),
                F.countDistinct("customer_id").alias("unique_customers"),
                F.countDistinct("region_code").alias("unique_regions")
            ).collect()[0].asDict()
            
            logger.info(f"Load summary: {summary}")
            return summary
            
        except Exception as e:
            logger.error(f"Failed to generate load summary: {str(e)}")
            return {}