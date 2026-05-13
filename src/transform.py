"""
Sales Order Processing - Transform Module
Applies business transformations including aggregations, validations, and mappings
"""
import logging
from typing import Dict, Any, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


class SalesOrderTransformer:
    """Handles transformation of sales order data"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize the transformer
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.transform_config = config.get('transform', {})
        
    def aggregate_order_amounts(self, orders_df: DataFrame, items_df: DataFrame) -> DataFrame:
        """
        Aggregate order amounts from line items
        
        Args:
            orders_df: Sales orders DataFrame
            items_df: Order items DataFrame
            
        Returns:
            DataFrame with aggregated amounts
        """
        try:
            logger.info("Aggregating order amounts")
            
            # Calculate line item totals
            items_with_totals = items_df.withColumn(
                "line_total",
                F.col("quantity") * F.col("unit_price")
            ).withColumn(
                "line_discount_amount",
                F.col("line_total") * F.col("discount_percent") / 100
            ).withColumn(
                "line_net_amount",
                F.col("line_total") - F.col("line_discount_amount")
            )
            
            # Aggregate by order
            order_aggregates = items_with_totals.groupBy("order_id").agg(
                F.sum("line_total").alias("total_amount"),
                F.sum("line_discount_amount").alias("total_discount"),
                F.sum("line_net_amount").alias("net_amount"),
                F.count("*").alias("line_item_count"),
                F.sum("quantity").alias("total_quantity")
            )
            
            # Join back to orders
            result = orders_df.join(
                order_aggregates,
                on="order_id",
                how="left"
            ).withColumn(
                "total_amount",
                F.coalesce(F.col("total_amount"), F.lit(0))
            ).withColumn(
                "total_discount",
                F.coalesce(F.col("total_discount"), F.lit(0))
            ).withColumn(
                "net_amount",
                F.coalesce(F.col("net_amount"), F.lit(0))
            ).withColumn(
                "line_item_count",
                F.coalesce(F.col("line_item_count"), F.lit(0))
            ).withColumn(
                "total_quantity",
                F.coalesce(F.col("total_quantity"), F.lit(0))
            )
            
            logger.info(f"Aggregated amounts for {result.count()} orders")
            return result
            
        except Exception as e:
            logger.error(f"Failed to aggregate order amounts: {str(e)}")
            raise
    
    def validate_order_status(self, df: DataFrame) -> DataFrame:
        """
        Validate and standardize order status values
        
        Args:
            df: Orders DataFrame
            
        Returns:
            DataFrame with validated status
        """
        try:
            logger.info("Validating order status")
            
            valid_statuses = self.transform_config.get('valid_statuses', [
                'PENDING', 'CONFIRMED', 'PROCESSING', 'SHIPPED', 
                'DELIVERED', 'CANCELLED', 'RETURNED'
            ])
            
            result = df.withColumn(
                "status_original",
                F.col("status")
            ).withColumn(
                "status",
                F.upper(F.trim(F.col("status")))
            ).withColumn(
                "status_valid",
                F.when(
                    F.col("status").isin(valid_statuses),
                    F.lit(True)
                ).otherwise(F.lit(False))
            ).withColumn(
                "status",
                F.when(
                    F.col("status_valid"),
                    F.col("status")
                ).otherwise(F.lit("UNKNOWN"))
            )
            
            invalid_count = result.filter(F.col("status") == "UNKNOWN").count()
            if invalid_count > 0:
                logger.warning(f"Found {invalid_count} orders with invalid status")
            
            logger.info("Order status validation completed")
            return result
            
        except Exception as e:
            logger.error(f"Failed to validate order status: {str(e)}")
            raise
    
    def apply_region_mapping(self, df: DataFrame, region_mapping_df: DataFrame) -> DataFrame:
        """
        Apply region mapping transformations based on customer location
        
        Args:
            df: Orders DataFrame with customer data
            region_mapping_df: Region mapping reference data
            
        Returns:
            DataFrame with region information
        """
        try:
            logger.info("Applying region mapping")
            
            # Join with region mapping
            result = df.join(
                region_mapping_df,
                on=["state", "country"],
                how="left"
            ).withColumn(
                "region_code",
                F.coalesce(F.col("region_code"), F.lit("UNKNOWN"))
            ).withColumn(
                "region_name",
                F.coalesce(F.col("region_name"), F.lit("Unknown Region"))
            ).withColumn(
                "sales_territory",
                F.coalesce(F.col("sales_territory"), F.lit("UNASSIGNED"))
            )
            
            unmapped_count = result.filter(F.col("region_code") == "UNKNOWN").count()
            if unmapped_count > 0:
                logger.warning(f"Found {unmapped_count} orders with unmapped regions")
            
            logger.info("Region mapping completed")
            return result
            
        except Exception as e:
            logger.error(f"Failed to apply region mapping: {str(e)}")
            raise
    
    def calculate_order_metrics(self, df: DataFrame) -> DataFrame:
        """
        Calculate additional order metrics and KPIs
        
        Args:
            df: Orders DataFrame
            
        Returns:
            DataFrame with calculated metrics
        """
        try:
            logger.info("Calculating order metrics")
            
            result = df.withColumn(
                "average_item_price",
                F.when(
                    F.col("line_item_count") > 0,
                    F.col("total_amount") / F.col("line_item_count")
                ).otherwise(F.lit(0))
            ).withColumn(
                "discount_percentage",
                F.when(
                    F.col("total_amount") > 0,
                    (F.col("total_discount") / F.col("total_amount")) * 100
                ).otherwise(F.lit(0))
            ).withColumn(
                "order_priority",
                F.when(F.col("net_amount") >= 1000, F.lit("HIGH"))
                .when(F.col("net_amount") >= 500, F.lit("MEDIUM"))
                .otherwise(F.lit("LOW"))
            ).withColumn(
                "processing_days",
                F.datediff(
                    F.coalesce(F.col("ship_date"), F.current_date()),
                    F.col("order_date")
                )
            ).withColumn(
                "is_rush_order",
                F.when(F.col("processing_days") <= 1, F.lit(True))
                .otherwise(F.lit(False))
            )
            
            logger.info("Order metrics calculation completed")
            return result
            
        except Exception as e:
            logger.error(f"Failed to calculate order metrics: {str(e)}")
            raise
    
    def enrich_with_customer_data(self, orders_df: DataFrame, customers_df: DataFrame) -> DataFrame:
        """
        Enrich orders with customer information
        
        Args:
            orders_df: Orders DataFrame
            customers_df: Customers DataFrame
            
        Returns:
            Enriched DataFrame
        """
        try:
            logger.info("Enriching orders with customer data")
            
            # Select relevant customer fields
            customer_fields = customers_df.select(
                "customer_id",
                "first_name",
                "last_name",
                F.concat_ws(" ", "first_name", "last_name").alias("customer_name"),
                "email",
                "phone",
                "city",
                "state",
                "country",
                "registration_date",
                "status"
            ).withColumnRenamed("status", "customer_status")
            
            # Join with orders
            result = orders_df.join(
                customer_fields,
                on="customer_id",
                how="left"
            ).withColumn(
                "customer_tenure_days",
                F.datediff(F.current_date(), F.col("registration_date"))
            ).withColumn(
                "is_new_customer",
                F.when(F.col("customer_tenure_days") <= 90, F.lit(True))
                .otherwise(F.lit(False))
            )
            
            logger.info("Customer data enrichment completed")
            return result
            
        except Exception as e:
            logger.error(f"Failed to enrich with customer data: {str(e)}")
            raise
    
    def apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules and data quality checks
        
        Args:
            df: Orders DataFrame
            
        Returns:
            DataFrame with business rules applied
        """
        try:
            logger.info("Applying business rules")
            
            min_order_amount = self.transform_config.get('min_order_amount', 0)
            max_discount_percent = self.transform_config.get('max_discount_percent', 50)
            
            result = df.withColumn(
                "is_valid_order",
                F.when(
                    (F.col("net_amount") >= min_order_amount) &
                    (F.col("discount_percentage") <= max_discount_percent) &
                    (F.col("status_valid") == True) &
                    (F.col("customer_id").isNotNull()),
                    F.lit(True)
                ).otherwise(F.lit(False))
            ).withColumn(
                "validation_errors",
                F.array_remove(
                    F.array(
                        F.when(F.col("net_amount") < min_order_amount, 
                               F.lit("AMOUNT_TOO_LOW")).otherwise(F.lit(None)),
                        F.when(F.col("discount_percentage") > max_discount_percent,
                               F.lit("DISCOUNT_TOO_HIGH")).otherwise(F.lit(None)),
                        F.when(F.col("status_valid") == False,
                               F.lit("INVALID_STATUS")).otherwise(F.lit(None)),
                        F.when(F.col("customer_id").isNull(),
                               F.lit("MISSING_CUSTOMER")).otherwise(F.lit(None))
                    ),
                    None
                )
            ).withColumn(
                "requires_review",
                F.when(
                    (F.col("is_valid_order") == False) |
                    (F.col("discount_percentage") > 30) |
                    (F.col("net_amount") > 10000),
                    F.lit(True)
                ).otherwise(F.lit(False))
            )
            
            invalid_count = result.filter(F.col("is_valid_order") == False).count()
            review_count = result.filter(F.col("requires_review") == True).count()
            
            logger.info(f"Business rules applied - Invalid: {invalid_count}, Review: {review_count}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to apply business rules: {str(e)}")
            raise
    
    def add_audit_columns(self, df: DataFrame) -> DataFrame:
        """
        Add audit and metadata columns
        
        Args:
            df: DataFrame to augment
            
        Returns:
            DataFrame with audit columns
        """
        try:
            logger.info("Adding audit columns")
            
            result = df.withColumn(
                "processed_timestamp",
                F.current_timestamp()
            ).withColumn(
                "processed_date",
                F.current_date()
            ).withColumn(
                "source_system",
                F.lit(self.transform_config.get('source_system', 'INFORMATICA'))
            ).withColumn(
                "batch_id",
                F.lit(self.transform_config.get('batch_id', 'UNKNOWN'))
            ).withColumn(
                "record_hash",
                F.sha2(F.concat_ws("|", *df.columns), 256)
            )
            
            logger.info("Audit columns added")
            return result
            
        except Exception as e:
            logger.error(f"Failed to add audit columns: {str(e)}")
            raise