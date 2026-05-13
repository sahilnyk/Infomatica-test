"""
Transform module for Sales Line Item Processing
Implements line total calculation, discount application, and quantity validation
"""
import logging
from typing import Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from decimal import Decimal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SalesLineItemTransformer:
    """Transforms sales line item data with business logic"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize transformer with configuration
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.transform_config = config.get('transform', {})
        self.validation_config = self.transform_config.get('validation', {})
        self.calculation_config = self.transform_config.get('calculation', {})
        
    def validate_quantity(self, df: DataFrame) -> DataFrame:
        """
        Validate quantity field according to business rules
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame: DataFrame with validation flags
        """
        min_quantity = self.validation_config.get('min_quantity', 1)
        max_quantity = self.validation_config.get('max_quantity', 9999)
        
        logger.info(f"Validating quantity (min: {min_quantity}, max: {max_quantity})")
        
        df = df.withColumn(
            "quantity_valid",
            F.when(
                (F.col("quantity").isNull()) |
                (F.col("quantity") < min_quantity) |
                (F.col("quantity") > max_quantity),
                F.lit(False)
            ).otherwise(F.lit(True))
        )
        
        df = df.withColumn(
            "quantity_validation_message",
            F.when(
                F.col("quantity").isNull(),
                F.lit("Quantity is null")
            ).when(
                F.col("quantity") < min_quantity,
                F.lit(f"Quantity below minimum ({min_quantity})")
            ).when(
                F.col("quantity") > max_quantity,
                F.lit(f"Quantity exceeds maximum ({max_quantity})")
            ).otherwise(F.lit(None))
        )
        
        invalid_count = df.filter(F.col("quantity_valid") == False).count()
        logger.info(f"Found {invalid_count} records with invalid quantity")
        
        return df
    
    def validate_unit_price(self, df: DataFrame) -> DataFrame:
        """
        Validate unit price field
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame: DataFrame with validation flags
        """
        min_price = Decimal(str(self.validation_config.get('min_unit_price', 0.01)))
        max_price = Decimal(str(self.validation_config.get('max_unit_price', 999999.99)))
        
        logger.info(f"Validating unit price (min: {min_price}, max: {max_price})")
        
        df = df.withColumn(
            "unit_price_valid",
            F.when(
                (F.col("unit_price").isNull()) |
                (F.col("unit_price") < F.lit(min_price)) |
                (F.col("unit_price") > F.lit(max_price)),
                F.lit(False)
            ).otherwise(F.lit(True))
        )
        
        df = df.withColumn(
            "unit_price_validation_message",
            F.when(
                F.col("unit_price").isNull(),
                F.lit("Unit price is null")
            ).when(
                F.col("unit_price") < F.lit(min_price),
                F.lit(f"Unit price below minimum ({min_price})")
            ).when(
                F.col("unit_price") > F.lit(max_price),
                F.lit(f"Unit price exceeds maximum ({max_price})")
            ).otherwise(F.lit(None))
        )
        
        invalid_count = df.filter(F.col("unit_price_valid") == False).count()
        logger.info(f"Found {invalid_count} records with invalid unit price")
        
        return df
    
    def validate_discount_percentage(self, df: DataFrame) -> DataFrame:
        """
        Validate discount percentage field
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame: DataFrame with validation flags
        """
        min_discount = Decimal(str(self.validation_config.get('min_discount_percentage', 0)))
        max_discount = Decimal(str(self.validation_config.get('max_discount_percentage', 100)))
        
        logger.info(f"Validating discount percentage (min: {min_discount}, max: {max_discount})")
        
        df = df.withColumn(
            "discount_percentage",
            F.when(F.col("discount_percentage").isNull(), F.lit(Decimal('0.00')))
            .otherwise(F.col("discount_percentage"))
        )
        
        df = df.withColumn(
            "discount_valid",
            F.when(
                (F.col("discount_percentage") < F.lit(min_discount)) |
                (F.col("discount_percentage") > F.lit(max_discount)),
                F.lit(False)
            ).otherwise(F.lit(True))
        )
        
        df = df.withColumn(
            "discount_validation_message",
            F.when(
                F.col("discount_percentage") < F.lit(min_discount),
                F.lit(f"Discount below minimum ({min_discount})")
            ).when(
                F.col("discount_percentage") > F.lit(max_discount),
                F.lit(f"Discount exceeds maximum ({max_discount})")
            ).otherwise(F.lit(None))
        )
        
        invalid_count = df.filter(F.col("discount_valid") == False).count()
        logger.info(f"Found {invalid_count} records with invalid discount percentage")
        
        return df
    
    def calculate_line_subtotal(self, df: DataFrame) -> DataFrame:
        """
        Calculate line subtotal (quantity * unit_price)
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame: DataFrame with line_subtotal column
        """
        logger.info("Calculating line subtotal")
        
        df = df.withColumn(
            "line_subtotal",
            (F.col("quantity") * F.col("unit_price")).cast(DecimalType(18, 2))
        )
        
        return df
    
    def apply_discount(self, df: DataFrame) -> DataFrame:
        """
        Apply discount percentage to calculate discount amount and net amount
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame: DataFrame with discount calculations
        """
        logger.info("Applying discount percentage")
        
        df = df.withColumn(
            "discount_amount",
            (F.col("line_subtotal") * F.col("discount_percentage") / F.lit(100))
            .cast(DecimalType(18, 2))
        )
        
        df = df.withColumn(
            "line_net_amount",
            (F.col("line_subtotal") - F.col("discount_amount"))
            .cast(DecimalType(18, 2))
        )
        
        return df
    
    def calculate_tax(self, df: DataFrame) -> DataFrame:
        """
        Calculate tax amount based on tax rate
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame: DataFrame with tax calculations
        """
        logger.info("Calculating tax amount")
        
        default_tax_rate = Decimal(str(self.calculation_config.get('default_tax_rate', 0)))
        
        df = df.withColumn(
            "tax_rate",
            F.when(F.col("tax_rate").isNull(), F.lit(default_tax_rate))
            .otherwise(F.col("tax_rate"))
        )
        
        df = df.withColumn(
            "tax_amount",
            (F.col("line_net_amount") * F.col("tax_rate") / F.lit(100))
            .cast(DecimalType(18, 2))
        )
        
        return df
    
    def calculate_line_total(self, df: DataFrame) -> DataFrame:
        """
        Calculate final line total (net amount + tax)
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame: DataFrame with line_total column
        """
        logger.info("Calculating line total")
        
        df = df.withColumn(
            "line_total",
            (F.col("line_net_amount") + F.col("tax_amount"))
            .cast(DecimalType(18, 2))
        )
        
        return df
    
    def add_validation_summary(self, df: DataFrame) -> DataFrame:
        """
        Add overall validation status column
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame: DataFrame with validation summary
        """
        logger.info("Adding validation summary")
        
        df = df.withColumn(
            "is_valid",
            F.col("quantity_valid") & 
            F.col("unit_price_valid") & 
            F.col("discount_valid")
        )
        
        df = df.withColumn(
            "validation_messages",
            F.concat_ws(
                "; ",
                F.col("quantity_validation_message"),
                F.col("unit_price_validation_message"),
                F.col("discount_validation_message")
            )
        )
        
        df = df.withColumn(
            "validation_messages",
            F.when(F.col("validation_messages") == "", F.lit(None))
            .otherwise(F.col("validation_messages"))
        )
        
        valid_count = df.filter(F.col("is_valid") == True).count()
        invalid_count = df.filter(F.col("is_valid") == False).count()
        
        logger.info(f"Validation summary - Valid: {valid_count}, Invalid: {invalid_count}")
        
        return df
    
    def add_audit_columns(self, df: DataFrame) -> DataFrame:
        """
        Add audit columns for tracking
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame: DataFrame with audit columns
        """
        logger.info("Adding audit columns")
        
        df = df.withColumn("processed_timestamp", F.current_timestamp())
        df = df.withColumn("processing_date", F.current_date())
        df = df.withColumn("etl_batch_id", F.lit(self.config.get('batch_id', 'BATCH_001')))
        
        return df
    
    def handle_invalid_records(self, df: DataFrame) -> tuple:
        """
        Separate valid and invalid records
        
        Args:
            df: Input DataFrame
            
        Returns:
            tuple: (valid_df, invalid_df)
        """
        logger.info("Separating valid and invalid records")
        
        valid_df = df.filter(F.col("is_valid") == True)
        invalid_df = df.filter(F.col("is_valid") == False)
        
        logger.info(f"Valid records: {valid_df.count()}")
        logger.info(f"Invalid records: {invalid_df.count()}")
        
        return valid_df, invalid_df
    
    def transform(self, df: DataFrame) -> tuple:
        """
        Main transformation pipeline
        
        Args:
            df: Input DataFrame
            
        Returns:
            tuple: (valid_df, invalid_df)
        """
        logger.info("Starting transformation pipeline")
        
        initial_count = df.count()
        logger.info(f"Initial record count: {initial_count}")
        
        # Validation steps
        df = self.validate_quantity(df)
        df = self.validate_unit_price(df)
        df = self.validate_discount_percentage(df)
        
        # Calculation steps
        df = self.calculate_line_subtotal(df)
        df = self.apply_discount(df)
        df = self.calculate_tax(df)
        df = self.calculate_line_total(df)
        
        # Add validation summary and audit columns
        df = self.add_validation_summary(df)
        df = self.add_audit_columns(df)
        
        # Separate valid and invalid records
        valid_df, invalid_df = self.handle_invalid_records(df)
        
        logger.info("Transformation pipeline completed")
        
        return valid_df, invalid_df


def main():
    """Main execution function"""
    from extract import load_config, SalesLineItemExtractor
    
    config = load_config()
    
    spark = SparkSession.builder \
        .appName("SalesLineItemTransform") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    try:
        # Extract
        extractor = SalesLineItemExtractor(spark, config)
        df = extractor.extract()
        
        # Transform
        transformer = SalesLineItemTransformer(config)
        valid_df, invalid_df = transformer.transform(df)
        
        # Display results
        logger.info("Valid records sample:")
        valid_df.select(
            "line_item_id", "quantity", "unit_price", "discount_percentage",
            "line_subtotal", "discount_amount", "line_net_amount", "tax_amount", "line_total"
        ).show(10, truncate=False)
        
        if invalid_df.count() > 0:
            logger.info("Invalid records sample:")
            invalid_df.select(
                "line_item_id", "validation_messages"
            ).show(10, truncate=False)
        
        logger.info("Transformation completed successfully")
        
    except Exception as e:
        logger.error(f"Transformation failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()