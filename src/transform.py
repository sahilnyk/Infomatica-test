"""
Sales Returns Processing - Transform Module
Transforms returns data including refund calculations, reason code validation, and status tracking.
"""

import logging
from typing import Dict, Any, Optional, List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from decimal import Decimal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SalesReturnsTransformer:
    """Handles transformation logic for sales returns processing."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the transformer.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.transform_config = config.get('transformations', {})
        self.business_rules = config.get('business_rules', {})
        
    def validate_reason_codes(self, returns_df: DataFrame) -> DataFrame:
        """
        Validate return reason codes against allowed values.
        
        Args:
            returns_df: DataFrame containing returns data
            
        Returns:
            DataFrame with validated reason codes and validation flag
        """
        try:
            logger.info("Validating return reason codes")
            
            valid_codes = self.business_rules.get('valid_reason_codes', [])
            
            # Add validation flag
            validated_df = returns_df.withColumn(
                'reason_code_valid',
                F.when(
                    F.col('reason_code').isin(valid_codes),
                    F.lit(True)
                ).otherwise(F.lit(False))
            )
            
            # Add validation message
            validated_df = validated_df.withColumn(
                'reason_code_validation_msg',
                F.when(
                    F.col('reason_code_valid') == False,
                    F.concat(
                        F.lit('Invalid reason code: '),
                        F.col('reason_code')
                    )
                ).otherwise(F.lit(None))
            )
            
            invalid_count = validated_df.filter(F.col('reason_code_valid') == False).count()
            logger.info(f"Found {invalid_count} returns with invalid reason codes")
            
            return validated_df
            
        except Exception as e:
            logger.error(f"Error validating reason codes: {str(e)}")
            raise
    
    def calculate_refund_amounts(
        self,
        return_items_df: DataFrame,
        products_df: DataFrame
    ) -> DataFrame:
        """
        Calculate refund amounts for return items including restocking fees.
        
        Args:
            return_items_df: DataFrame containing return items
            products_df: DataFrame containing product pricing
            
        Returns:
            DataFrame with calculated refund amounts
        """
        try:
            logger.info("Calculating refund amounts")
            
            restocking_fee_pct = self.business_rules.get('restocking_fee_percentage', 0.15)
            min_refund_amount = self.business_rules.get('min_refund_amount', 0.0)
            
            # Join with products to get original prices
            items_with_price = return_items_df.join(
                products_df.select('product_id', 'unit_price'),
                on='product_id',
                how='left'
            )
            
            # Calculate base refund amount
            refund_df = items_with_price.withColumn(
                'base_refund_amount',
                F.col('quantity') * F.coalesce(F.col('return_unit_price'), F.col('unit_price'))
            )
            
            # Calculate restocking fee
            refund_df = refund_df.withColumn(
                'restocking_fee',
                F.when(
                    F.col('apply_restocking_fee') == True,
                    F.col('base_refund_amount') * F.lit(restocking_fee_pct)
                ).otherwise(F.lit(0.0))
            )
            
            # Calculate net refund amount
            refund_df = refund_df.withColumn(
                'net_refund_amount',
                F.greatest(
                    F.col('base_refund_amount') - F.col('restocking_fee'),
                    F.lit(min_refund_amount)
                )
            )
            
            # Round to 2 decimal places
            refund_df = refund_df.withColumn(
                'net_refund_amount',
                F.round(F.col('net_refund_amount'), 2)
            ).withColumn(
                'restocking_fee',
                F.round(F.col('restocking_fee'), 2)
            ).withColumn(
                'base_refund_amount',
                F.round(F.col('base_refund_amount'), 2)
            )
            
            total_refund = refund_df.agg(
                F.sum('net_refund_amount').alias('total')
            ).collect()[0]['total']
            
            logger.info(f"Calculated total refund amount: ${total_refund:,.2f}")
            
            return refund_df
            
        except Exception as e:
            logger.error(f"Error calculating refund amounts: {str(e)}")
            raise
    
    def determine_return_status(self, returns_df: DataFrame) -> DataFrame:
        """
        Determine return status based on business rules.
        
        Args:
            returns_df: DataFrame containing returns data
            
        Returns:
            DataFrame with determined return status
        """
        try:
            logger.info("Determining return status")
            
            max_return_days = self.business_rules.get('max_return_days', 30)
            
            # Calculate days since purchase
            status_df = returns_df.withColumn(
                'days_since_purchase',
                F.datediff(F.col('return_date'), F.col('order_date'))
            )
            
            # Determine eligibility
            status_df = status_df.withColumn(
                'return_eligible',
                F.when(
                    (F.col('days_since_purchase') <= max_return_days) &
                    (F.col('reason_code_valid') == True) &
                    (F.col('order_status') == 'COMPLETED'),
                    F.lit(True)
                ).otherwise(F.lit(False))
            )
            
            # Determine return status
            status_df = status_df.withColumn(
                'return_status',
                F.when(
                    F.col('return_eligible') == False,
                    F.lit('REJECTED')
                ).when(
                    F.col('current_status').isNull(),
                    F.lit('PENDING')
                ).otherwise(F.col('current_status'))
            )
            
            # Add rejection reason
            status_df = status_df.withColumn(
                'rejection_reason',
                F.when(
                    (F.col('return_eligible') == False) & 
                    (F.col('days_since_purchase') > max_return_days),
                    F.lit('Return period exceeded')
                ).when(
                    (F.col('return_eligible') == False) & 
                    (F.col('reason_code_valid') == False),
                    F.lit('Invalid reason code')
                ).when(
                    (F.col('return_eligible') == False) & 
                    (F.col('order_status') != 'COMPLETED'),
                    F.lit('Order not completed')
                ).otherwise(F.lit(None))
            )
            
            # Status distribution
            status_counts = status_df.groupBy('return_status').count().collect()
            for row in status_counts:
                logger.info(f"Status {row['return_status']}: {row['count']} returns")
            
            return status_df
            
        except Exception as e:
            logger.error(f"Error determining return status: {str(e)}")
            raise
    
    def aggregate_return_totals(
        self,
        returns_df: DataFrame,
        return_items_df: DataFrame
    ) -> DataFrame:
        """
        Aggregate return totals by return ID.
        
        Args:
            returns_df: DataFrame containing returns header data
            return_items_df: DataFrame containing return items with refund amounts
            
        Returns:
            DataFrame with aggregated return totals
        """
        try:
            logger.info("Aggregating return totals")
            
            # Aggregate item-level data
            item_aggregates = return_items_df.groupBy('return_id').agg(
                F.sum('quantity').alias('total_items_returned'),
                F.sum('base_refund_amount').alias('total_base_refund'),
                F.sum('restocking_fee').alias('total_restocking_fee'),
                F.sum('net_refund_amount').alias('total_net_refund'),
                F.count('*').alias('line_item_count')
            )
            
            # Join with returns header
            aggregated_df = returns_df.join(
                item_aggregates,
                on='return_id',
                how='left'
            )
            
            # Fill nulls for returns with no items
            aggregated_df = aggregated_df.fillna({
                'total_items_returned': 0,
                'total_base_refund': 0.0,
                'total_restocking_fee': 0.0,
                'total_net_refund': 0.0,
                'line_item_count': 0
            })
            
            logger.info("Return totals aggregated successfully")
            
            return aggregated_df
            
        except Exception as e:
            logger.error(f"Error aggregating return totals: {str(e)}")
            raise
    
    def enrich_with_customer_data(
        self,
        returns_df: DataFrame,
        customers_df: DataFrame
    ) -> DataFrame:
        """
        Enrich returns data with customer information.
        
        Args:
            returns_df: DataFrame containing returns data
            customers_df: DataFrame containing customer data
            
        Returns:
            DataFrame enriched with customer information
        """
        try:
            logger.info("Enriching returns with customer data")
            
            # Select relevant customer fields
            customer_fields = customers_df.select(
                'customer_id',
                'first_name',
                'last_name',
                'email',
                'phone',
                'status',
                'registration_date'
            ).withColumnRenamed('status', 'customer_status')
            
            # Join with returns
            enriched_df = returns_df.join(
                customer_fields,
                on='customer_id',
                how='left'
            )
            
            # Add customer full name
            enriched_df = enriched_df.withColumn(
                'customer_full_name',
                F.concat_ws(' ', F.col('first_name'), F.col('last_name'))
            )
            
            # Calculate customer tenure
            enriched_df = enriched_df.withColumn(
                'customer_tenure_days',
                F.datediff(F.col('return_date'), F.col('registration_date'))
            )
            
            logger.info("Customer data enrichment complete")
            
            return enriched_df
            
        except Exception as e:
            logger.error(f"Error enriching with customer data: {str(e)}")
            raise
    
    def add_audit_columns(self, df: DataFrame) -> DataFrame:
        """
        Add audit columns to the DataFrame.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with audit columns added
        """
        try:
            logger.info("Adding audit columns")
            
            audit_df = df.withColumn(
                'processed_timestamp',
                F.current_timestamp()
            ).withColumn(
                'processed_date',
                F.current_date()
            ).withColumn(
                'source_system',
                F.lit('INFORMATICA_MIGRATION')
            ).withColumn(
                'data_quality_score',
                F.when(
                    (F.col('reason_code_valid') == True) &
                    (F.col('return_eligible') == True),
                    F.lit(100)
                ).when(
                    F.col('reason_code_valid') == False,
                    F.lit(50)
                ).otherwise(F.lit(75))
            )
            
            return audit_df
            
        except Exception as e:
            logger.error(f"Error adding audit columns: {str(e)}")
            raise
    
    def transform_returns(
        self,
        returns_df: DataFrame,
        return_items_df: DataFrame,
        customers_df: DataFrame,
        orders_df: DataFrame,
        products_df: DataFrame
    ) -> Dict[str, DataFrame]:
        """
        Execute complete transformation pipeline for returns processing.
        
        Args:
            returns_df: Returns header data
            return_items_df: Return line items data
            customers_df: Customer master data
            orders_df: Order data
            products_df: Product master data
            
        Returns:
            Dictionary containing transformed DataFrames
        """
        try:
            logger.info("Starting returns transformation pipeline")
            
            # Join returns with orders to get order details
            returns_with_orders = returns_df.join(
                orders_df.select(
                    'order_id',
                    F.col('order_date').alias('order_date'),
                    F.col('status').alias('order_status')
                ),
                on='order_id',
                how='left'
            )
            
            # Step 1: Validate reason codes
            validated_returns = self.validate_reason_codes(returns_with_orders)
            
            # Step 2: Determine return status
            status_returns = self.determine_return_status(validated_returns)
            
            # Step 3: Calculate refund amounts for items
            refund_items = self.calculate_refund_amounts(return_items_df, products_df)
            
            # Step 4: Aggregate return totals
            aggregated_returns = self.aggregate_return_totals(status_returns, refund_items)
            
            # Step 5: Enrich with customer data
            enriched_returns = self.enrich_with_customer_data(
                aggregated_returns,
                customers_df
            )
            
            # Step 6: Add audit columns
            final_returns = self.add_audit_columns(enriched_returns)
            final_items = self.add_audit_columns(refund_items)
            
            logger.info("Returns transformation pipeline completed successfully")
            
            return {
                'returns': final_returns,
                'return_items': final_items
            }
            
        except Exception as e:
            logger.error(f"Error in transformation pipeline: {str(e)}")
            raise


if __name__ == "__main__":
    from extract import load_config
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("SalesReturnsTransform") \
        .getOrCreate()
    
    try:
        # Load configuration
        config = load_config()
        
        # Initialize transformer
        transformer = SalesReturnsTransformer(config)
        
        logger.info("Transformer initialized successfully")
        
    except Exception as e:
        logger.error(f"Transformation job failed: {str(e)}")
        raise
    finally:
        spark.stop()