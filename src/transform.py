"""
Transform module for sales processing integration testing.
Handles data transformation and business logic application.
"""
import logging
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)


class SalesDataTransformer:
    """Transforms sales data according to business rules."""
    
    def __init__(self, config: Dict):
        """
        Initialize the transformer with configuration.
        
        Args:
            config: Configuration dictionary containing transformation rules
        """
        self.config = config
        self.transform_config = config.get('transformations', {})
        
    def transform_customers(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform customer data.
        
        Args:
            df: Raw customer DataFrame
            
        Returns:
            Transformed customer DataFrame
        """
        try:
            logger.info("Transforming customer data")
            df_transformed = df.copy()
            
            # Standardize names
            df_transformed['first_name'] = df_transformed['first_name'].str.strip().str.title()
            df_transformed['last_name'] = df_transformed['last_name'].str.strip().str.title()
            df_transformed['full_name'] = (
                df_transformed['first_name'] + ' ' + df_transformed['last_name']
            )
            
            # Standardize email
            df_transformed['email'] = df_transformed['email'].str.lower().str.strip()
            
            # Validate email format
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            df_transformed['email_valid'] = df_transformed['email'].str.match(email_pattern)
            
            # Standardize phone numbers
            df_transformed['phone'] = df_transformed['phone'].str.replace(r'[^\d]', '', regex=True)
            
            # Standardize address
            df_transformed['city'] = df_transformed['city'].str.strip().str.title()
            df_transformed['state'] = df_transformed['state'].str.upper().str.strip()
            df_transformed['country'] = df_transformed['country'].str.upper().str.strip()
            df_transformed['zip_code'] = df_transformed['zip_code'].str.strip()
            
            # Calculate customer tenure
            df_transformed['customer_tenure_days'] = (
                pd.Timestamp.now() - df_transformed['registration_date']
            ).dt.days
            
            # Add data quality flags
            df_transformed['has_complete_address'] = (
                df_transformed['address_line1'].notna() &
                df_transformed['city'].notna() &
                df_transformed['state'].notna() &
                df_transformed['zip_code'].notna()
            )
            
            # Add processing metadata
            df_transformed['processed_date'] = pd.Timestamp.now()
            df_transformed['record_source'] = 'SRC_CUSTOMERS'
            
            logger.info(f"Transformed {len(df_transformed)} customer records")
            return df_transformed
            
        except Exception as e:
            logger.error(f"Error transforming customer data: {str(e)}")
            raise
    
    def transform_sales_orders(
        self, 
        orders_df: pd.DataFrame,
        customers_df: pd.DataFrame,
        line_items_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Transform sales order data with enrichment.
        
        Args:
            orders_df: Raw sales orders DataFrame
            customers_df: Transformed customers DataFrame
            line_items_df: Raw line items DataFrame
            
        Returns:
            Transformed sales orders DataFrame
        """
        try:
            logger.info("Transforming sales order data")
            df_transformed = orders_df.copy()
            
            # Join with customer data
            df_transformed = df_transformed.merge(
                customers_df[['customer_id', 'full_name', 'email', 'state', 'country']],
                on='customer_id',
                how='left',
                suffixes=('', '_customer')
            )
            
            # Calculate order metrics from line items
            order_metrics = line_items_df.groupby('order_id').agg({
                'line_item_id': 'count',
                'quantity': 'sum',
                'line_total': 'sum',
                'tax_amount': 'sum'
            }).reset_index()
            
            order_metrics.columns = [
                'order_id', 'line_item_count', 'total_quantity', 
                'subtotal', 'total_tax'
            ]
            
            df_transformed = df_transformed.merge(
                order_metrics,
                on='order_id',
                how='left'
            )
            
            # Calculate total order amount
            df_transformed['order_total'] = (
                df_transformed['subtotal'] + df_transformed['total_tax']
            )
            
            # Calculate shipping days
            df_transformed['shipping_days'] = (
                df_transformed['ship_date'] - df_transformed['order_date']
            ).dt.days
            
            df_transformed['delivery_days'] = (
                df_transformed['delivery_date'] - df_transformed['ship_date']
            ).dt.days
            
            df_transformed['total_fulfillment_days'] = (
                df_transformed['delivery_date'] - df_transformed['order_date']
            ).dt.days
            
            # Categorize order size
            df_transformed['order_size_category'] = pd.cut(
                df_transformed['order_total'],
                bins=[0, 100, 500, 1000, float('inf')],
                labels=['Small', 'Medium', 'Large', 'Extra Large']
            )
            
            # Flag late shipments (more than 3 days)
            df_transformed['is_late_shipment'] = df_transformed['shipping_days'] > 3
            
            # Flag late deliveries (more than 7 days)
            df_transformed['is_late_delivery'] = df_transformed['delivery_days'] > 7
            
            # Calculate order year, quarter, month
            df_transformed['order_year'] = df_transformed['order_date'].dt.year
            df_transformed['order_quarter'] = df_transformed['order_date'].dt.quarter
            df_transformed['order_month'] = df_transformed['order_date'].dt.month
            df_transformed['order_week'] = df_transformed['order_date'].dt.isocalendar().week
            
            # Add processing metadata
            df_transformed['processed_date'] = pd.Timestamp.now()
            df_transformed['record_source'] = 'SRC_SALES_ORDERS'
            
            logger.info(f"Transformed {len(df_transformed)} sales order records")
            return df_transformed
            
        except Exception as e:
            logger.error(f"Error transforming sales order data: {str(e)}")
            raise
    
    def transform_order_line_items(
        self,
        line_items_df: pd.DataFrame,
        products_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Transform order line item data with product enrichment.
        
        Args:
            line_items_df: Raw line items DataFrame
            products_df: Products DataFrame
            
        Returns:
            Transformed line items DataFrame
        """
        try:
            logger.info("Transforming order line item data")
            df_transformed = line_items_df.copy()
            
            # Join with product data
            df_transformed = df_transformed.merge(
                products_df[['product_id', 'product_name', 'category', 
                           'subcategory', 'brand', 'cost']],
                on='product_id',
                how='left'
            )
            
            # Calculate discount amount
            df_transformed['discount_amount'] = (
                df_transformed['unit_price'] * 
                df_transformed['quantity'] * 
                df_transformed['discount_percent'] / 100
            )
            
            # Recalculate line total for validation
            df_transformed['calculated_line_total'] = (
                (df_transformed['unit_price'] * df_transformed['quantity']) -
                df_transformed['discount_amount'] +
                df_transformed['tax_amount']
            )
            
            # Flag discrepancies
            df_transformed['has_amount_discrepancy'] = (
                abs(df_transformed['line_total'] - 
                    df_transformed['calculated_line_total']) > 0.01
            )
            
            # Calculate profit metrics
            df_transformed['line_cost'] = df_transformed['cost'] * df_transformed['quantity']
            df_transformed['line_profit'] = (
                df_transformed['line_total'] - 
                df_transformed['tax_amount'] - 
                df_transformed['line_cost']
            )
            df_transformed['profit_margin'] = (
                df_transformed['line_profit'] / 
                (df_transformed['line_total'] - df_transformed['tax_amount']) * 100
            ).fillna(0)
            
            # Categorize discount levels
            df_transformed['discount_category'] = pd.cut(
                df_transformed['discount_percent'],
                bins=[-0.1, 0, 10, 20, 100],
                labels=['None', 'Low', 'Medium', 'High']
            )
            
            # Add processing metadata
            df_transformed['processed_date'] = pd.Timestamp.now()
            df_transformed['record_source'] = 'SRC_ORDER_LINE_ITEMS'
            
            logger.info(f"Transformed {len(df_transformed)} order line item records")
            return df_transformed
            
        except Exception as e:
            logger.error(f"Error transforming order line item data: {str(e)}")
            raise
    
    def create_sales_summary(
        self,
        orders_df: pd.DataFrame,
        line_items_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Create aggregated sales summary.
        
        Args:
            orders_df: Transformed orders DataFrame
            line_items_df: Transformed line items DataFrame
            
        Returns:
            Sales summary DataFrame
        """
        try:
            logger.info("Creating sales summary")
            
            # Aggregate by customer
            customer_summary = orders_df.groupby('customer_id').agg({
                'order_id': 'count',
                'order_total': 'sum',
                'order_date': ['min', 'max']
            }).reset_index()
            
            customer_summary.columns = [
                'customer_id', 'total_orders', 'total_revenue',
                'first_order_date', 'last_order_date'
            ]
            
            # Calculate customer lifetime value metrics
            customer_summary['customer_lifetime_days'] = (
                customer_summary['last_order_date'] - 
                customer_summary['first_order_date']
            ).dt.days
            
            customer_summary['average_order_value'] = (
                customer_summary['total_revenue'] / customer_summary['total_orders']
            )
            
            # Categorize customers
            customer_summary['customer_segment'] = pd.cut(
                customer_summary['total_orders'],
                bins=[0, 1, 5, 10, float('inf')],
                labels=['One-time', 'Occasional', 'Regular', 'VIP']
            )
            
            logger.info(f"Created sales summary for {len(customer_summary)} customers")
            return customer_summary
            
        except Exception as e:
            logger.error(f"Error creating sales summary: {str(e)}")
            raise
    
    def transform_all(self, extracted_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Transform all extracted data.
        
        Args:
            extracted_data: Dictionary of extracted DataFrames
            
        Returns:
            Dictionary of transformed DataFrames
        """
        logger.info("Starting transformation of all data")
        
        # Transform in dependency order
        transformed_customers = self.transform_customers(extracted_data['customers'])
        
        transformed_line_items = self.transform_order_line_items(
            extracted_data['order_line_items'],
            extracted_data['products']
        )
        
        transformed_orders = self.transform_sales_orders(
            extracted_data['sales_orders'],
            transformed_customers,
            extracted_data['order_line_items']
        )
        
        sales_summary = self.create_sales_summary(
            transformed_orders,
            transformed_line_items
        )
        
        transformed_data = {
            'customers': transformed_customers,
            'sales_orders': transformed_orders,
            'order_line_items': transformed_line_items,
            'sales_summary': sales_summary
        }
        
        logger.info("Completed transformation of all data")
        return transformed_data


def validate_transformations(transformed_data: Dict[str, pd.DataFrame]) -> Dict[str, Dict]:
    """
    Validate transformed data quality.
    
    Args:
        transformed_data: Dictionary of transformed DataFrames
        
    Returns:
        Dictionary containing validation results
    """
    validation_results = {}
    
    for data_name, df in transformed_data.items():
        results = {
            'record_count': len(df),
            'null_counts': df.isnull().sum().to_dict(),
            'columns': list(df.columns)
        }
        
        # Add specific validations
        if data_name == 'customers':
            results['invalid_emails'] = (~df['email_valid']).sum()
            results['incomplete_addresses'] = (~df['has_complete_address']).sum()
        elif data_name == 'sales_orders':
            results['late_shipments'] = df['is_late_shipment'].sum()
            results['late_deliveries'] = df['is_late_delivery'].sum()
        elif data_name == 'order_line_items':
            results['amount_discrepancies'] = df['has_amount_discrepancy'].sum()
        
        validation_results[data_name] = results
        logger.info(f"Validation for {data_name}: {results}")
    
    return validation_results