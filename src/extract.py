"""
Extract module for sales processing integration testing.
Handles data extraction from multiple source systems.
"""
import logging
from typing import Dict, List, Optional
from datetime import datetime
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)


class SalesDataExtractor:
    """Extracts sales data from various source systems."""
    
    def __init__(self, config: Dict):
        """
        Initialize the extractor with configuration.
        
        Args:
            config: Configuration dictionary containing source paths and settings
        """
        self.config = config
        self.source_config = config.get('sources', {})
        
    def extract_customers(self) -> pd.DataFrame:
        """
        Extract customer master data.
        
        Returns:
            DataFrame containing customer records
        """
        try:
            source_path = self.source_config.get('customers', {}).get('path')
            logger.info(f"Extracting customer data from {source_path}")
            
            df = pd.read_csv(
                source_path,
                dtype={
                    'customer_id': str,
                    'first_name': str,
                    'last_name': str,
                    'email': str,
                    'phone': str,
                    'address_line1': str,
                    'address_line2': str,
                    'city': str,
                    'state': str,
                    'zip_code': str,
                    'country': str,
                    'status': str
                },
                parse_dates=['registration_date']
            )
            
            logger.info(f"Extracted {len(df)} customer records")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting customer data: {str(e)}")
            raise
    
    def extract_customer_addresses(self) -> pd.DataFrame:
        """
        Extract customer address data.
        
        Returns:
            DataFrame containing customer address records
        """
        try:
            source_path = self.source_config.get('customer_addresses', {}).get('path')
            logger.info(f"Extracting customer address data from {source_path}")
            
            df = pd.read_csv(
                source_path,
                dtype={
                    'address_id': str,
                    'customer_id': str,
                    'address_type': str
                }
            )
            
            logger.info(f"Extracted {len(df)} customer address records")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting customer address data: {str(e)}")
            raise
    
    def extract_sales_orders(self) -> pd.DataFrame:
        """
        Extract sales order data.
        
        Returns:
            DataFrame containing sales order records
        """
        try:
            source_path = self.source_config.get('sales_orders', {}).get('path')
            logger.info(f"Extracting sales order data from {source_path}")
            
            df = pd.read_csv(
                source_path,
                dtype={
                    'order_id': str,
                    'customer_id': str,
                    'order_status': str,
                    'payment_method': str,
                    'shipping_method': str
                },
                parse_dates=['order_date', 'ship_date', 'delivery_date']
            )
            
            logger.info(f"Extracted {len(df)} sales order records")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting sales order data: {str(e)}")
            raise
    
    def extract_order_line_items(self) -> pd.DataFrame:
        """
        Extract order line item data.
        
        Returns:
            DataFrame containing order line item records
        """
        try:
            source_path = self.source_config.get('order_line_items', {}).get('path')
            logger.info(f"Extracting order line item data from {source_path}")
            
            df = pd.read_csv(
                source_path,
                dtype={
                    'line_item_id': str,
                    'order_id': str,
                    'product_id': str,
                    'quantity': int,
                    'unit_price': float,
                    'discount_percent': float,
                    'tax_amount': float,
                    'line_total': float
                }
            )
            
            logger.info(f"Extracted {len(df)} order line item records")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting order line item data: {str(e)}")
            raise
    
    def extract_products(self) -> pd.DataFrame:
        """
        Extract product master data.
        
        Returns:
            DataFrame containing product records
        """
        try:
            source_path = self.source_config.get('products', {}).get('path')
            logger.info(f"Extracting product data from {source_path}")
            
            df = pd.read_csv(
                source_path,
                dtype={
                    'product_id': str,
                    'product_name': str,
                    'category': str,
                    'subcategory': str,
                    'brand': str,
                    'sku': str,
                    'unit_price': float,
                    'cost': float,
                    'status': str
                }
            )
            
            logger.info(f"Extracted {len(df)} product records")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting product data: {str(e)}")
            raise
    
    def extract_all(self) -> Dict[str, pd.DataFrame]:
        """
        Extract all source data.
        
        Returns:
            Dictionary mapping source names to DataFrames
        """
        logger.info("Starting extraction of all source data")
        
        extracted_data = {
            'customers': self.extract_customers(),
            'customer_addresses': self.extract_customer_addresses(),
            'sales_orders': self.extract_sales_orders(),
            'order_line_items': self.extract_order_line_items(),
            'products': self.extract_products()
        }
        
        logger.info("Completed extraction of all source data")
        return extracted_data


def validate_extraction(data: Dict[str, pd.DataFrame]) -> Dict[str, Dict]:
    """
    Validate extracted data quality.
    
    Args:
        data: Dictionary of extracted DataFrames
        
    Returns:
        Dictionary containing validation results
    """
    validation_results = {}
    
    for source_name, df in data.items():
        results = {
            'record_count': len(df),
            'null_counts': df.isnull().sum().to_dict(),
            'duplicate_count': df.duplicated().sum(),
            'columns': list(df.columns)
        }
        validation_results[source_name] = results
        
        logger.info(f"Validation for {source_name}: {results['record_count']} records, "
                   f"{results['duplicate_count']} duplicates")
    
    return validation_results