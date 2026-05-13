"""
Extract module for customer address data from source systems.
Handles reading customer and address data from flat files.
"""
import logging
from typing import Dict, List, Optional
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)


class AddressDataExtractor:
    """Extracts customer and address data from source files."""
    
    def __init__(self, config: Dict):
        """
        Initialize the extractor with configuration.
        
        Args:
            config: Configuration dictionary containing source paths and settings
        """
        self.config = config
        self.source_config = config.get('source', {})
        
    def extract_customers(self) -> pd.DataFrame:
        """
        Extract customer master data from source file.
        
        Returns:
            DataFrame containing customer records
            
        Raises:
            FileNotFoundError: If source file does not exist
            ValueError: If required columns are missing
        """
        file_path = self.source_config.get('customers_file')
        if not file_path:
            raise ValueError("customers_file not configured")
            
        logger.info(f"Extracting customer data from {file_path}")
        
        try:
            df = pd.read_csv(
                file_path,
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
            
            required_columns = ['customer_id', 'first_name', 'last_name']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            logger.info(f"Extracted {len(df)} customer records")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting customer data: {str(e)}")
            raise
    
    def extract_addresses(self) -> pd.DataFrame:
        """
        Extract customer address data from source file.
        
        Returns:
            DataFrame containing address records
            
        Raises:
            FileNotFoundError: If source file does not exist
            ValueError: If required columns are missing
        """
        file_path = self.source_config.get('addresses_file')
        if not file_path:
            raise ValueError("addresses_file not configured")
            
        logger.info(f"Extracting address data from {file_path}")
        
        try:
            df = pd.read_csv(
                file_path,
                dtype={
                    'address_id': str,
                    'customer_id': str,
                    'address_type': str,
                    'address_line1': str,
                    'address_line2': str,
                    'city': str,
                    'state': str,
                    'zip_code': str,
                    'country': str,
                    'is_primary': str,
                    'status': str
                },
                parse_dates=['created_date', 'modified_date']
            )
            
            required_columns = ['address_id', 'customer_id', 'address_type']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            logger.info(f"Extracted {len(df)} address records")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting address data: {str(e)}")
            raise
    
    def extract_all(self) -> Dict[str, pd.DataFrame]:
        """
        Extract all source data.
        
        Returns:
            Dictionary with 'customers' and 'addresses' DataFrames
        """
        return {
            'customers': self.extract_customers(),
            'addresses': self.extract_addresses()
        }