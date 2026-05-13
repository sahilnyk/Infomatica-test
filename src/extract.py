"""
Extract module for customer master data from flat files.
Handles reading source data with proper encoding and error handling.
"""

import logging
import pandas as pd
from pathlib import Path
from typing import Dict, Any, Optional
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CustomerDataExtractor:
    """Extract customer master data from flat files."""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize extractor with configuration."""
        self.config = self._load_config(config_path)
        self.source_config = self.config.get('source', {})
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load config from {config_path}: {e}")
            raise
    
    def extract_customers(self, file_path: Optional[str] = None) -> pd.DataFrame:
        """
        Extract customer master data from flat file.
        
        Args:
            file_path: Path to customer data file. If None, uses config.
            
        Returns:
            DataFrame containing customer data
        """
        if file_path is None:
            file_path = self.source_config.get('customers_file')
        
        if not file_path:
            raise ValueError("Customer file path not provided")
        
        logger.info(f"Extracting customer data from {file_path}")
        
        try:
            # Define expected schema based on Informatica source definition
            dtype_mapping = {
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
            }
            
            df = pd.read_csv(
                file_path,
                dtype=dtype_mapping,
                encoding=self.source_config.get('encoding', 'utf-8'),
                na_values=self.source_config.get('na_values', ['', 'NULL', 'null', 'NA']),
                keep_default_na=True
            )
            
            # Parse registration_date separately to handle datetime
            if 'registration_date' in df.columns:
                df['registration_date'] = pd.to_datetime(
                    df['registration_date'],
                    errors='coerce',
                    format=self.source_config.get('date_format', '%Y-%m-%d %H:%M:%S')
                )
            
            logger.info(f"Successfully extracted {len(df)} customer records")
            logger.info(f"Columns: {list(df.columns)}")
            
            return df
            
        except FileNotFoundError:
            logger.error(f"Customer file not found: {file_path}")
            raise
        except Exception as e:
            logger.error(f"Error extracting customer data: {e}")
            raise
    
    def extract_addresses(self, file_path: Optional[str] = None) -> pd.DataFrame:
        """
        Extract customer address data from flat file.
        
        Args:
            file_path: Path to address data file. If None, uses config.
            
        Returns:
            DataFrame containing address data
        """
        if file_path is None:
            file_path = self.source_config.get('addresses_file')
        
        if not file_path:
            raise ValueError("Address file path not provided")
        
        logger.info(f"Extracting address data from {file_path}")
        
        try:
            dtype_mapping = {
                'address_id': str,
                'customer_id': str,
                'address_type': str
            }
            
            df = pd.read_csv(
                file_path,
                dtype=dtype_mapping,
                encoding=self.source_config.get('encoding', 'utf-8'),
                na_values=self.source_config.get('na_values', ['', 'NULL', 'null', 'NA']),
                keep_default_na=True
            )
            
            logger.info(f"Successfully extracted {len(df)} address records")
            
            return df
            
        except FileNotFoundError:
            logger.error(f"Address file not found: {file_path}")
            raise
        except Exception as e:
            logger.error(f"Error extracting address data: {e}")
            raise
    
    def validate_extraction(self, df: pd.DataFrame, source_name: str) -> bool:
        """
        Validate extracted data meets basic requirements.
        
        Args:
            df: DataFrame to validate
            source_name: Name of source for logging
            
        Returns:
            True if validation passes
        """
        logger.info(f"Validating {source_name} extraction")
        
        if df.empty:
            logger.warning(f"{source_name} extraction resulted in empty DataFrame")
            return False
        
        # Check for required columns based on source
        if source_name == 'customers':
            required_cols = ['customer_id', 'first_name', 'last_name']
        elif source_name == 'addresses':
            required_cols = ['address_id', 'customer_id']
        else:
            required_cols = []
        
        missing_cols = set(required_cols) - set(df.columns)
        if missing_cols:
            logger.error(f"Missing required columns in {source_name}: {missing_cols}")
            return False
        
        # Check for null customer_id (NOT NULL constraint)
        if 'customer_id' in df.columns:
            null_count = df['customer_id'].isna().sum()
            if null_count > 0:
                logger.error(f"Found {null_count} null customer_id values in {source_name}")
                return False
        
        logger.info(f"{source_name} validation passed")
        return True