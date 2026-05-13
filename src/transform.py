"""
Transform module for customer address data processing.
Handles address type validation, primary flag processing, and customer ID lookup.
"""
import logging
from typing import Dict, List, Optional, Set
import pandas as pd
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)


class AddressDataTransformer:
    """Transforms customer address data with validation and enrichment."""
    
    # Valid address types based on business rules
    VALID_ADDRESS_TYPES = {
        'HOME', 'WORK', 'BILLING', 'SHIPPING', 'MAILING', 'OTHER'
    }
    
    # Default address type for invalid values
    DEFAULT_ADDRESS_TYPE = 'OTHER'
    
    def __init__(self, config: Dict):
        """
        Initialize the transformer with configuration.
        
        Args:
            config: Configuration dictionary containing transformation rules
        """
        self.config = config
        self.transform_config = config.get('transform', {})
        self.valid_types = set(self.transform_config.get(
            'valid_address_types', 
            list(self.VALID_ADDRESS_TYPES)
        ))
        self.default_type = self.transform_config.get(
            'default_address_type',
            self.DEFAULT_ADDRESS_TYPE
        )
        
    def validate_address_type(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate and standardize address types.
        
        Args:
            df: DataFrame containing address records
            
        Returns:
            DataFrame with validated address_type column
        """
        logger.info("Validating address types")
        
        df = df.copy()
        
        # Standardize to uppercase
        df['address_type'] = df['address_type'].str.upper().str.strip()
        
        # Track invalid types for logging
        invalid_mask = ~df['address_type'].isin(self.valid_types)
        invalid_count = invalid_mask.sum()
        
        if invalid_count > 0:
            logger.warning(
                f"Found {invalid_count} invalid address types, "
                f"setting to default: {self.default_type}"
            )
            invalid_types = df.loc[invalid_mask, 'address_type'].unique()
            logger.debug(f"Invalid types found: {list(invalid_types)}")
            
        # Replace invalid types with default
        df.loc[invalid_mask, 'address_type'] = self.default_type
        
        # Handle null values
        null_mask = df['address_type'].isna()
        if null_mask.any():
            logger.warning(
                f"Found {null_mask.sum()} null address types, "
                f"setting to default: {self.default_type}"
            )
            df.loc[null_mask, 'address_type'] = self.default_type
        
        logger.info("Address type validation complete")
        return df
    
    def process_primary_flag(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process and validate primary address flags.
        Ensures only one primary address per customer.
        
        Args:
            df: DataFrame containing address records
            
        Returns:
            DataFrame with validated is_primary flag
        """
        logger.info("Processing primary address flags")
        
        df = df.copy()
        
        # Standardize primary flag to boolean
        df['is_primary'] = df['is_primary'].astype(str).str.upper().str.strip()
        df['is_primary_flag'] = df['is_primary'].isin(['Y', 'YES', 'TRUE', '1', 'T'])
        
        # Check for multiple primary addresses per customer
        primary_counts = df[df['is_primary_flag']].groupby('customer_id').size()
        multiple_primary = primary_counts[primary_counts > 1]
        
        if len(multiple_primary) > 0:
            logger.warning(
                f"Found {len(multiple_primary)} customers with multiple primary addresses"
            )
            
            # Keep only the most recent primary address per customer
            for customer_id in multiple_primary.index:
                customer_mask = df['customer_id'] == customer_id
                primary_mask = customer_mask & df['is_primary_flag']
                
                # Sort by modified_date (most recent first) or created_date
                date_col = 'modified_date' if 'modified_date' in df.columns else 'created_date'
                if date_col in df.columns:
                    customer_primaries = df[primary_mask].sort_values(
                        date_col, 
                        ascending=False
                    )
                    # Keep first (most recent), reset others
                    indices_to_reset = customer_primaries.index[1:]
                    df.loc[indices_to_reset, 'is_primary_flag'] = False
                    logger.debug(
                        f"Customer {customer_id}: kept most recent primary, "
                        f"reset {len(indices_to_reset)} others"
                    )
        
        # Ensure each customer has at least one primary address
        customers_without_primary = df.groupby('customer_id')['is_primary_flag'].any()
        customers_without_primary = customers_without_primary[~customers_without_primary].index
        
        if len(customers_without_primary) > 0:
            logger.warning(
                f"Found {len(customers_without_primary)} customers without primary address"
            )
            
            for customer_id in customers_without_primary:
                customer_mask = df['customer_id'] == customer_id
                # Set first address as primary
                first_idx = df[customer_mask].index[0]
                df.loc[first_idx, 'is_primary_flag'] = True
                logger.debug(f"Set first address as primary for customer {customer_id}")
        
        logger.info("Primary flag processing complete")
        return df
    
    def lookup_customer_data(
        self, 
        addresses_df: pd.DataFrame, 
        customers_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Enrich address data with customer information.
        
        Args:
            addresses_df: DataFrame containing address records
            customers_df: DataFrame containing customer records
            
        Returns:
            DataFrame with enriched customer information
        """
        logger.info("Performing customer data lookup")
        
        # Select relevant customer columns for enrichment
        customer_cols = ['customer_id', 'first_name', 'last_name', 'email', 'status']
        available_cols = [col for col in customer_cols if col in customers_df.columns]
        
        # Perform left join to preserve all addresses
        enriched_df = addresses_df.merge(
            customers_df[available_cols],
            on='customer_id',
            how='left',
            suffixes=('', '_customer')
        )
        
        # Check for addresses without matching customers
        missing_customers = enriched_df['first_name'].isna()
        if missing_customers.any():
            missing_count = missing_customers.sum()
            logger.warning(
                f"Found {missing_count} addresses without matching customer records"
            )
            missing_ids = enriched_df.loc[missing_customers, 'customer_id'].unique()
            logger.debug(f"Customer IDs not found: {list(missing_ids)[:10]}")
        
        logger.info(f"Customer lookup complete: {len(enriched_df)} records enriched")
        return enriched_df
    
    def standardize_address_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize address field formats and values.
        
        Args:
            df: DataFrame containing address records
            
        Returns:
            DataFrame with standardized address fields
        """
        logger.info("Standardizing address fields")
        
        df = df.copy()
        
        # Standardize state codes to uppercase
        if 'state' in df.columns:
            df['state'] = df['state'].str.upper().str.strip()
        
        # Standardize country codes to uppercase
        if 'country' in df.columns:
            df['country'] = df['country'].str.upper().str.strip()
            # Default to US if null
            df['country'] = df['country'].fillna('US')
        
        # Clean zip codes (remove spaces, standardize format)
        if 'zip_code' in df.columns:
            df['zip_code'] = df['zip_code'].astype(str).str.strip()
            df['zip_code'] = df['zip_code'].replace('nan', '')
        
        # Trim and clean address lines
        address_fields = ['address_line1', 'address_line2', 'city']
        for field in address_fields:
            if field in df.columns:
                df[field] = df[field].str.strip()
                df[field] = df[field].replace('', None)
        
        logger.info("Address field standardization complete")
        return df
    
    def add_audit_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add audit columns for tracking data lineage.
        
        Args:
            df: DataFrame to add audit columns to
            
        Returns:
            DataFrame with audit columns added
        """
        logger.info("Adding audit columns")
        
        df = df.copy()
        current_timestamp = datetime.now()
        
        df['etl_processed_date'] = current_timestamp
        df['etl_source_system'] = 'INFORMATICA_MIGRATION'
        df['etl_batch_id'] = self.config.get('batch_id', 'BATCH_001')
        
        logger.info("Audit columns added")
        return df
    
    def transform(
        self, 
        addresses_df: pd.DataFrame, 
        customers_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Execute complete transformation pipeline.
        
        Args:
            addresses_df: DataFrame containing address records
            customers_df: DataFrame containing customer records
            
        Returns:
            Fully transformed DataFrame ready for loading
        """
        logger.info("Starting address data transformation pipeline")
        
        # Step 1: Validate address types
        df = self.validate_address_type(addresses_df)
        
        # Step 2: Process primary flags
        df = self.process_primary_flag(df)
        
        # Step 3: Lookup customer data
        df = self.lookup_customer_data(df, customers_df)
        
        # Step 4: Standardize address fields
        df = self.standardize_address_fields(df)
        
        # Step 5: Add audit columns
        df = self.add_audit_columns(df)
        
        logger.info(
            f"Transformation pipeline complete: {len(df)} records transformed"
        )
        return df