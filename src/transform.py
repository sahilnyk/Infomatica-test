"""
Transform module for customer master data.
Handles data type conversion, null handling, and address standardization.
"""

import logging
import pandas as pd
import re
from typing import Dict, Any, Optional
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CustomerDataTransformer:
    """Transform customer master data with type conversion and standardization."""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize transformer with configuration."""
        self.config = self._load_config(config_path)
        self.transform_config = self.config.get('transform', {})
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load config from {config_path}: {e}")
            raise
    
    def transform_customers(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply all transformations to customer data.
        
        Args:
            df: Raw customer DataFrame
            
        Returns:
            Transformed DataFrame
        """
        logger.info(f"Starting transformation of {len(df)} customer records")
        
        df_transformed = df.copy()
        
        # Apply transformations in sequence
        df_transformed = self._handle_null_values(df_transformed)
        df_transformed = self._convert_data_types(df_transformed)
        df_transformed = self._standardize_names(df_transformed)
        df_transformed = self._standardize_email(df_transformed)
        df_transformed = self._standardize_phone(df_transformed)
        df_transformed = self._standardize_address(df_transformed)
        df_transformed = self._standardize_state(df_transformed)
        df_transformed = self._standardize_zip_code(df_transformed)
        df_transformed = self._standardize_country(df_transformed)
        df_transformed = self._standardize_status(df_transformed)
        df_transformed = self._add_derived_fields(df_transformed)
        
        logger.info(f"Transformation complete. Output records: {len(df_transformed)}")
        
        return df_transformed
    
    def _handle_null_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle null values according to business rules."""
        logger.info("Handling null values")
        
        null_handling = self.transform_config.get('null_handling', {})
        
        # Apply default values for specific fields
        for field, default_value in null_handling.get('defaults', {}).items():
            if field in df.columns:
                df[field] = df[field].fillna(default_value)
                logger.debug(f"Filled null values in {field} with '{default_value}'")
        
        # Trim whitespace that might be treated as null
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
            # Replace empty strings with None
            df[col] = df[col].replace('', None)
        
        return df
    
    def _convert_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert data types according to target schema."""
        logger.info("Converting data types")
        
        # Ensure customer_id is string (as per source definition)
        if 'customer_id' in df.columns:
            df['customer_id'] = df['customer_id'].astype(str)
        
        # Ensure registration_date is datetime
        if 'registration_date' in df.columns:
            df['registration_date'] = pd.to_datetime(df['registration_date'], errors='coerce')
        
        # Ensure string fields are proper strings
        string_fields = [
            'first_name', 'last_name', 'email', 'phone',
            'address_line1', 'address_line2', 'city', 'state',
            'zip_code', 'country', 'status'
        ]
        
        for field in string_fields:
            if field in df.columns:
                df[field] = df[field].astype(str).replace('nan', None)
        
        return df
    
    def _standardize_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize first and last names."""
        logger.info("Standardizing names")
        
        name_config = self.transform_config.get('name_standardization', {})
        
        for name_field in ['first_name', 'last_name']:
            if name_field in df.columns:
                # Title case
                if name_config.get('title_case', True):
                    df[name_field] = df[name_field].apply(
                        lambda x: x.title() if isinstance(x, str) and x else x
                    )
                
                # Remove extra whitespace
                df[name_field] = df[name_field].apply(
                    lambda x: ' '.join(x.split()) if isinstance(x, str) and x else x
                )
        
        return df
    
    def _standardize_email(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize email addresses."""
        logger.info("Standardizing email addresses")
        
        if 'email' not in df.columns:
            return df
        
        # Convert to lowercase
        df['email'] = df['email'].apply(
            lambda x: x.lower().strip() if isinstance(x, str) and x else x
        )
        
        # Validate email format
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        df['email_valid'] = df['email'].apply(
            lambda x: bool(re.match(email_pattern, x)) if isinstance(x, str) and x else False
        )
        
        # Set invalid emails to None if configured
        if self.transform_config.get('email_standardization', {}).get('null_invalid', False):
            df.loc[~df['email_valid'], 'email'] = None
        
        return df
    
    def _standardize_phone(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize phone numbers."""
        logger.info("Standardizing phone numbers")
        
        if 'phone' not in df.columns:
            return df
        
        phone_config = self.transform_config.get('phone_standardization', {})
        
        def clean_phone(phone):
            if not isinstance(phone, str) or not phone:
                return None
            
            # Remove all non-digit characters
            digits = re.sub(r'\D', '', phone)
            
            # Handle different formats
            if len(digits) == 10:
                # US format: (XXX) XXX-XXXX
                return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
            elif len(digits) == 11 and digits[0] == '1':
                # US format with country code
                return f"+1 ({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
            else:
                # Return original if can't standardize
                return phone
        
        if phone_config.get('standardize_format', True):
            df['phone'] = df['phone'].apply(clean_phone)
        
        return df
    
    def _standardize_address(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize address fields."""
        logger.info("Standardizing addresses")
        
        address_config = self.transform_config.get('address_standardization', {})
        
        address_fields = ['address_line1', 'address_line2', 'city']
        
        for field in address_fields:
            if field in df.columns:
                # Title case for addresses
                if address_config.get('title_case', True):
                    df[field] = df[field].apply(
                        lambda x: x.title() if isinstance(x, str) and x else x
                    )
                
                # Standardize common abbreviations
                if address_config.get('standardize_abbreviations', True):
                    df[field] = df[field].apply(self._standardize_address_abbreviations)
        
        return df
    
    def _standardize_address_abbreviations(self, address: str) -> str:
        """Standardize common address abbreviations."""
        if not isinstance(address, str) or not address:
            return address
        
        abbreviations = {
            r'\bSt\.?\b': 'Street',
            r'\bAve\.?\b': 'Avenue',
            r'\bRd\.?\b': 'Road',
            r'\bBlvd\.?\b': 'Boulevard',
            r'\bDr\.?\b': 'Drive',
            r'\bLn\.?\b': 'Lane',
            r'\bCt\.?\b': 'Court',
            r'\bPl\.?\b': 'Place',
            r'\bApt\.?\b': 'Apartment',
            r'\bSte\.?\b': 'Suite',
        }
        
        result = address
        for pattern, replacement in abbreviations.items():
            result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
        
        return result
    
    def _standardize_state(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize state codes."""
        logger.info("Standardizing state codes")
        
        if 'state' not in df.columns:
            return df
        
        # Convert to uppercase
        df['state'] = df['state'].apply(
            lambda x: x.upper().strip() if isinstance(x, str) and x else x
        )
        
        # Validate state codes (US states)
        valid_states = {
            'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
            'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
            'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
            'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
            'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
            'DC', 'PR', 'VI', 'GU', 'AS', 'MP'
        }
        
        df['state_valid'] = df['state'].apply(
            lambda x: x in valid_states if isinstance(x, str) and x else False
        )
        
        return df
    
    def _standardize_zip_code(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize ZIP codes."""
        logger.info("Standardizing ZIP codes")
        
        if 'zip_code' not in df.columns:
            return df
        
        def clean_zip(zip_code):
            if not isinstance(zip_code, str) or not zip_code:
                return None
            
            # Remove all non-digit/hyphen characters
            cleaned = re.sub(r'[^\d-]', '', zip_code)
            
            # Handle 5-digit and 9-digit formats
            digits = re.sub(r'\D', '', cleaned)
            
            if len(digits) == 5:
                return digits
            elif len(digits) == 9:
                return f"{digits[:5]}-{digits[5:]}"
            else:
                return zip_code
        
        df['zip_code'] = df['zip_code'].apply(clean_zip)
        
        return df
    
    def _standardize_country(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize country codes."""
        logger.info("Standardizing country codes")
        
        if 'country' not in df.columns:
            return df
        
        # Convert to uppercase
        df['country'] = df['country'].apply(
            lambda x: x.upper().strip() if isinstance(x, str) and x else x
        )
        
        # Default to US if null
        default_country = self.transform_config.get('country_default', 'US')
        df['country'] = df['country'].fillna(default_country)
        
        return df
    
    def _standardize_status(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize customer status."""
        logger.info("Standardizing customer status")
        
        if 'status' not in df.columns:
            return df
        
        # Convert to uppercase
        df['status'] = df['status'].apply(
            lambda x: x.upper().strip() if isinstance(x, str) and x else x
        )
        
        # Map to standard values
        status_mapping = self.transform_config.get('status_mapping', {
            'ACTIVE': 'ACTIVE',
            'INACTIVE': 'INACTIVE',
            'SUSPENDED': 'SUSPENDED',
            'PENDING': 'PENDING'
        })
        
        df['status'] = df['status'].map(status_mapping).fillna('UNKNOWN')
        
        return df
    
    def _add_derived_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add derived fields for analytics."""
        logger.info("Adding derived fields")
        
        # Full name
        if 'first_name' in df.columns and 'last_name' in df.columns:
            df['full_name'] = df.apply(
                lambda row: f"{row['first_name']} {row['last_name']}"
                if pd.notna(row['first_name']) and pd.notna(row['last_name'])
                else None,
                axis=1
            )
        
        # Full address
        address_parts = ['address_line1', 'address_line2', 'city', 'state', 'zip_code']
        if all(col in df.columns for col in address_parts):
            df['full_address'] = df.apply(
                lambda row: ', '.join(filter(None, [
                    str(row['address_line1']) if pd.notna(row['address_line1']) else None,
                    str(row['address_line2']) if pd.notna(row['address_line2']) else None,
                    str(row['city']) if pd.notna(row['city']) else None,
                    f"{row['state']} {row['zip_code']}" if pd.notna(row['state']) and pd.notna(row['zip_code']) else None
                ])),
                axis=1
            )
        
        # Days since registration
        if 'registration_date' in df.columns:
            df['days_since_registration'] = (
                pd.Timestamp.now() - df['registration_date']
            ).dt.days
        
        # Data quality score
        df['data_quality_score'] = self._calculate_data_quality_score(df)
        
        return df
    
    def _calculate_data_quality_score(self, df: pd.DataFrame) -> pd.Series:
        """Calculate data quality score for each record."""
        score = pd.Series(0, index=df.index)
        
        # Points for non-null critical fields
        critical_fields = ['first_name', 'last_name', 'email', 'phone']
        for field in critical_fields:
            if field in df.columns:
                score += df[field].notna().astype(int) * 25
        
        # Bonus points for valid email
        if 'email_valid' in df.columns:
            score += df['email_valid'].astype(int) * 10
        
        # Bonus points for valid state
        if 'state_valid' in df.columns:
            score += df['state_valid'].astype(int) * 10
        
        # Cap at 100
        score = score.clip(upper=100)
        
        return score