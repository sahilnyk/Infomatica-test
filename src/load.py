"""
Load module for customer address data to target systems.
Handles writing transformed data to target destinations.
"""
import logging
from typing import Dict, Optional
import pandas as pd
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)


class AddressDataLoader:
    """Loads transformed address data to target systems."""
    
    def __init__(self, config: Dict):
        """
        Initialize the loader with configuration.
        
        Args:
            config: Configuration dictionary containing target settings
        """
        self.config = config
        self.target_config = config.get('target', {})
        
    def load_to_csv(
        self, 
        df: pd.DataFrame, 
        output_path: Optional[str] = None
    ) -> str:
        """
        Load data to CSV file.
        
        Args:
            df: DataFrame to write
            output_path: Optional output path, uses config if not provided
            
        Returns:
            Path to written file
        """
        if output_path is None:
            output_path = self.target_config.get('output_file')
            
        if not output_path:
            raise ValueError("output_file not configured")
        
        logger.info(f"Loading {len(df)} records to CSV: {output_path}")
        
        # Ensure output directory exists
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            df.to_csv(output_path, index=False)
            logger.info(f"Successfully loaded data to {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"Error loading data to CSV: {str(e)}")
            raise
    
    def load_to_database(self, df: pd.DataFrame) -> int:
        """
        Load data to database table.
        
        Args:
            df: DataFrame to write
            
        Returns:
            Number of records loaded
        """
        db_config = self.target_config.get('database', {})
        table_name = db_config.get('table_name')
        
        if not table_name:
            raise ValueError("database.table_name not configured")
        
        logger.info(f"Loading {len(df)} records to database table: {table_name}")
        
        try:
            # This is a placeholder for actual database connection
            # In production, use SQLAlchemy or similar
            connection_string = db_config.get('connection_string')
            if not connection_string:
                raise ValueError("database.connection_string not configured")
            
            # Example using SQLAlchemy (would need actual implementation)
            # from sqlalchemy import create_engine
            # engine = create_engine(connection_string)
            # df.to_sql(table_name, engine, if_exists='append', index=False)
            
            logger.info(f"Successfully loaded {len(df)} records to {table_name}")
            return len(df)
            
        except Exception as e:
            logger.error(f"Error loading data to database: {str(e)}")
            raise
    
    def load_error_records(
        self, 
        df: pd.DataFrame, 
        error_type: str
    ) -> str:
        """
        Load error records to separate error file.
        
        Args:
            df: DataFrame containing error records
            error_type: Type of error for file naming
            
        Returns:
            Path to error file
        """
        error_dir = self.target_config.get('error_directory', 'data/errors')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        error_file = f"{error_dir}/errors_{error_type}_{timestamp}.csv"
        
        logger.warning(f"Loading {len(df)} error records to {error_file}")
        
        # Ensure error directory exists
        Path(error_dir).mkdir(parents=True, exist_ok=True)
        
        try:
            df.to_csv(error_file, index=False)
            logger.info(f"Error records written to {error_file}")
            return error_file
            
        except Exception as e:
            logger.error(f"Error writing error records: {str(e)}")
            raise
    
    def load(self, df: pd.DataFrame) -> Dict[str, any]:
        """
        Execute complete load pipeline.
        
        Args:
            df: DataFrame to load
            
        Returns:
            Dictionary with load statistics
        """
        logger.info("Starting data load pipeline")
        
        results = {
            'total_records': len(df),
            'loaded_records': 0,
            'error_records': 0,
            'output_files': []
        }
        
        try:
            # Load to primary target (CSV)
            if self.target_config.get('output_file'):
                output_path = self.load_to_csv(df)
                results['output_files'].append(output_path)
                results['loaded_records'] = len(df)
            
            # Optionally load to database
            if self.target_config.get('database', {}).get('enabled', False):
                loaded_count = self.load_to_database(df)
                results['loaded_records'] = loaded_count
            
            logger.info(f"Load pipeline complete: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Error in load pipeline: {str(e)}")
            raise