"""
Load module for customer master data.
Handles writing transformed data to target destinations.
"""

import logging
import pandas as pd
from pathlib import Path
from typing import Dict, Any, Optional
import yaml
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CustomerDataLoader:
    """Load transformed customer data to target destinations."""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize loader with configuration."""
        self.config = self._load_config(config_path)
        self.target_config = self.config.get('target', {})
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load config from {config_path}: {e}")
            raise
    
    def load_customers(self, df: pd.DataFrame, output_path: Optional[str] = None) -> bool:
        """
        Load customer data to target destination.
        
        Args:
            df: Transformed customer DataFrame
            output_path: Path to output file. If None, uses config.
            
        Returns:
            True if load successful
        """
        if output_path is None:
            output_path = self.target_config.get('customers_output')
        
        if not output_path:
            raise ValueError("Output path not provided")
        
        logger.info(f"Loading {len(df)} customer records to {output_path}")
        
        try:
            # Create output directory if it doesn't exist
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            
            # Determine output format
            output_format = self.target_config.get('format', 'csv').lower()
            
            if output_format == 'csv':
                self._load_to_csv(df, output_path)
            elif output_format == 'parquet':
                self._load_to_parquet(df, output_path)
            elif output_format == 'json':
                self._load_to_json(df, output_path)
            else:
                raise ValueError(f"Unsupported output format: {output_format}")
            
            logger.info(f"Successfully loaded customer data to {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error loading customer data: {e}")
            raise
    
    def _load_to_csv(self, df: pd.DataFrame, output_path: str) -> None:
        """Load data to CSV file."""
        df.to_csv(
            output_path,
            index=False,
            encoding=self.target_config.get('encoding', 'utf-8'),
            date_format=self.target_config.get('date_format', '%Y-%m-%d %H:%M:%S')
        )
    
    def _load_to_parquet(self, df: pd.DataFrame, output_path: str) -> None:
        """Load data to Parquet file."""
        df.to_parquet(
            output_path,
            index=False,
            compression=self.target_config.get('compression', 'snappy')
        )
    
    def _load_to_json(self, df: pd.DataFrame, output_path: str) -> None:
        """Load data to JSON file."""
        df.to_json(
            output_path,
            orient=self.target_config.get('json_orient', 'records'),
            date_format='iso',
            indent=2
        )
    
    def load_data_quality_report(self, df: pd.DataFrame, report_path: Optional[str] = None) -> bool:
        """
        Generate and load data quality report.
        
        Args:
            df: Transformed customer DataFrame
            report_path: Path to report file. If None, uses config.
            
        Returns:
            True if report generated successfully
        """
        if report_path is None:
            report_path = self.target_config.get('quality_report_output')
        
        if not report_path:
            logger.warning("Quality report path not configured, skipping report generation")
            return False
        
        logger.info(f"Generating data quality report to {report_path}")
        
        try:
            # Create output directory if it doesn't exist
            Path(report_path).parent.mkdir(parents=True, exist_ok=True)
            
            report = self._generate_quality_report(df)
            
            with open(report_path, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            
            logger.info(f"Successfully generated quality report at {report_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error generating quality report: {e}")
            raise
    
    def _generate_quality_report(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate data quality metrics report."""
        report = {
            'summary': {
                'total_records': len(df),
                'timestamp': pd.Timestamp.now().isoformat()
            },
            'completeness': {},
            'validity': {},
            'quality_scores': {}
        }
        
        # Completeness metrics
        for col in df.columns:
            null_count = df[col].isna().sum()
            null_pct = (null_count / len(df)) * 100 if len(df) > 0 else 0
            report['completeness'][col] = {
                'null_count': int(null_count),
                'null_percentage': round(null_pct, 2),
                'complete_count': int(len(df) - null_count)
            }
        
        # Validity metrics
        if 'email_valid' in df.columns:
            valid_count = df['email_valid'].sum()
            report['validity']['email'] = {
                'valid_count': int(valid_count),
                'valid_percentage': round((valid_count / len(df)) * 100, 2) if len(df) > 0 else 0
            }
        
        if 'state_valid' in df.columns:
            valid_count = df['state_valid'].sum()
            report['validity']['state'] = {
                'valid_count': int(valid_count),
                'valid_percentage': round((valid_count / len(df)) * 100, 2) if len(df) > 0 else 0
            }
        
        # Quality score distribution
        if 'data_quality_score' in df.columns:
            report['quality_scores'] = {
                'mean': round(df['data_quality_score'].mean(), 2),
                'median': round(df['data_quality_score'].median(), 2),
                'min': round(df['data_quality_score'].min(), 2),
                'max': round(df['data_quality_score'].max(), 2),
                'std': round(df['data_quality_score'].std(), 2)
            }
        
        # Status distribution
        if 'status' in df.columns:
            status_counts = df['status'].value_counts().to_dict()
            report['status_distribution'] = {
                str(k): int(v) for k, v in status_counts.items()
            }
        
        return report
    
    def load_error_records(self, df: pd.DataFrame, error_path: Optional[str] = None) -> bool:
        """
        Load records that failed validation to error file.
        
        Args:
            df: DataFrame containing error records
            error_path: Path to error file. If None, uses config.
            
        Returns:
            True if errors loaded successfully
        """
        if df.empty:
            logger.info("No error records to load")
            return True
        
        if error_path is None:
            error_path = self.target_config.get('error_output')
        
        if not error_path:
            logger.warning("Error output path not configured, skipping error file generation")
            return False
        
        logger.info(f"Loading {len(df)} error records to {error_path}")
        
        try:
            # Create output directory if it doesn't exist
            Path(error_path).parent.mkdir(parents=True, exist_ok=True)
            
            df.to_csv(
                error_path,
                index=False,
                encoding=self.target_config.get('encoding', 'utf-8')
            )
            
            logger.info(f"Successfully loaded error records to {error_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error loading error records: {e}")
            raise