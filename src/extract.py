"""
Extract module for customer master data quality validation.
Handles data extraction from NiFi flow files.
"""
import json
import logging
from typing import Dict, Any, List
from datetime import datetime

logger = logging.getLogger(__name__)


class CustomerDataExtractor:
    """Extract customer master data from NiFi flow files."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize extractor with configuration.
        
        Args:
            config: Configuration dictionary containing extraction settings
        """
        self.config = config
        self.source_fields = config.get('source_fields', {})
        
    def extract_customer_data(self, flowfile_content: str) -> Dict[str, Any]:
        """
        Extract customer data from flow file content.
        
        Args:
            flowfile_content: Raw content from NiFi flow file
            
        Returns:
            Dictionary containing extracted customer data
            
        Raises:
            ValueError: If content cannot be parsed
        """
        try:
            if isinstance(flowfile_content, str):
                data = json.loads(flowfile_content)
            else:
                data = flowfile_content
                
            customer_record = {
                'customer_id': data.get('customer_id', '').strip(),
                'first_name': data.get('first_name', '').strip(),
                'last_name': data.get('last_name', '').strip(),
                'email': data.get('email', '').strip(),
                'phone': data.get('phone', '').strip(),
                'address_line1': data.get('address_line1', '').strip(),
                'address_line2': data.get('address_line2', '').strip(),
                'city': data.get('city', '').strip(),
                'state': data.get('state', '').strip(),
                'zip_code': data.get('zip_code', '').strip(),
                'country': data.get('country', '').strip(),
                'registration_date': data.get('registration_date', ''),
                'status': data.get('status', '').strip(),
                'extraction_timestamp': datetime.utcnow().isoformat()
            }
            
            logger.info(f"Extracted customer record: {customer_record.get('customer_id')}")
            return customer_record
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON content: {e}")
            raise ValueError(f"Invalid JSON format: {e}")
        except Exception as e:
            logger.error(f"Extraction error: {e}")
            raise
            
    def extract_address_data(self, flowfile_content: str) -> Dict[str, Any]:
        """
        Extract customer address data from flow file content.
        
        Args:
            flowfile_content: Raw content from NiFi flow file
            
        Returns:
            Dictionary containing extracted address data
        """
        try:
            if isinstance(flowfile_content, str):
                data = json.loads(flowfile_content)
            else:
                data = flowfile_content
                
            address_record = {
                'address_id': data.get('address_id', '').strip(),
                'customer_id': data.get('customer_id', '').strip(),
                'address_type': data.get('address_type', '').strip(),
                'extraction_timestamp': datetime.utcnow().isoformat()
            }
            
            logger.info(f"Extracted address record: {address_record.get('address_id')}")
            return address_record
            
        except Exception as e:
            logger.error(f"Address extraction error: {e}")
            raise
            
    def extract_batch(self, flowfiles: List[Any]) -> List[Dict[str, Any]]:
        """
        Extract data from multiple flow files.
        
        Args:
            flowfiles: List of NiFi flow files
            
        Returns:
            List of extracted customer records
        """
        extracted_records = []
        
        for flowfile in flowfiles:
            try:
                content = flowfile.get('content', '')
                record = self.extract_customer_data(content)
                extracted_records.append(record)
            except Exception as e:
                logger.error(f"Failed to extract flowfile: {e}")
                continue
                
        logger.info(f"Extracted {len(extracted_records)} records from {len(flowfiles)} flowfiles")
        return extracted_records