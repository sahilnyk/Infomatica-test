"""
Transaction data extraction module.
Handles reading transaction data from various sources with error handling.
"""

import logging
from typing import Dict, List, Optional, Any
from pathlib import Path
import csv
import json
from datetime import datetime
import nipyapi
from nipyapi.nifi import ProcessorConfigDTO

logger = logging.getLogger(__name__)


class TransactionExtractor:
    """Extract transaction data from source systems."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the extractor.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.source_config = config['sources']
        
    def create_extraction_processors(
        self, 
        process_group_id: str,
        canvas: nipyapi.nifi.ProcessGroupFlowEntity
    ) -> Dict[str, Any]:
        """
        Create NiFi processors for data extraction.
        
        Args:
            process_group_id: Parent process group ID
            canvas: NiFi canvas object
            
        Returns:
            Dictionary of created processors
        """
        processors = {}
        
        try:
            # Create GetFile processor for transaction data
            get_transactions = self._create_get_file_processor(
                process_group_id=process_group_id,
                name="Get_Transaction_Files",
                config=self.source_config['transactions'],
                position=(100, 100)
            )
            processors['get_transactions'] = get_transactions
            
            # Create GetFile processor for reference data
            get_reference = self._create_get_file_processor(
                process_group_id=process_group_id,
                name="Get_Reference_Data",
                config=self.source_config['reference_data'],
                position=(100, 300)
            )
            processors['get_reference'] = get_reference
            
            # Create SplitRecord processor for transaction batching
            split_transactions = self._create_split_record_processor(
                process_group_id=process_group_id,
                name="Split_Transaction_Batches",
                position=(300, 100)
            )
            processors['split_transactions'] = split_transactions
            
            # Create UpdateAttribute for metadata enrichment
            enrich_metadata = self._create_update_attribute_processor(
                process_group_id=process_group_id,
                name="Enrich_Transaction_Metadata",
                position=(500, 100)
            )
            processors['enrich_metadata'] = enrich_metadata
            
            logger.info("Successfully created extraction processors")
            return processors
            
        except Exception as e:
            logger.error(f"Error creating extraction processors: {str(e)}")
            raise
    
    def _create_get_file_processor(
        self,
        process_group_id: str,
        name: str,
        config: Dict[str, Any],
        position: tuple
    ) -> nipyapi.nifi.ProcessorEntity:
        """Create a GetFile processor."""
        processor_config = ProcessorConfigDTO()
        processor_config.properties = {
            'Input Directory': config['path'],
            'File Filter': config['pattern'],
            'Keep Source File': 'false',
            'Recurse Subdirectories': 'true',
            'Polling Interval': '10 sec',
            'Batch Size': '10',
            'Ignore Hidden Files': 'true'
        }
        processor_config.scheduling_period = '30 sec'
        processor_config.auto_terminated_relationships = ['not.found']
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(process_group_id, 'id'),
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.GetFile'),
            location=(position[0], position[1]),
            name=name,
            config=processor_config
        )
        
        logger.info(f"Created GetFile processor: {name}")
        return processor
    
    def _create_split_record_processor(
        self,
        process_group_id: str,
        name: str,
        position: tuple
    ) -> nipyapi.nifi.ProcessorEntity:
        """Create a SplitRecord processor for batching."""
        processor_config = ProcessorConfigDTO()
        processor_config.properties = {
            'Record Reader': 'CSVReader',
            'Record Writer': 'CSVRecordSetWriter',
            'Records Per Split': str(self.config['validation']['reconciliation']['batch_size']),
            'Fragment Count Attribute': 'fragment.count',
            'Fragment Index Attribute': 'fragment.index',
            'Fragment Identifier Attribute': 'fragment.identifier'
        }
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(process_group_id, 'id'),
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.SplitRecord'),
            location=(position[0], position[1]),
            name=name,
            config=processor_config
        )
        
        logger.info(f"Created SplitRecord processor: {name}")
        return processor
    
    def _create_update_attribute_processor(
        self,
        process_group_id: str,
        name: str,
        position: tuple
    ) -> nipyapi.nifi.ProcessorEntity:
        """Create an UpdateAttribute processor for metadata enrichment."""
        processor_config = ProcessorConfigDTO()
        processor_config.properties = {
            'extraction.timestamp': '${now():format("yyyy-MM-dd HH:mm:ss")}',
            'source.filename': '${filename}',
            'source.path': '${absolute.path}',
            'processing.id': '${UUID()}',
            'batch.id': '${fragment.identifier}',
            'batch.index': '${fragment.index}',
            'batch.count': '${fragment.count}'
        }
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(process_group_id, 'id'),
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.attributes.UpdateAttribute'),
            location=(position[0], position[1]),
            name=name,
            config=processor_config
        )
        
        logger.info(f"Created UpdateAttribute processor: {name}")
        return processor
    
    def extract_local_data(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Extract data from local file for testing.
        
        Args:
            file_path: Path to the data file
            
        Returns:
            List of transaction records
        """
        transactions = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    transactions.append(row)
            
            logger.info(f"Extracted {len(transactions)} transactions from {file_path}")
            return transactions
            
        except Exception as e:
            logger.error(f"Error extracting data from {file_path}: {str(e)}")
            raise
    
    def validate_source_schema(self, data: List[Dict[str, Any]]) -> bool:
        """
        Validate that source data has required fields.
        
        Args:
            data: List of transaction records
            
        Returns:
            True if schema is valid
        """
        required_fields = [
            'transaction_id',
            'transaction_date',
            'transaction_type',
            'amount',
            'currency',
            'status'
        ]
        
        if not data:
            logger.warning("No data to validate")
            return False
        
        first_record = data[0]
        missing_fields = [field for field in required_fields if field not in first_record]
        
        if missing_fields:
            logger.error(f"Missing required fields: {missing_fields}")
            return False
        
        logger.info("Source schema validation passed")
        return True