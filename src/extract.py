"""
Extract module for customer and address data ingestion.
Handles reading from multiple sources and initial data loading.
"""

import logging
from typing import Dict, List, Any, Optional
from pathlib import Path
import nipyapi
from nipyapi.nifi import ProcessorConfigDTO, ProcessGroupEntity
import yaml

logger = logging.getLogger(__name__)


class AddressDataExtractor:
    """Extracts customer and address data from source systems."""
    
    def __init__(self, config: Dict[str, Any], canvas: Any):
        """
        Initialize the extractor.
        
        Args:
            config: Configuration dictionary
            canvas: NiFi canvas object
        """
        self.config = config
        self.canvas = canvas
        self.sources = config.get('sources', {})
        
    def create_extraction_flow(self, process_group: ProcessGroupEntity) -> Dict[str, Any]:
        """
        Create the extraction flow in NiFi.
        
        Args:
            process_group: Parent process group
            
        Returns:
            Dictionary containing processor references
        """
        logger.info("Creating extraction flow for customer and address data")
        
        processors = {}
        
        try:
            # Create customer data extractor
            processors['customer_reader'] = self._create_file_reader(
                process_group,
                "Read Customer Data",
                self.sources['customers']['path'],
                position={'x': 100, 'y': 100}
            )
            
            # Create address data extractor
            processors['address_reader'] = self._create_file_reader(
                process_group,
                "Read Address Data",
                self.sources['addresses']['path'],
                position={'x': 100, 'y': 300}
            )
            
            # Create schema validation processors
            processors['validate_customer_schema'] = self._create_schema_validator(
                process_group,
                "Validate Customer Schema",
                self.sources['customers']['schema'],
                position={'x': 400, 'y': 100}
            )
            
            processors['validate_address_schema'] = self._create_schema_validator(
                process_group,
                "Validate Address Schema",
                self.sources['addresses']['schema'],
                position={'x': 400, 'y': 300}
            )
            
            # Create connections
            self._create_connection(
                process_group,
                processors['customer_reader'],
                processors['validate_customer_schema'],
                ['success']
            )
            
            self._create_connection(
                process_group,
                processors['address_reader'],
                processors['validate_address_schema'],
                ['success']
            )
            
            logger.info("Extraction flow created successfully")
            return processors
            
        except Exception as e:
            logger.error(f"Failed to create extraction flow: {str(e)}")
            raise
    
    def _create_file_reader(
        self,
        process_group: ProcessGroupEntity,
        name: str,
        input_path: str,
        position: Dict[str, int]
    ) -> Any:
        """Create a GetFile processor for reading source data."""
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=process_group,
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.GetFile'),
            location=(position['x'], position['y']),
            name=name,
            config=ProcessorConfigDTO(
                properties={
                    'Input Directory': input_path,
                    'File Filter': '[^\\.].*\\.csv',
                    'Keep Source File': 'false',
                    'Recurse Subdirectories': 'false',
                    'Polling Interval': '10 sec',
                    'Batch Size': '100',
                    'Ignore Hidden Files': 'true'
                },
                auto_terminated_relationships=['not.found'],
                scheduling_period='30 sec',
                scheduling_strategy='TIMER_DRIVEN',
                execution_node='ALL'
            )
        )
        
        logger.info(f"Created file reader processor: {name}")
        return processor
    
    def _create_schema_validator(
        self,
        process_group: ProcessGroupEntity,
        name: str,
        schema: List[Dict[str, str]],
        position: Dict[str, int]
    ) -> Any:
        """Create a ValidateRecord processor for schema validation."""
        
        # Create Avro schema from config
        avro_schema = self._generate_avro_schema(schema)
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=process_group,
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.ValidateRecord'),
            location=(position['x'], position['y']),
            name=name,
            config=ProcessorConfigDTO(
                properties={
                    'Record Reader': 'CSVReader',
                    'Record Writer': 'CSVRecordSetWriter',
                    'Schema Access Strategy': 'Use String Schema Property',
                    'Schema Text': avro_schema,
                    'Allow Extra Fields': 'true',
                    'Strict Type Checking': 'true'
                },
                auto_terminated_relationships=[],
                scheduling_period='0 sec',
                scheduling_strategy='EVENT_DRIVEN'
            )
        )
        
        logger.info(f"Created schema validator processor: {name}")
        return processor
    
    def _generate_avro_schema(self, schema: List[Dict[str, str]]) -> str:
        """Generate Avro schema from configuration."""
        
        fields = []
        for field_def in schema:
            for field_name, field_type in field_def.items():
                avro_type = self._map_to_avro_type(field_type)
                fields.append({
                    'name': field_name,
                    'type': ['null', avro_type]
                })
        
        avro_schema = {
            'type': 'record',
            'name': 'Record',
            'fields': fields
        }
        
        import json
        return json.dumps(avro_schema)
    
    def _map_to_avro_type(self, field_type: str) -> str:
        """Map configuration type to Avro type."""
        
        type_mapping = {
            'string': 'string',
            'integer': 'int',
            'long': 'long',
            'float': 'float',
            'double': 'double',
            'boolean': 'boolean',
            'timestamp': 'long',
            'date': 'int'
        }
        
        return type_mapping.get(field_type.lower(), 'string')
    
    def _create_connection(
        self,
        process_group: ProcessGroupEntity,
        source: Any,
        destination: Any,
        relationships: List[str]
    ) -> Any:
        """Create a connection between processors."""
        
        connection = nipyapi.canvas.create_connection(
            source=source,
            target=destination,
            relationships=relationships
        )
        
        logger.debug(f"Created connection from {source.id} to {destination.id}")
        return connection