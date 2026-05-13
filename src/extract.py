"""
Extract module for transaction data processing pipeline.
Handles reading transaction data from source systems with proper connection management.
"""

import logging
from typing import Dict, Any, Optional
import nipyapi
from nipyapi.nifi import ProcessorConfigDTO, ProcessGroupsApi
from nipyapi.canvas import get_process_group, create_processor

logger = logging.getLogger(__name__)


class TransactionExtractor:
    """Handles extraction of transaction data from source systems."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the transaction extractor.
        
        Args:
            config: Configuration dictionary containing source connection details
        """
        self.config = config
        self.source_config = config.get('source', {})
        
    def create_extraction_flow(self, canvas: Any, parent_pg: Any) -> Dict[str, Any]:
        """
        Create NiFi processors for transaction data extraction.
        
        Args:
            canvas: NiFi canvas object
            parent_pg: Parent process group
            
        Returns:
            Dictionary containing created processor references
        """
        logger.info("Creating transaction extraction flow")
        
        processors = {}
        
        try:
            # Create GetFile processor for transaction source
            get_file_config = {
                'Input Directory': self.source_config.get('input_directory', '/data/input/transactions'),
                'File Filter': self.source_config.get('file_filter', 'transactions_.*\\.csv'),
                'Keep Source File': 'false',
                'Recurse Subdirectories': 'false',
                'Polling Interval': self.source_config.get('polling_interval', '10 sec'),
                'Batch Size': str(self.source_config.get('batch_size', 10)),
                'Ignore Hidden Files': 'true'
            }
            
            get_file = create_processor(
                parent_pg=parent_pg,
                processor=canvas.get_processor_type('GetFile'),
                location=(100, 100),
                name='Extract_Transaction_Files',
                config=ProcessorConfigDTO(
                    properties=get_file_config,
                    auto_terminated_relationships=['not.found'],
                    scheduling_period=self.source_config.get('scheduling_period', '30 sec'),
                    scheduling_strategy='TIMER_DRIVEN',
                    execution_node='ALL'
                )
            )
            processors['get_file'] = get_file
            logger.info(f"Created GetFile processor: {get_file.id}")
            
            # Create UpdateAttribute for metadata enrichment
            update_attr_config = {
                'source_system': self.source_config.get('source_system', 'TRANSACTION_DB'),
                'extraction_timestamp': '${now():format("yyyy-MM-dd HH:mm:ss")}',
                'file_name': '${filename}',
                'file_size': '${fileSize}',
                'processing_date': '${now():format("yyyy-MM-dd")}'
            }
            
            update_attr = create_processor(
                parent_pg=parent_pg,
                processor=canvas.get_processor_type('UpdateAttribute'),
                location=(100, 250),
                name='Enrich_Transaction_Metadata',
                config=ProcessorConfigDTO(
                    properties=update_attr_config,
                    scheduling_period='0 sec',
                    scheduling_strategy='EVENT_DRIVEN'
                )
            )
            processors['update_attr'] = update_attr
            logger.info(f"Created UpdateAttribute processor: {update_attr.id}")
            
            # Create ValidateRecord for schema validation
            validate_config = {
                'Record Reader': self.source_config.get('record_reader_service', 'CSVReader'),
                'Record Writer': self.source_config.get('record_writer_service', 'CSVRecordSetWriter'),
                'Schema Access Strategy': 'Use Schema Name Property',
                'Schema Registry': self.source_config.get('schema_registry_service', 'AvroSchemaRegistry'),
                'Schema Name': '${schema.name}',
                'Allow Extra Fields': 'true',
                'Strict Type Checking': 'true'
            }
            
            validate_record = create_processor(
                parent_pg=parent_pg,
                processor=canvas.get_processor_type('ValidateRecord'),
                location=(100, 400),
                name='Validate_Transaction_Schema',
                config=ProcessorConfigDTO(
                    properties=validate_config,
                    auto_terminated_relationships=['failure'],
                    scheduling_period='0 sec',
                    scheduling_strategy='EVENT_DRIVEN'
                )
            )
            processors['validate_record'] = validate_record
            logger.info(f"Created ValidateRecord processor: {validate_record.id}")
            
            # Create RouteOnAttribute for error handling
            route_config = {
                'Routing Strategy': 'Route to Property name',
                'valid_transaction': "${record.count:gt(0):and(${fileSize:gt(0)})}",
                'empty_file': "${fileSize:equals(0)}",
                'invalid_format': "${mime.type:equals('application/octet-stream')}"
            }
            
            route_attr = create_processor(
                parent_pg=parent_pg,
                processor=canvas.get_processor_type('RouteOnAttribute'),
                location=(100, 550),
                name='Route_Transaction_Quality',
                config=ProcessorConfigDTO(
                    properties=route_config,
                    auto_terminated_relationships=['unmatched', 'empty_file', 'invalid_format'],
                    scheduling_period='0 sec',
                    scheduling_strategy='EVENT_DRIVEN'
                )
            )
            processors['route_attr'] = route_attr
            logger.info(f"Created RouteOnAttribute processor: {route_attr.id}")
            
            return processors
            
        except Exception as e:
            logger.error(f"Error creating extraction flow: {str(e)}")
            raise
    
    def create_connections(self, processors: Dict[str, Any]) -> None:
        """
        Create connections between extraction processors.
        
        Args:
            processors: Dictionary of processor references
        """
        try:
            # Connect GetFile to UpdateAttribute
            nipyapi.canvas.create_connection(
                source=processors['get_file'],
                target=processors['update_attr'],
                relationships=['success']
            )
            
            # Connect UpdateAttribute to ValidateRecord
            nipyapi.canvas.create_connection(
                source=processors['update_attr'],
                target=processors['validate_record'],
                relationships=['success']
            )
            
            # Connect ValidateRecord to RouteOnAttribute
            nipyapi.canvas.create_connection(
                source=processors['validate_record'],
                target=processors['route_attr'],
                relationships=['valid']
            )
            
            logger.info("Successfully created extraction flow connections")
            
        except Exception as e:
            logger.error(f"Error creating connections: {str(e)}")
            raise


def setup_extraction_controller_services(canvas: Any, parent_pg: Any, config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Setup controller services required for extraction.
    
    Args:
        canvas: NiFi canvas object
        parent_pg: Parent process group
        config: Configuration dictionary
        
    Returns:
        Dictionary of controller service references
    """
    services = {}
    
    try:
        # CSV Reader Service
        csv_reader_props = {
            'Schema Access Strategy': 'Use String Fields From Header',
            'CSV Format': 'Custom Format',
            'Value Separator': config.get('csv_delimiter', ','),
            'Skip Header Line': 'true',
            'Quote Character': '"',
            'Escape Character': '\\',
            'Comment Marker': '#',
            'Null String': '',
            'Trim Fields': 'true',
            'Charset': 'UTF-8',
            'Date Format': config.get('date_format', 'yyyy-MM-dd'),
            'Time Format': config.get('time_format', 'HH:mm:ss'),
            'Timestamp Format': config.get('timestamp_format', 'yyyy-MM-dd HH:mm:ss')
        }
        
        csv_reader = nipyapi.canvas.create_controller_service(
            parent_pg=parent_pg,
            service_type='org.apache.nifi.csv.CSVReader',
            name='Transaction_CSV_Reader',
            properties=csv_reader_props
        )
        services['csv_reader'] = csv_reader
        logger.info(f"Created CSV Reader service: {csv_reader.id}")
        
        # CSV Writer Service
        csv_writer_props = {
            'Schema Write Strategy': 'Set Schema Name',
            'Schema Access Strategy': 'Inherit Record Schema',
            'CSV Format': 'Custom Format',
            'Value Separator': ',',
            'Include Header Line': 'true',
            'Quote Character': '"',
            'Escape Character': '\\',
            'Comment Marker': '',
            'Null String': '',
            'Trim Fields': 'true',
            'Charset': 'UTF-8',
            'Date Format': 'yyyy-MM-dd',
            'Time Format': 'HH:mm:ss',
            'Timestamp Format': 'yyyy-MM-dd HH:mm:ss'
        }
        
        csv_writer = nipyapi.canvas.create_controller_service(
            parent_pg=parent_pg,
            service_type='org.apache.nifi.csv.CSVRecordSetWriter',
            name='Transaction_CSV_Writer',
            properties=csv_writer_props
        )
        services['csv_writer'] = csv_writer
        logger.info(f"Created CSV Writer service: {csv_writer.id}")
        
        # Enable services
        for service_name, service in services.items():
            nipyapi.canvas.schedule_controller_service(service, scheduled=True)
            logger.info(f"Enabled controller service: {service_name}")
        
        return services
        
    except Exception as e:
        logger.error(f"Error setting up controller services: {str(e)}")
        raise