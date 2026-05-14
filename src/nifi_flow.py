"""
NiFi flow orchestration for customer address data migration.
Uses nipyapi to create and manage NiFi processor groups and processors.
"""
import logging
from typing import Dict, Optional
import nipyapi
from nipyapi.nifi import ProcessGroupsApi, ProcessorsApi, ControllerServicesApi
from nipyapi.canvas import get_process_group, create_process_group

logger = logging.getLogger(__name__)


class AddressDataNiFiFlow:
    """Manages NiFi flow for address data processing."""
    
    def __init__(self, config: Dict):
        """
        Initialize NiFi flow manager.
        
        Args:
            config: Configuration dictionary with NiFi settings
        """
        self.config = config
        self.nifi_config = config.get('nifi', {})
        self.flow_name = 'CustomerAddressDataFlow'
        self.process_group = None
        
        # Configure nipyapi
        nipyapi.config.nifi_config.host = self.nifi_config.get(
            'host', 
            'http://localhost:8080/nifi-api'
        )
        
    def create_process_group(self, parent_id: Optional[str] = None) -> Dict:
        """
        Create main process group for address data flow.
        
        Args:
            parent_id: Parent process group ID, uses root if None
            
        Returns:
            Process group object
        """
        logger.info(f"Creating process group: {self.flow_name}")
        
        try:
            if parent_id is None:
                root_pg = nipyapi.canvas.get_root_pg_id()
                parent_id = root_pg
            
            pg = nipyapi.canvas.create_process_group(
                parent_id,
                self.flow_name,
                location=(400.0, 400.0)
            )
            
            self.process_group = pg
            logger.info(f"Process group created: {pg.id}")
            return pg
            
        except Exception as e:
            logger.error(f"Error creating process group: {str(e)}")
            raise
    
    def create_extract_processor(self) -> Dict:
        """
        Create processor for data extraction.
        
        Returns:
            Processor object
        """
        logger.info("Creating extract processor")
        
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=self.process_group,
                processor=nipyapi.canvas.get_processor_type('GetFile'),
                location=(400.0, 200.0),
                name='ExtractCustomerData',
                config=nipyapi.nifi.ProcessorConfigDTO(
                    properties={
                        'Input Directory': self.config['source'].get('input_directory', '/data/input'),
                        'File Filter': self.config['source'].get('file_pattern', '.*\\.csv'),
                        'Keep Source File': 'false',
                        'Batch Size': '10'
                    },
                    auto_terminated_relationships=['not.found']
                )
            )
            
            logger.info(f"Extract processor created: {processor.id}")
            return processor
            
        except Exception as e:
            logger.error(f"Error creating extract processor: {str(e)}")
            raise
    
    def create_transform_processor(self) -> Dict:
        """
        Create processor for data transformation using ExecuteScript.
        
        Returns:
            Processor object
        """
        logger.info("Creating transform processor")
        
        # Python script for transformation
        transform_script = """
import json
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import StreamCallback

class TransformCallback(StreamCallback):
    def __init__(self):
        pass
        
    def process(self, inputStream, outputStream):
        text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
        # Transformation logic would go here
        # This is a placeholder for the actual Python transformation
        outputStream.write(text.encode('utf-8'))

flowFile = session.get()
if flowFile is not None:
    flowFile = session.write(flowFile, TransformCallback())
    session.transfer(flowFile, REL_SUCCESS)
"""
        
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=self.process_group,
                processor=nipyapi.canvas.get_processor_type('ExecuteScript'),
                location=(400.0, 400.0),
                name='TransformAddressData',
                config=nipyapi.nifi.ProcessorConfigDTO(
                    properties={
                        'Script Engine': 'python',
                        'Script Body': transform_script,
                        'Module Directory': self.config.get('nifi', {}).get('module_directory', '')
                    },
                    auto_terminated_relationships=['failure']
                )
            )
            
            logger.info(f"Transform processor created: {processor.id}")
            return processor
            
        except Exception as e:
            logger.error(f"Error creating transform processor: {str(e)}")
            raise
    
    def create_load_processor(self) -> Dict:
        """
        Create processor for data loading.
        
        Returns:
            Processor object
        """
        logger.info("Creating load processor")
        
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=self.process_group,
                processor=nipyapi.canvas.get_processor_type('PutFile'),
                location=(400.0, 600.0),
                name='LoadTransformedData',
                config=nipyapi.nifi.ProcessorConfigDTO(
                    properties={
                        'Directory': self.config['target'].get('output_directory', '/data/output'),
                        'Conflict Resolution Strategy': 'replace',
                        'Create Missing Directories': 'true'
                    },
                    auto_terminated_relationships=['success', 'failure']
                )
            )
            
            logger.info(f"Load processor created: {processor.id}")
            return processor
            
        except Exception as e:
            logger.error(f"Error creating load processor: {str(e)}")
            raise
    
    def create_error_handler(self) -> Dict:
        """
        Create processor for error handling.
        
        Returns:
            Processor object
        """
        logger.info("Creating error handler processor")
        
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=self.process_group,
                processor=nipyapi.canvas.get_processor_type('PutFile'),
                location=(700.0, 400.0),
                name='ErrorHandler',
                config=nipyapi.nifi.ProcessorConfigDTO(
                    properties={
                        'Directory': self.config['target'].get('error_directory', '/data/errors'),
                        'Conflict Resolution Strategy': 'replace',
                        'Create Missing Directories': 'true'
                    },
                    auto_terminated_relationships=['success', 'failure']
                )
            )
            
            logger.info(f"Error handler created: {processor.id}")
            return processor
            
        except Exception as e:
            logger.error(f"Error creating error handler: {str(e)}")
            raise
    
    def connect_processors(
        self, 
        source: Dict, 
        destination: Dict, 
        relationships: list
    ):
        """
        Create connection between processors.
        
        Args:
            source: Source processor
            destination: Destination processor
            relationships: List of relationship names to connect
        """
        logger.info(f"Connecting processors: {source.id} -> {destination.id}")
        
        try:
            for relationship in relationships:
                connection = nipyapi.canvas.create_connection(
                    source,
                    destination,
                    relationships=[relationship]
                )
                logger.debug(f"Created connection for relationship: {relationship}")
            
            logger.info("Processors connected successfully")
            
        except Exception as e:
            logger.error(f"Error connecting processors: {str(e)}")
            raise
    
    def build_flow(self) -> Dict:
        """
        Build complete NiFi flow for address data processing.
        
        Returns:
            Dictionary with flow components
        """
        logger.info("Building complete NiFi flow")
        
        try:
            # Create process group
            pg = self.create_process_group()
            
            # Create processors
            extract_proc = self.create_extract_processor()
            transform_proc = self.create_transform_processor()
            load_proc = self.create_load_processor()
            error_proc = self.create_error_handler()
            
            # Connect processors
            self.connect_processors(extract_proc, transform_proc, ['success'])
            self.connect_processors(transform_proc, load_proc, ['success'])
            self.connect_processors(transform_proc, error_proc, ['failure'])
            
            flow_components = {
                'process_group': pg,
                'extract_processor': extract_proc,
                'transform_processor': transform_proc,
                'load_processor': load_proc,
                'error_processor': error_proc
            }
            
            logger.info("NiFi flow built successfully")
            return flow_components
            
        except Exception as e:
            logger.error(f"Error building NiFi flow: {str(e)}")
            raise
    
    def start_flow(self):
        """Start all processors in the flow."""
        logger.info("Starting NiFi flow")
        
        try:
            if self.process_group:
                nipyapi.canvas.schedule_process_group(
                    self.process_group.id,
                    scheduled=True
                )
                logger.info("Flow started successfully")
            else:
                raise ValueError("Process group not created")
                
        except Exception as e:
            logger.error(f"Error starting flow: {str(e)}")
            raise
    
    def stop_flow(self):
        """Stop all processors in the flow."""
        logger.info("Stopping NiFi flow")
        
        try:
            if self.process_group:
                nipyapi.canvas.schedule_process_group(
                    self.process_group.id,
                    scheduled=False
                )
                logger.info("Flow stopped successfully")
            else:
                raise ValueError("Process group not created")
                
        except Exception as e:
            logger.error(f"Error stopping flow: {str(e)}")
            raise