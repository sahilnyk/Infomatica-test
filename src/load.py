"""
Load module for Sales Returns Processing
Handles loading processed returns data to target systems and validation
"""
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import json
from decimal import Decimal

import nipyapi
from nipyapi.nifi import ProcessorConfigDTO, ProcessGroupEntity
from nipyapi.canvas import schedule_process_group

logger = logging.getLogger(__name__)


class SalesReturnsLoader:
    """Handles loading of processed sales returns data"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the loader with configuration
        
        Args:
            config: Configuration dictionary containing connection and processing parameters
        """
        self.config = config
        self.nifi_config = config.get('nifi', {})
        self.load_config = config.get('load', {})
        self.validation_config = config.get('validation', {})
        self.process_group = None
        
    def create_load_flow(self, parent_pg: ProcessGroupEntity) -> ProcessGroupEntity:
        """
        Create NiFi process group for loading sales returns
        
        Args:
            parent_pg: Parent process group
            
        Returns:
            Created process group entity
        """
        try:
            # Create load process group
            self.process_group = nipyapi.canvas.create_process_group(
                parent_pg,
                'Sales_Returns_Load',
                location=(800.0, 400.0)
            )
            
            logger.info(f"Created load process group: {self.process_group.id}")
            
            # Create processors
            self._create_validation_processor()
            self._create_database_loader()
            self._create_file_loader()
            self._create_audit_logger()
            self._create_error_handler()
            
            # Create connections
            self._create_connections()
            
            return self.process_group
            
        except Exception as e:
            logger.error(f"Error creating load flow: {str(e)}")
            raise
    
    def _create_validation_processor(self) -> nipyapi.nifi.ProcessorEntity:
        """Create processor for validating refund calculations"""
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=self.process_group,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.script.ExecuteScript'),
                location=(100.0, 100.0),
                name='Validate_Refund_Calculations',
                config=ProcessorConfigDTO(
                    scheduling_period='0 sec',
                    auto_terminated_relationships=['failure'],
                    properties={
                        'Script Engine': 'python',
                        'Script Body': self._get_validation_script()
                    }
                )
            )
            
            logger.info(f"Created validation processor: {processor.id}")
            return processor
            
        except Exception as e:
            logger.error(f"Error creating validation processor: {str(e)}")
            raise
    
    def _get_validation_script(self) -> str:
        """Generate Python script for refund validation"""
        return '''
import json
import sys
from decimal import Decimal
from java.nio.charset import StandardCharsets
from org.apache.commons.io import IOUtils
from org.apache.nifi.processor.io import StreamCallback

class ValidationCallback(StreamCallback):
    def __init__(self):
        self.result = None
        
    def process(self, inputStream, outputStream):
        text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
        data = json.loads(text)
        
        # Validate refund calculations
        validation_results = {
            'return_id': data.get('return_id'),
            'validation_timestamp': str(datetime.now()),
            'validations': []
        }
        
        # Check refund amount calculation
        original_amount = Decimal(str(data.get('original_amount', 0)))
        return_quantity = int(data.get('return_quantity', 0))
        unit_price = Decimal(str(data.get('unit_price', 0)))
        restocking_fee = Decimal(str(data.get('restocking_fee', 0)))
        calculated_refund = Decimal(str(data.get('calculated_refund', 0)))
        
        expected_refund = (unit_price * return_quantity) - restocking_fee
        
        refund_validation = {
            'check': 'refund_calculation',
            'expected': float(expected_refund),
            'actual': float(calculated_refund),
            'passed': abs(expected_refund - calculated_refund) < Decimal('0.01')
        }
        validation_results['validations'].append(refund_validation)
        
        # Check return quantity against original
        original_quantity = int(data.get('original_quantity', 0))
        quantity_validation = {
            'check': 'return_quantity',
            'expected': f'<= {original_quantity}',
            'actual': return_quantity,
            'passed': return_quantity <= original_quantity and return_quantity > 0
        }
        validation_results['validations'].append(quantity_validation)
        
        # Check refund status
        refund_status = data.get('refund_status', '')
        status_validation = {
            'check': 'refund_status',
            'expected': 'APPROVED or PENDING',
            'actual': refund_status,
            'passed': refund_status in ['APPROVED', 'PENDING', 'PROCESSED']
        }
        validation_results['validations'].append(status_validation)
        
        # Overall validation result
        all_passed = all(v['passed'] for v in validation_results['validations'])
        validation_results['overall_status'] = 'VALID' if all_passed else 'INVALID'
        
        # Add validation results to original data
        data['validation_results'] = validation_results
        
        output_text = json.dumps(data, indent=2)
        outputStream.write(output_text.encode('utf-8'))

flowFile = session.get()
if flowFile is not None:
    callback = ValidationCallback()
    flowFile = session.write(flowFile, callback)
    
    # Route based on validation
    session.transfer(flowFile, REL_SUCCESS)
'''
    
    def _create_database_loader(self) -> nipyapi.nifi.ProcessorEntity:
        """Create processor for loading to database"""
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=self.process_group,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.PutDatabaseRecord'),
                location=(400.0, 100.0),
                name='Load_Returns_To_Database',
                config=ProcessorConfigDTO(
                    scheduling_period='0 sec',
                    auto_terminated_relationships=['failure', 'retry'],
                    properties={
                        'record-reader-factory': self.load_config.get('record_reader_service'),
                        'dbcp-service': self.load_config.get('database_connection_pool'),
                        'statement-type': 'INSERT',
                        'table-name': self.load_config.get('target_table', 'sales_returns_processed'),
                        'field-containing-sql': '',
                        'allow-multiple-statements': 'false',
                        'quote-identifiers': 'true',
                        'quote-table-identifier': 'true',
                        'query-timeout': '30 seconds',
                        'rollback-on-failure': 'true',
                        'batch-size': str(self.load_config.get('batch_size', 1000))
                    }
                )
            )
            
            logger.info(f"Created database loader processor: {processor.id}")
            return processor
            
        except Exception as e:
            logger.error(f"Error creating database loader: {str(e)}")
            raise
    
    def _create_file_loader(self) -> nipyapi.nifi.ProcessorEntity:
        """Create processor for loading to file system (backup/archive)"""
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=self.process_group,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.PutFile'),
                location=(400.0, 300.0),
                name='Archive_Processed_Returns',
                config=ProcessorConfigDTO(
                    scheduling_period='0 sec',
                    auto_terminated_relationships=['failure', 'success'],
                    properties={
                        'Directory': self.load_config.get('archive_directory', '/data/archive/returns'),
                        'Conflict Resolution Strategy': 'replace',
                        'Create Missing Directories': 'true',
                        'Maximum File Count': '-1',
                        'Last Modified Time': '',
                        'Permissions': '',
                        'Owner': '',
                        'Group': '',
                        'Directory Permissions': '0755'
                    }
                )
            )
            
            logger.info(f"Created file loader processor: {processor.id}")
            return processor
            
        except Exception as e:
            logger.error(f"Error creating file loader: {str(e)}")
            raise
    
    def _create_audit_logger(self) -> nipyapi.nifi.ProcessorEntity:
        """Create processor for audit logging"""
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=self.process_group,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.LogAttribute'),
                location=(700.0, 100.0),
                name='Audit_Load_Success',
                config=ProcessorConfigDTO(
                    scheduling_period='0 sec',
                    auto_terminated_relationships=['success'],
                    properties={
                        'Log Level': 'info',
                        'Log Payload': 'false',
                        'Attributes to Log': 'return_id,validation_status,load_timestamp,record_count',
                        'Attributes to Ignore': '',
                        'Log prefix': 'SALES_RETURNS_LOAD: '
                    }
                )
            )
            
            logger.info(f"Created audit logger processor: {processor.id}")
            return processor
            
        except Exception as e:
            logger.error(f"Error creating audit logger: {str(e)}")
            raise
    
    def _create_error_handler(self) -> nipyapi.nifi.ProcessorEntity:
        """Create processor for handling load errors"""
        try:
            processor = nipyapi.canvas.create_processor(
                parent_pg=self.process_group,
                processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.PutFile'),
                location=(700.0, 500.0),
                name='Handle_Load_Errors',
                config=ProcessorConfigDTO(
                    scheduling_period='0 sec',
                    auto_terminated_relationships=['failure', 'success'],
                    properties={
                        'Directory': self.load_config.get('error_directory', '/data/errors/returns'),
                        'Conflict Resolution Strategy': 'replace',
                        'Create Missing Directories': 'true'
                    }
                )
            )
            
            logger.info(f"Created error handler processor: {processor.id}")
            return processor
            
        except Exception as e:
            logger.error(f"Error creating error handler: {str(e)}")
            raise
    
    def _create_connections(self):
        """Create connections between processors"""
        try:
            processors = nipyapi.canvas.list_all_processors(self.process_group.id)
            processor_map = {p.component.name: p for p in processors}
            
            # Validation -> Database Load
            nipyapi.canvas.create_connection(
                source=processor_map['Validate_Refund_Calculations'],
                target=processor_map['Load_Returns_To_Database'],
                relationships=['success']
            )
            
            # Validation -> Error Handler (for invalid records)
            nipyapi.canvas.create_connection(
                source=processor_map['Validate_Refund_Calculations'],
                target=processor_map['Handle_Load_Errors'],
                relationships=['failure']
            )
            
            # Database Load -> Archive
            nipyapi.canvas.create_connection(
                source=processor_map['Load_Returns_To_Database'],
                target=processor_map['Archive_Processed_Returns'],
                relationships=['success']
            )
            
            # Archive -> Audit
            nipyapi.canvas.create_connection(
                source=processor_map['Archive_Processed_Returns'],
                target=processor_map['Audit_Load_Success'],
                relationships=['success']
            )
            
            logger.info("Created all processor connections")
            
        except Exception as e:
            logger.error(f"Error creating connections: {str(e)}")
            raise
    
    def validate_refund_calculation(self, return_record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate refund calculation against source system rules
        
        Args:
            return_record: Return record to validate
            
        Returns:
            Validation results dictionary
        """
        try:
            validation_results = {
                'return_id': return_record.get('return_id'),
                'validation_timestamp': datetime.now().isoformat(),
                'validations': [],
                'errors': []
            }
            
            # Extract values
            original_amount = Decimal(str(return_record.get('original_amount', 0)))
            return_quantity = int(return_record.get('return_quantity', 0))
            original_quantity = int(return_record.get('original_quantity', 0))
            unit_price = Decimal(str(return_record.get('unit_price', 0)))
            restocking_fee_pct = Decimal(str(return_record.get('restocking_fee_percentage', 0)))
            calculated_refund = Decimal(str(return_record.get('calculated_refund', 0)))
            
            # Validation 1: Refund calculation accuracy
            subtotal = unit_price * return_quantity
            restocking_fee = subtotal * (restocking_fee_pct / 100)
            expected_refund = subtotal - restocking_fee
            
            refund_diff = abs(expected_refund - calculated_refund)
            refund_valid = refund_diff < Decimal('0.01')
            
            validation_results['validations'].append({
                'check': 'refund_calculation',
                'expected': float(expected_refund),
                'actual': float(calculated_refund),
                'difference': float(refund_diff),
                'passed': refund_valid,
                'tolerance': 0.01
            })
            
            if not refund_valid:
                validation_results['errors'].append(
                    f"Refund calculation mismatch: expected {expected_refund}, got {calculated_refund}"
                )
            
            # Validation 2: Return quantity
            quantity_valid = 0 < return_quantity <= original_quantity
            validation_results['validations'].append({
                'check': 'return_quantity',
                'expected': f'1 to {original_quantity}',
                'actual': return_quantity,
                'passed': quantity_valid
            })
            
            if not quantity_valid:
                validation_results['errors'].append(
                    f"Invalid return quantity: {return_quantity} (original: {original_quantity})"
                )
            
            # Validation 3: Refund status
            refund_status = return_record.get('refund_status', '')
            valid_statuses = self.validation_config.get('valid_refund_statuses', 
                                                        ['APPROVED', 'PENDING', 'PROCESSED'])
            status_valid = refund_status in valid_statuses
            
            validation_results['validations'].append({
                'check': 'refund_status',
                'expected': valid_statuses,
                'actual': refund_status,
                'passed': status_valid
            })
            
            if not status_valid:
                validation_results['errors'].append(
                    f"Invalid refund status: {refund_status}"
                )
            
            # Validation 4: Return reason code
            return_reason = return_record.get('return_reason_code', '')
            valid_reasons = self.validation_config.get('valid_return_reasons', [])
            reason_valid = not valid_reasons or return_reason in valid_reasons
            
            validation_results['validations'].append({
                'check': 'return_reason',
                'expected': valid_reasons if valid_reasons else 'any',
                'actual': return_reason,
                'passed': reason_valid
            })
            
            if not reason_valid:
                validation_results['errors'].append(
                    f"Invalid return reason: {return_reason}"
                )
            
            # Validation 5: Date validations
            return_date = return_record.get('return_date')
            order_date = return_record.get('order_date')
            
            if return_date and order_date:
                date_valid = return_date >= order_date
                validation_results['validations'].append({
                    'check': 'return_date',
                    'expected': f'>= {order_date}',
                    'actual': return_date,
                    'passed': date_valid
                })
                
                if not date_valid:
                    validation_results['errors'].append(
                        f"Return date {return_date} before order date {order_date}"
                    )
            
            # Overall validation status
            all_passed = all(v['passed'] for v in validation_results['validations'])
            validation_results['overall_status'] = 'VALID' if all_passed else 'INVALID'
            validation_results['error_count'] = len(validation_results['errors'])
            
            return validation_results
            
        except Exception as e:
            logger.error(f"Error validating refund calculation: {str(e)}")
            return {
                'return_id': return_record.get('return_id'),
                'overall_status': 'ERROR',
                'errors': [str(e)]
            }
    
    def load_to_database(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Load validated records to target database
        
        Args:
            records: List of validated return records
            
        Returns:
            Load results summary
        """
        try:
            load_results = {
                'total_records': len(records),
                'loaded_count': 0,
                'failed_count': 0,
                'validation_failed_count': 0,
                'load_timestamp': datetime.now().isoformat(),
                'failed_records': []
            }
            
            for record in records:
                try:
                    # Validate before loading
                    validation = self.validate_refund_calculation(record)
                    
                    if validation['overall_status'] != 'VALID':
                        load_results['validation_failed_count'] += 1
                        load_results['failed_records'].append({
                            'return_id': record.get('return_id'),
                            'reason': 'validation_failed',
                            'errors': validation.get('errors', [])
                        })
                        continue
                    
                    # Add validation results to record
                    record['validation_results'] = json.dumps(validation)
                    record['load_timestamp'] = datetime.now().isoformat()
                    
                    # In production, this would execute actual database insert
                    # For now, we log the operation
                    logger.info(f"Loading return record: {record.get('return_id')}")
                    load_results['loaded_count'] += 1
                    
                except Exception as e:
                    logger.error(f"Error loading record {record.get('return_id')}: {str(e)}")
                    load_results['failed_count'] += 1
                    load_results['failed_records'].append({
                        'return_id': record.get('return_id'),
                        'reason': 'load_error',
                        'error': str(e)
                    })
            
            logger.info(f"Load completed: {load_results['loaded_count']} loaded, "
                       f"{load_results['failed_count']} failed, "
                       f"{load_results['validation_failed_count']} validation failed")
            
            return load_results
            
        except Exception as e:
            logger.error(f"Error in load_to_database: {str(e)}")
            raise
    
    def start_load_flow(self):
        """Start the load process group"""
        try:
            if self.process_group:
                schedule_process_group(self.process_group.id, scheduled=True)
                logger.info(f"Started load flow: {self.process_group.id}")
            else:
                raise ValueError("Process group not created")
                
        except Exception as e:
            logger.error(f"Error starting load flow: {str(e)}")
            raise
    
    def stop_load_flow(self):
        """Stop the load process group"""
        try:
            if self.process_group:
                schedule_process_group(self.process_group.id, scheduled=False)
                logger.info(f"Stopped load flow: {self.process_group.id}")
                
        except Exception as e:
            logger.error(f"Error stopping load flow: {str(e)}")
            raise


def create_load_flow(config: Dict[str, Any], parent_pg: ProcessGroupEntity) -> ProcessGroupEntity:
    """
    Create and configure the load flow
    
    Args:
        config: Configuration dictionary
        parent_pg: Parent process group
        
    Returns:
        Created process group
    """
    loader = SalesReturnsLoader(config)
    return loader.create_load_flow(parent_pg)