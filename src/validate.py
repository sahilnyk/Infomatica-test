"""
Transaction validation module.
Implements comprehensive validation rules for transaction data.
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
import re
import nipyapi
from nipyapi.nifi import ProcessorConfigDTO

logger = logging.getLogger(__name__)


class TransactionValidator:
    """Validate transaction data against business rules."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the validator.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.validation_config = config['validation']
        self.validation_errors = []
        
    def create_validation_processors(
        self,
        process_group_id: str,
        canvas: nipyapi.nifi.ProcessGroupFlowEntity
    ) -> Dict[str, Any]:
        """
        Create NiFi processors for validation.
        
        Args:
            process_group_id: Parent process group ID
            canvas: NiFi canvas object
            
        Returns:
            Dictionary of created processors
        """
        processors = {}
        
        try:
            # Create ValidateRecord processor
            validate_record = self._create_validate_record_processor(
                process_group_id=process_group_id,
                name="Validate_Transaction_Schema",
                position=(700, 100)
            )
            processors['validate_record'] = validate_record
            
            # Create ExecuteScript processor for custom validation
            custom_validation = self._create_custom_validation_processor(
                process_group_id=process_group_id,
                name="Custom_Business_Rules_Validation",
                position=(900, 100)
            )
            processors['custom_validation'] = custom_validation
            
            # Create RouteOnAttribute for validation routing
            route_validation = self._create_route_validation_processor(
                process_group_id=process_group_id,
                name="Route_Validation_Results",
                position=(1100, 100)
            )
            processors['route_validation'] = route_validation
            
            # Create UpdateAttribute for validation metadata
            update_validation_metadata = self._create_validation_metadata_processor(
                process_group_id=process_group_id,
                name="Update_Validation_Metadata",
                position=(1300, 100)
            )
            processors['update_validation_metadata'] = update_validation_metadata
            
            logger.info("Successfully created validation processors")
            return processors
            
        except Exception as e:
            logger.error(f"Error creating validation processors: {str(e)}")
            raise
    
    def _create_validate_record_processor(
        self,
        process_group_id: str,
        name: str,
        position: tuple
    ) -> nipyapi.nifi.ProcessorEntity:
        """Create a ValidateRecord processor."""
        processor_config = ProcessorConfigDTO()
        processor_config.properties = {
            'Record Reader': 'CSVReader',
            'Record Writer': 'CSVRecordSetWriter',
            'Schema Access Strategy': 'schema-name',
            'Allow Extra Fields': 'true',
            'Strict Type Checking': 'true'
        }
        processor_config.auto_terminated_relationships = []
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(process_group_id, 'id'),
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.ValidateRecord'),
            location=(position[0], position[1]),
            name=name,
            config=processor_config
        )
        
        logger.info(f"Created ValidateRecord processor: {name}")
        return processor
    
    def _create_custom_validation_processor(
        self,
        process_group_id: str,
        name: str,
        position: tuple
    ) -> nipyapi.nifi.ProcessorEntity:
        """Create an ExecuteScript processor for custom validation."""
        validation_script = self._generate_validation_script()
        
        processor_config = ProcessorConfigDTO()
        processor_config.properties = {
            'Script Engine': 'python',
            'Script Body': validation_script,
            'Module Directory': '/opt/nifi/scripts/modules'
        }
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(process_group_id, 'id'),
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.script.ExecuteScript'),
            location=(position[0], position[1]),
            name=name,
            config=processor_config
        )
        
        logger.info(f"Created ExecuteScript processor: {name}")
        return processor
    
    def _create_route_validation_processor(
        self,
        process_group_id: str,
        name: str,
        position: tuple
    ) -> nipyapi.nifi.ProcessorEntity:
        """Create a RouteOnAttribute processor for validation routing."""
        processor_config = ProcessorConfigDTO()
        processor_config.properties = {
            'Routing Strategy': 'Route to Property name',
            'valid': '${validation.status:equals("VALID")}',
            'invalid': '${validation.status:equals("INVALID")}',
            'warning': '${validation.status:equals("WARNING")}'
        }
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=nipyapi.canvas.get_process_group(process_group_id, 'id'),
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.RouteOnAttribute'),
            location=(position[0], position[1]),
            name=name,
            config=processor_config
        )
        
        logger.info(f"Created RouteOnAttribute processor: {name}")
        return processor
    
    def _create_validation_metadata_processor(
        self,
        process_group_id: str,
        name: str,
        position: tuple
    ) -> nipyapi.nifi.ProcessorEntity:
        """Create an UpdateAttribute processor for validation metadata."""
        processor_config = ProcessorConfigDTO()
        processor_config.properties = {
            'validation.timestamp': '${now():format("yyyy-MM-dd HH:mm:ss")}',
            'validation.processor': name,
            'validation.rules.applied': 'amount,date_range,type,status'
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
    
    def _generate_validation_script(self) -> str:
        """Generate Python validation script for ExecuteScript processor."""
        script = f"""
import json
import csv
from datetime import datetime, timedelta
from decimal import Decimal
from io import StringIO
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import StreamCallback

class ValidationCallback(StreamCallback):
    def __init__(self):
        self.validation_config = {json.dumps(self.validation_config)}
        
    def process(self, inputStream, outputStream):
        text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
        reader = csv.DictReader(StringIO(text))
        
        validated_records = []
        validation_errors = []
        
        for record in reader:
            is_valid, errors = self.validate_transaction(record)
            
            record['validation_status'] = 'VALID' if is_valid else 'INVALID'
            record['validation_errors'] = '|'.join(errors) if errors else ''
            record['validation_timestamp'] = datetime.now().isoformat()
            
            validated_records.append(record)
            
            if errors:
                validation_errors.extend(errors)
        
        # Write validated records
        output = StringIO()
        if validated_records:
            writer = csv.DictWriter(output, fieldnames=validated_records[0].keys())
            writer.writeheader()
            writer.writerows(validated_records)
        
        outputStream.write(output.getvalue().encode('utf-8'))
        
        # Set flowfile attributes
        flowFile = session.get()
        if flowFile:
            flowFile = session.putAttribute(flowFile, 'validation.record.count', str(len(validated_records)))
            flowFile = session.putAttribute(flowFile, 'validation.error.count', str(len(validation_errors)))
            flowFile = session.putAttribute(flowFile, 'validation.status', 
                'VALID' if not validation_errors else 'INVALID')
    
    def validate_transaction(self, record):
        errors = []
        
        # Amount validation
        amount_errors = self.validate_amount(record.get('amount'))
        errors.extend(amount_errors)
        
        # Date validation
        date_errors = self.validate_date(record.get('transaction_date'))
        errors.extend(date_errors)
        
        # Type validation
        type_errors = self.validate_type(record.get('transaction_type'))
        errors.extend(type_errors)
        
        # Status validation
        status_errors = self.validate_status(record.get('status'))
        errors.extend(status_errors)
        
        return len(errors) == 0, errors
    
    def validate_amount(self, amount_str):
        errors = []
        config = self.validation_config['amount']
        
        try:
            amount = Decimal(str(amount_str))
            
            if amount < Decimal(str(config['min_value'])):
                errors.append(f"Amount {{amount}} below minimum {{config['min_value']}}")
            
            if amount > Decimal(str(config['max_value'])):
                errors.append(f"Amount {{amount}} exceeds maximum {{config['max_value']}}")
            
            if not config['allow_negative'] and amount < 0:
                errors.append(f"Negative amounts not allowed: {{amount}}")
                
        except Exception as e:
            errors.append(f"Invalid amount format: {{amount_str}}")
        
        return errors
    
    def validate_date(self, date_str):
        errors = []
        config = self.validation_config['date_range']
        
        try:
            trans_date = datetime.strptime(date_str, config['date_format'])
            min_date = datetime.strptime(config['min_date'], config['date_format'])
            max_date = datetime.now() + timedelta(days=config['max_date_offset_days'])
            
            if trans_date < min_date:
                errors.append(f"Date {{date_str}} before minimum {{config['min_date']}}")
            
            if trans_date > max_date:
                errors.append(f"Date {{date_str}} is in the future")
                
        except Exception as e:
            errors.append(f"Invalid date format: {{date_str}}")
        
        return errors
    
    def validate_type(self, trans_type):
        errors = []
        allowed = self.validation_config['transaction_types']['allowed']
        
        if trans_type not in allowed:
            errors.append(f"Invalid transaction type: {{trans_type}}")
        
        return errors
    
    def validate_status(self, status):
        errors = []
        allowed = self.validation_config['status_codes']['allowed']
        
        if status not in allowed:
            errors.append(f"Invalid status code: {{status}}")
        
        return errors

flowFile = session.get()
if flowFile:
    flowFile = session.write(flowFile, ValidationCallback())
    session.transfer(flowFile, REL_SUCCESS)
"""
        return script
    
    def validate_transaction(self, transaction: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate a single transaction record.
        
        Args:
            transaction: Transaction record
            
        Returns:
            Tuple of (is_valid, list of error messages)
        """
        errors = []
        
        # Amount validation
        amount_errors = self.validate_amount(transaction.get('amount'))
        errors.extend(amount_errors)
        
        # Date validation
        date_errors = self.validate_date(transaction.get('transaction_date'))
        errors.extend(date_errors)
        
        # Type validation
        type_errors = self.validate_transaction_type(transaction.get('transaction_type'))
        errors.extend(type_errors)
        
        # Status validation
        status_errors = self.validate_status(transaction.get('status'))
        errors.extend(status_errors)
        
        # Currency validation
        currency_errors = self.validate_currency(transaction.get('currency'))
        errors.extend(currency_errors)
        
        # ID validation
        id_errors = self.validate_transaction_id(transaction.get('transaction_id'))
        errors.extend(id_errors)
        
        is_valid = len(errors) == 0
        
        if not is_valid:
            logger.warning(f"Transaction {transaction.get('transaction_id')} validation failed: {errors}")
        
        return is_valid, errors
    
    def validate_amount(self, amount_str: str) -> List[str]:
        """Validate transaction amount."""
        errors = []
        config = self.validation_config['amount']
        
        try:
            amount = Decimal(str(amount_str))
            
            if amount < Decimal(str(config['min_value'])):
                errors.append(f"Amount {amount} below minimum {config['min_value']}")
            
            if amount > Decimal(str(config['max_value'])):
                errors.append(f"Amount {amount} exceeds maximum {config['max_value']}")
            
            if not config['allow_negative'] and amount < 0:
                errors.append(f"Negative amounts not allowed: {amount}")
            
            # Check precision
            decimal_places = abs(amount.as_tuple().exponent)
            if decimal_places > config['precision']:
                errors.append(f"Amount precision {decimal_places} exceeds maximum {config['precision']}")
                
        except (InvalidOperation, ValueError, TypeError) as e:
            errors.append(f"Invalid amount format: {amount_str}")
        
        return errors
    
    def validate_date(self, date_str: str) -> List[str]:
        """Validate transaction date."""
        errors = []
        config = self.validation_config['date_range']
        
        try:
            trans_date = datetime.strptime(date_str, config['date_format'])
            min_date = datetime.strptime(config['min_date'], config['date_format'])
            max_date = datetime.now() + timedelta(days=config['max_date_offset_days'])
            
            if trans_date < min_date:
                errors.append(f"Date {date_str} before minimum {config['min_date']}")
            
            if trans_date > max_date:
                errors.append(f"Date {date_str} is in the future")
                
        except (ValueError, TypeError) as e:
            errors.append(f"Invalid date format: {date_str}")
        
        return errors
    
    def validate_transaction_type(self, trans_type: str) -> List[str]:
        """Validate transaction type."""
        errors = []
        allowed = self.validation_config['transaction_types']['allowed']
        
        if not trans_type:
            errors.append("Transaction type is required")
        elif trans_type not in allowed:
            errors.append(f"Invalid transaction type: {trans_type}. Allowed: {allowed}")
        
        return errors
    
    def validate_status(self, status: str) -> List[str]:
        """Validate transaction status."""
        errors = []
        allowed = self.validation_config['status_codes']['allowed']
        
        if not status:
            errors.append("Status is required")
        elif status not in allowed:
            errors.append(f"Invalid status: {status}. Allowed: {allowed}")
        
        return errors
    
    def validate_currency(self, currency: str) -> List[str]:
        """Validate currency code."""
        errors = []
        
        if not currency:
            errors.append("Currency is required")
        elif not re.match(r'^[A-Z]{3}$', currency):
            errors.append(f"Invalid currency format: {currency}. Expected 3-letter ISO code")
        
        return errors
    
    def validate_transaction_id(self, trans_id: str) -> List[str]:
        """Validate transaction ID."""
        errors = []
        
        if not trans_id:
            errors.append("Transaction ID is required")
        elif len(trans_id) > 50:
            errors.append(f"Transaction ID too long: {len(trans_id)} characters")
        
        return errors
    
    def validate_batch(self, transactions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate a batch of transactions.
        
        Args:
            transactions: List of transaction records
            
        Returns:
            Validation summary
        """
        valid_count = 0
        invalid_count = 0
        validation_results = []
        
        for transaction in transactions:
            is_valid, errors = self.validate_transaction(transaction)
            
            result = {
                'transaction_id': transaction.get('transaction_id'),
                'is_valid': is_valid,
                'errors': errors,
                'validation_timestamp': datetime.now().isoformat()
            }
            validation_results.append(result)
            
            if is_valid:
                valid_count += 1
            else:
                invalid_count += 1
        
        summary = {
            'total_records': len(transactions),
            'valid_count': valid_count,
            'invalid_count': invalid_count,
            'validation_rate': valid_count / len(transactions) if transactions else 0,
            'validation_results': validation_results,
            'validation_timestamp': datetime.now().isoformat()
        }
        
        logger.info(f"Batch validation complete: {valid_count} valid, {invalid_count} invalid")
        return summary