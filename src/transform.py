"""
Transform module for transaction data processing pipeline.
Handles decimal precision, date/time conversion, and transaction type validation.
"""

import logging
from typing import Dict, Any, List, Optional
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime
import nipyapi
from nipyapi.nifi import ProcessorConfigDTO
from nipyapi.canvas import create_processor

logger = logging.getLogger(__name__)


class TransactionTransformer:
    """Handles transformation of transaction data with precision and validation."""
    
    # Transaction type validation mapping
    VALID_TRANSACTION_TYPES = {
        'PURCHASE': 'PUR',
        'REFUND': 'REF',
        'ADJUSTMENT': 'ADJ',
        'PAYMENT': 'PAY',
        'TRANSFER': 'TRF',
        'WITHDRAWAL': 'WTH',
        'DEPOSIT': 'DEP'
    }
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the transaction transformer.
        
        Args:
            config: Configuration dictionary containing transformation rules
        """
        self.config = config
        self.transform_config = config.get('transform', {})
        self.precision_config = self.transform_config.get('precision', {})
        self.date_config = self.transform_config.get('date_conversion', {})
        
    def create_transformation_flow(self, canvas: Any, parent_pg: Any) -> Dict[str, Any]:
        """
        Create NiFi processors for transaction data transformation.
        
        Args:
            canvas: NiFi canvas object
            parent_pg: Parent process group
            
        Returns:
            Dictionary containing created processor references
        """
        logger.info("Creating transaction transformation flow")
        
        processors = {}
        
        try:
            # Create QueryRecord for decimal precision handling
            query_record_config = {
                'Record Reader': self.transform_config.get('record_reader_service', 'CSVReader'),
                'Record Writer': self.transform_config.get('record_writer_service', 'CSVRecordSetWriter'),
                'Include Zero Record FlowFiles': 'false',
                'Cache Schema': 'true',
                'transaction_precision': self._build_precision_query()
            }
            
            query_record = create_processor(
                parent_pg=parent_pg,
                processor=canvas.get_processor_type('QueryRecord'),
                location=(400, 100),
                name='Apply_Decimal_Precision',
                config=ProcessorConfigDTO(
                    properties=query_record_config,
                    auto_terminated_relationships=['failure', 'original'],
                    scheduling_period='0 sec',
                    scheduling_strategy='EVENT_DRIVEN',
                    bulletin_level='WARN'
                )
            )
            processors['query_record'] = query_record
            logger.info(f"Created QueryRecord processor for precision: {query_record.id}")
            
            # Create UpdateRecord for date/time conversion
            update_record_config = {
                'Record Reader': self.transform_config.get('record_reader_service', 'CSVReader'),
                'Record Writer': self.transform_config.get('record_writer_service', 'CSVRecordSetWriter'),
                'Replacement Value Strategy': 'Record Path Value',
                '/transaction_date': self._build_date_conversion_expression('transaction_date'),
                '/transaction_time': self._build_time_conversion_expression('transaction_time'),
                '/transaction_timestamp': self._build_timestamp_conversion_expression('transaction_timestamp'),
                '/created_date': "format(now(), 'yyyy-MM-dd HH:mm:ss')",
                '/modified_date': "format(now(), 'yyyy-MM-dd HH:mm:ss')"
            }
            
            update_record = create_processor(
                parent_pg=parent_pg,
                processor=canvas.get_processor_type('UpdateRecord'),
                location=(400, 250),
                name='Convert_DateTime_Fields',
                config=ProcessorConfigDTO(
                    properties=update_record_config,
                    auto_terminated_relationships=['failure'],
                    scheduling_period='0 sec',
                    scheduling_strategy='EVENT_DRIVEN'
                )
            )
            processors['update_record'] = update_record
            logger.info(f"Created UpdateRecord processor for dates: {update_record.id}")
            
            # Create LookupRecord for transaction type validation
            lookup_record_config = {
                'Record Reader': self.transform_config.get('record_reader_service', 'CSVReader'),
                'Record Writer': self.transform_config.get('record_writer_service', 'CSVRecordSetWriter'),
                'Lookup Service': self.transform_config.get('lookup_service', 'SimpleKeyValueLookupService'),
                'Result RecordPath': '/transaction_type_code',
                'Routing Strategy': 'Route to Success',
                'Record Result Contents': 'Insert Entire Record',
                'Record Update Strategy': 'Use Property',
                'transaction_type': '/transaction_type'
            }
            
            lookup_record = create_processor(
                parent_pg=parent_pg,
                processor=canvas.get_processor_type('LookupRecord'),
                location=(400, 400),
                name='Validate_Transaction_Type',
                config=ProcessorConfigDTO(
                    properties=lookup_record_config,
                    auto_terminated_relationships=['failure'],
                    scheduling_period='0 sec',
                    scheduling_strategy='EVENT_DRIVEN'
                )
            )
            processors['lookup_record'] = lookup_record
            logger.info(f"Created LookupRecord processor: {lookup_record.id}")
            
            # Create PartitionRecord for transaction categorization
            partition_config = {
                'Record Reader': self.transform_config.get('record_reader_service', 'CSVReader'),
                'Record Writer': self.transform_config.get('record_writer_service', 'CSVRecordSetWriter'),
                'high_value': "/transaction_amount >= " + str(self.transform_config.get('high_value_threshold', 10000)),
                'medium_value': "/transaction_amount >= " + str(self.transform_config.get('medium_value_threshold', 1000)) + 
                               " and /transaction_amount < " + str(self.transform_config.get('high_value_threshold', 10000)),
                'low_value': "/transaction_amount < " + str(self.transform_config.get('medium_value_threshold', 1000))
            }
            
            partition_record = create_processor(
                parent_pg=parent_pg,
                processor=canvas.get_processor_type('PartitionRecord'),
                location=(400, 550),
                name='Categorize_Transaction_Value',
                config=ProcessorConfigDTO(
                    properties=partition_config,
                    auto_terminated_relationships=['failure'],
                    scheduling_period='0 sec',
                    scheduling_strategy='EVENT_DRIVEN'
                )
            )
            processors['partition_record'] = partition_record
            logger.info(f"Created PartitionRecord processor: {partition_record.id}")
            
            # Create ValidateRecord for final validation
            validate_config = {
                'Record Reader': self.transform_config.get('record_reader_service', 'CSVReader'),
                'Record Writer': self.transform_config.get('record_writer_service', 'CSVRecordSetWriter'),
                'Schema Access Strategy': 'Use Schema Name Property',
                'Schema Registry': self.transform_config.get('schema_registry_service', 'AvroSchemaRegistry'),
                'Schema Name': 'transaction_schema',
                'Allow Extra Fields': 'false',
                'Strict Type Checking': 'true',
                'Validation Strategy': 'All Records Valid'
            }
            
            validate_final = create_processor(
                parent_pg=parent_pg,
                processor=canvas.get_processor_type('ValidateRecord'),
                location=(400, 700),
                name='Validate_Transformed_Data',
                config=ProcessorConfigDTO(
                    properties=validate_config,
                    auto_terminated_relationships=['failure'],
                    scheduling_period='0 sec',
                    scheduling_strategy='EVENT_DRIVEN'
                )
            )
            processors['validate_final'] = validate_final
            logger.info(f"Created final ValidateRecord processor: {validate_final.id}")
            
            # Create ExecuteScript for complex business rules
            script_config = {
                'Script Engine': 'python',
                'Script File': self.transform_config.get('business_rules_script', '/opt/nifi/scripts/transaction_rules.py'),
                'Script Body': self._get_business_rules_script(),
                'Module Directory': '/opt/nifi/scripts/modules'
            }
            
            execute_script = create_processor(
                parent_pg=parent_pg,
                processor=canvas.get_processor_type('ExecuteScript'),
                location=(400, 850),
                name='Apply_Business_Rules',
                config=ProcessorConfigDTO(
                    properties=script_config,
                    auto_terminated_relationships=['failure'],
                    scheduling_period='0 sec',
                    scheduling_strategy='EVENT_DRIVEN'
                )
            )
            processors['execute_script'] = execute_script
            logger.info(f"Created ExecuteScript processor: {execute_script.id}")
            
            return processors
            
        except Exception as e:
            logger.error(f"Error creating transformation flow: {str(e)}")
            raise
    
    def _build_precision_query(self) -> str:
        """
        Build SQL query for decimal precision handling.
        
        Returns:
            SQL query string with CAST operations for decimal fields
        """
        amount_precision = self.precision_config.get('amount_precision', 2)
        rate_precision = self.precision_config.get('rate_precision', 4)
        quantity_precision = self.precision_config.get('quantity_precision', 3)
        
        query = f"""
        SELECT 
            transaction_id,
            customer_id,
            transaction_type,
            CAST(transaction_amount AS DECIMAL(18, {amount_precision})) as transaction_amount,
            CAST(tax_amount AS DECIMAL(18, {amount_precision})) as tax_amount,
            CAST(discount_amount AS DECIMAL(18, {amount_precision})) as discount_amount,
            CAST(total_amount AS DECIMAL(18, {amount_precision})) as total_amount,
            CAST(exchange_rate AS DECIMAL(10, {rate_precision})) as exchange_rate,
            CAST(quantity AS DECIMAL(15, {quantity_precision})) as quantity,
            CAST(unit_price AS DECIMAL(18, {amount_precision})) as unit_price,
            transaction_date,
            transaction_time,
            transaction_timestamp,
            currency_code,
            payment_method,
            status,
            description,
            reference_number,
            merchant_id,
            terminal_id,
            batch_id
        FROM FLOWFILE
        WHERE transaction_amount IS NOT NULL
        """
        
        return query.strip()
    
    def _build_date_conversion_expression(self, field_name: str) -> str:
        """
        Build RecordPath expression for date conversion.
        
        Args:
            field_name: Name of the date field
            
        Returns:
            RecordPath expression string
        """
        source_format = self.date_config.get('source_date_format', 'MM/dd/yyyy')
        target_format = self.date_config.get('target_date_format', 'yyyy-MM-dd')
        
        return f"format(toDate(/{field_name}, '{source_format}'), '{target_format}')"
    
    def _build_time_conversion_expression(self, field_name: str) -> str:
        """
        Build RecordPath expression for time conversion.
        
        Args:
            field_name: Name of the time field
            
        Returns:
            RecordPath expression string
        """
        source_format = self.date_config.get('source_time_format', 'hh:mm:ss a')
        target_format = self.date_config.get('target_time_format', 'HH:mm:ss')
        
        return f"format(toDate(concat(format(now(), 'yyyy-MM-dd'), ' ', /{field_name}), 'yyyy-MM-dd {source_format}'), '{target_format}')"
    
    def _build_timestamp_conversion_expression(self, field_name: str) -> str:
        """
        Build RecordPath expression for timestamp conversion.
        
        Args:
            field_name: Name of the timestamp field
            
        Returns:
            RecordPath expression string
        """
        source_format = self.date_config.get('source_timestamp_format', 'MM/dd/yyyy hh:mm:ss a')
        target_format = self.date_config.get('target_timestamp_format', 'yyyy-MM-dd HH:mm:ss')
        
        return f"format(toDate(/{field_name}, '{source_format}'), '{target_format}')"
    
    def _get_business_rules_script(self) -> str:
        """
        Get Python script for complex business rules.
        
        Returns:
            Python script as string
        """
        return """
import json
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import StreamCallback

class TransactionProcessor(StreamCallback):
    def __init__(self):
        pass
    
    def process(self, inputStream, outputStream):
        text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
        records = json.loads(text)
        
        processed_records = []
        for record in records:
            # Apply business rules
            record = self.validate_transaction_amount(record)
            record = self.calculate_derived_fields(record)
            record = self.apply_fraud_checks(record)
            processed_records.append(record)
        
        outputStream.write(json.dumps(processed_records).encode('utf-8'))
    
    def validate_transaction_amount(self, record):
        # Ensure total = amount + tax - discount
        amount = Decimal(str(record.get('transaction_amount', 0)))
        tax = Decimal(str(record.get('tax_amount', 0)))
        discount = Decimal(str(record.get('discount_amount', 0)))
        
        calculated_total = (amount + tax - discount).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
        record['total_amount'] = str(calculated_total)
        record['amount_validated'] = 'true'
        
        return record
    
    def calculate_derived_fields(self, record):
        # Calculate effective rate
        if record.get('quantity') and float(record.get('quantity', 0)) > 0:
            amount = Decimal(str(record.get('transaction_amount', 0)))
            quantity = Decimal(str(record.get('quantity', 1)))
            unit_price = (amount / quantity).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            record['unit_price'] = str(unit_price)
        
        # Add processing metadata
        record['processing_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        record['processing_version'] = '1.0'
        
        return record
    
    def apply_fraud_checks(self, record):
        # Simple fraud indicators
        fraud_score = 0
        
        amount = float(record.get('transaction_amount', 0))
        if amount > 10000:
            fraud_score += 30
        if amount > 50000:
            fraud_score += 50
        
        # Check for unusual patterns
        if record.get('transaction_type') == 'WITHDRAWAL' and amount > 5000:
            fraud_score += 20
        
        record['fraud_score'] = fraud_score
        record['fraud_flag'] = 'true' if fraud_score >= 50 else 'false'
        
        return record

flowFile = session.get()
if flowFile is not None:
    flowFile = session.write(flowFile, TransactionProcessor())
    session.transfer(flowFile, REL_SUCCESS)
"""
    
    def create_connections(self, processors: Dict[str, Any]) -> None:
        """
        Create connections between transformation processors.
        
        Args:
            processors: Dictionary of processor references
        """
        try:
            # Connect QueryRecord to UpdateRecord
            nipyapi.canvas.create_connection(
                source=processors['query_record'],
                target=processors['update_record'],
                relationships=['transaction_precision']
            )
            
            # Connect UpdateRecord to LookupRecord
            nipyapi.canvas.create_connection(
                source=processors['update_record'],
                target=processors['lookup_record'],
                relationships=['success']
            )
            
            # Connect LookupRecord to PartitionRecord
            nipyapi.canvas.create_connection(
                source=processors['lookup_record'],
                target=processors['partition_record'],
                relationships=['matched', 'unmatched']
            )
            
            # Connect PartitionRecord to ValidateRecord
            for relationship in ['high_value', 'medium_value', 'low_value']:
                nipyapi.canvas.create_connection(
                    source=processors['partition_record'],
                    target=processors['validate_final'],
                    relationships=[relationship]
                )
            
            # Connect ValidateRecord to ExecuteScript
            nipyapi.canvas.create_connection(
                source=processors['validate_final'],
                target=processors['execute_script'],
                relationships=['valid']
            )
            
            logger.info("Successfully created transformation flow connections")
            
        except Exception as e:
            logger.error(f"Error creating transformation connections: {str(e)}")
            raise


def setup_lookup_service(canvas: Any, parent_pg: Any, config: Dict[str, Any]) -> Any:
    """
    Setup lookup service for transaction type validation.
    
    Args:
        canvas: NiFi canvas object
        parent_pg: Parent process group
        config: Configuration dictionary
        
    Returns:
        Controller service reference
    """
    try:
        lookup_props = {
            'PURCHASE': 'PUR',
            'REFUND': 'REF',
            'ADJUSTMENT': 'ADJ',
            'PAYMENT': 'PAY',
            'TRANSFER': 'TRF',
            'WITHDRAWAL': 'WTH',
            'DEPOSIT': 'DEP'
        }
        
        lookup_service = nipyapi.canvas.create_controller_service(
            parent_pg=parent_pg,
            service_type='org.apache.nifi.lookup.SimpleKeyValueLookupService',
            name='Transaction_Type_Lookup',
            properties=lookup_props
        )
        
        nipyapi.canvas.schedule_controller_service(lookup_service, scheduled=True)
        logger.info(f"Created and enabled lookup service: {lookup_service.id}")
        
        return lookup_service
        
    except Exception as e:
        logger.error(f"Error setting up lookup service: {str(e)}")
        raise