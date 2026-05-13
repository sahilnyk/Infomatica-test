# Sales Order Processing - Load and Testing Migration

===FILE: src/load.py===
"""
Sales Order Load Module
Loads processed sales orders to target systems with data quality validation
"""
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import pandas as pd
from dataclasses import dataclass
import nipyapi
from nipyapi.nifi import ProcessorConfigDTO, ProcessGroupEntity
import json

logger = logging.getLogger(__name__)


@dataclass
class LoadMetrics:
    """Metrics for load operations"""
    records_loaded: int = 0
    records_failed: int = 0
    load_start_time: Optional[datetime] = None
    load_end_time: Optional[datetime] = None
    validation_errors: List[Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.validation_errors is None:
            self.validation_errors = []
    
    @property
    def success_rate(self) -> float:
        total = self.records_loaded + self.records_failed
        return (self.records_loaded / total * 100) if total > 0 else 0.0
    
    @property
    def duration_seconds(self) -> float:
        if self.load_start_time and self.load_end_time:
            return (self.load_end_time - self.load_start_time).total_seconds()
        return 0.0


class DataQualityValidator:
    """Validates data quality against source system rules"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.validation_rules = config.get('validation_rules', {})
        self.quality_thresholds = config.get('quality_thresholds', {})
        
    def validate_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a single record against quality rules
        
        Returns:
            Dict with validation results and errors
        """
        errors = []
        warnings = []
        
        # Required field validation
        required_fields = self.validation_rules.get('required_fields', [])
        for field in required_fields:
            if not record.get(field):
                errors.append({
                    'field': field,
                    'rule': 'required',
                    'message': f'Required field {field} is missing or empty'
                })
        
        # Data type validation
        type_rules = self.validation_rules.get('field_types', {})
        for field, expected_type in type_rules.items():
            if field in record and record[field] is not None:
                if not self._validate_type(record[field], expected_type):
                    errors.append({
                        'field': field,
                        'rule': 'type',
                        'message': f'Field {field} has invalid type, expected {expected_type}'
                    })
        
        # Range validation
        range_rules = self.validation_rules.get('ranges', {})
        for field, range_config in range_rules.items():
            if field in record and record[field] is not None:
                value = record[field]
                if 'min' in range_config and value < range_config['min']:
                    errors.append({
                        'field': field,
                        'rule': 'range',
                        'message': f'Field {field} value {value} below minimum {range_config["min"]}'
                    })
                if 'max' in range_config and value > range_config['max']:
                    errors.append({
                        'field': field,
                        'rule': 'range',
                        'message': f'Field {field} value {value} above maximum {range_config["max"]}'
                    })
        
        # Pattern validation
        pattern_rules = self.validation_rules.get('patterns', {})
        for field, pattern in pattern_rules.items():
            if field in record and record[field] is not None:
                import re
                if not re.match(pattern, str(record[field])):
                    errors.append({
                        'field': field,
                        'rule': 'pattern',
                        'message': f'Field {field} does not match required pattern'
                    })
        
        # Business rule validation
        if not self._validate_business_rules(record):
            errors.append({
                'field': 'business_rules',
                'rule': 'business_logic',
                'message': 'Record failed business rule validation'
            })
        
        # Referential integrity
        if not self._validate_referential_integrity(record):
            warnings.append({
                'field': 'referential_integrity',
                'rule': 'reference',
                'message': 'Potential referential integrity issue detected'
            })
        
        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings,
            'record': record
        }
    
    def _validate_type(self, value: Any, expected_type: str) -> bool:
        """Validate value type"""
        type_map = {
            'string': str,
            'integer': int,
            'decimal': (int, float),
            'date': (str, datetime),
            'boolean': bool
        }
        expected = type_map.get(expected_type)
        if expected:
            return isinstance(value, expected)
        return True
    
    def _validate_business_rules(self, record: Dict[str, Any]) -> bool:
        """Validate business-specific rules"""
        # Order total must match line items
        if 'order_total' in record and 'line_items' in record:
            calculated_total = sum(
                item.get('quantity', 0) * item.get('unit_price', 0)
                for item in record.get('line_items', [])
            )
            if abs(calculated_total - record['order_total']) > 0.01:
                return False
        
        # Order date must be before or equal to ship date
        if 'order_date' in record and 'ship_date' in record:
            if record['ship_date'] and record['order_date']:
                if record['ship_date'] < record['order_date']:
                    return False
        
        # Quantity must be positive
        if 'quantity' in record:
            if record['quantity'] <= 0:
                return False
        
        return True
    
    def _validate_referential_integrity(self, record: Dict[str, Any]) -> bool:
        """Validate referential integrity (simplified check)"""
        # In production, this would check against actual reference tables
        # For now, just validate that foreign keys are present
        foreign_keys = self.validation_rules.get('foreign_keys', [])
        for fk in foreign_keys:
            if fk in record and not record[fk]:
                return False
        return True
    
    def validate_batch(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate a batch of records
        
        Returns:
            Dict with batch validation results
        """
        results = {
            'total_records': len(records),
            'valid_records': 0,
            'invalid_records': 0,
            'records_with_warnings': 0,
            'validation_details': []
        }
        
        for record in records:
            validation_result = self.validate_record(record)
            results['validation_details'].append(validation_result)
            
            if validation_result['valid']:
                results['valid_records'] += 1
            else:
                results['invalid_records'] += 1
            
            if validation_result['warnings']:
                results['records_with_warnings'] += 1
        
        # Check quality thresholds
        quality_rate = (results['valid_records'] / results['total_records'] * 100) if results['total_records'] > 0 else 0
        min_quality = self.quality_thresholds.get('min_quality_rate', 95.0)
        
        results['quality_rate'] = quality_rate
        results['meets_threshold'] = quality_rate >= min_quality
        
        return results


class SalesOrderLoader:
    """Loads processed sales orders to target systems"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.load_config = config.get('load', {})
        self.validator = DataQualityValidator(config)
        self.metrics = LoadMetrics()
        
    def setup_nifi_load_flow(self, process_group: ProcessGroupEntity) -> Dict[str, Any]:
        """
        Setup NiFi processors for loading data
        
        Returns:
            Dict with processor IDs
        """
        logger.info("Setting up NiFi load flow")
        
        processors = {}
        
        # Validate Records processor
        validate_config = ProcessorConfigDTO()
        validate_config.properties = {
            'validation-strategy': 'schema-based',
            'schema-access-strategy': 'schema-name',
            'schema-name': self.load_config.get('validation_schema', 'sales-order-schema'),
            'allow-extra-fields': 'true'
        }
        
        validate_processor = nipyapi.canvas.create_processor(
            parent_pg=process_group,
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.ValidateRecord'),
            location=(400.0, 200.0),
            name='Validate Sales Orders',
            config=validate_config
        )
        processors['validate'] = validate_processor
        
        # Route on validation result
        route_config = ProcessorConfigDTO()
        route_config.properties = {
            'Routing Strategy': 'Route to Property name'
        }
        route_config.auto_terminated_relationships = []
        
        route_processor = nipyapi.canvas.create_processor(
            parent_pg=process_group,
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.RouteOnAttribute'),
            location=(600.0, 200.0),
            name='Route Valid/Invalid',
            config=route_config
        )
        processors['route'] = route_processor
        
        # PutDatabaseRecord for valid records
        db_config = ProcessorConfigDTO()
        db_config.properties = {
            'record-reader-factory': 'JsonTreeReader',
            'statement-type': 'INSERT',
            'database-connection-pooling-service': self.load_config.get('db_connection_pool', 'DBCPConnectionPool'),
            'table-name': self.load_config.get('target_table', 'sales_orders'),
            'translate-field-names': 'true',
            'field-containing-sql': '',
            'allow-multiple-statements': 'false'
        }
        
        put_db_processor = nipyapi.canvas.create_processor(
            parent_pg=process_group,
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.PutDatabaseRecord'),
            location=(800.0, 150.0),
            name='Load to Database',
            config=db_config
        )
        processors['put_db'] = put_db_processor
        
        # PutFile for invalid records (error handling)
        error_config = ProcessorConfigDTO()
        error_config.properties = {
            'Directory': self.load_config.get('error_directory', '/data/errors/sales_orders'),
            'Conflict Resolution Strategy': 'replace',
            'Create Missing Directories': 'true'
        }
        
        error_processor = nipyapi.canvas.create_processor(
            parent_pg=process_group,
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.PutFile'),
            location=(800.0, 300.0),
            name='Write Validation Errors',
            config=error_config
        )
        processors['error'] = error_processor
        
        # UpdateAttribute to add load metadata
        update_config = ProcessorConfigDTO()
        update_config.properties = {
            'load_timestamp': '${now():format("yyyy-MM-dd HH:mm:ss")}',
            'load_batch_id': '${UUID()}',
            'source_system': self.load_config.get('source_system', 'informatica_migration')
        }
        
        update_processor = nipyapi.canvas.create_processor(
            parent_pg=process_group,
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.attributes.UpdateAttribute'),
            location=(200.0, 200.0),
            name='Add Load Metadata',
            config=update_config
        )
        processors['update_attr'] = update_processor
        
        # Create connections
        nipyapi.canvas.create_connection(
            update_processor,
            validate_processor,
            ['success']
        )
        
        nipyapi.canvas.create_connection(
            validate_processor,
            route_processor,
            ['valid', 'invalid']
        )
        
        nipyapi.canvas.create_connection(
            route_processor,
            put_db_processor,
            ['valid']
        )
        
        nipyapi.canvas.create_connection(
            route_processor,
            error_processor,
            ['invalid', 'unmatched']
        )
        
        # Auto-terminate success relationships
        put_db_processor.component.config.auto_terminated_relationships = ['success']
        error_processor.component.config.auto_terminated_relationships = ['success']
        
        logger.info(f"Created {len(processors)} load processors")
        return processors
    
    def load_records(self, records: List[Dict[str, Any]], validate: bool = True) -> LoadMetrics:
        """
        Load records to target system with optional validation
        
        Args:
            records: List of records to load
            validate: Whether to validate before loading
            
        Returns:
            LoadMetrics with load results
        """
        self.metrics = LoadMetrics()
        self.metrics.load_start_time = datetime.now()
        
        logger.info(f"Starting load of {len(records)} records")
        
        try:
            # Validate if requested
            if validate:
                validation_results = self.validator.validate_batch(records)
                logger.info(f"Validation complete: {validation_results['valid_records']} valid, "
                           f"{validation_results['invalid_records']} invalid")
                
                if not validation_results['meets_threshold']:
                    logger.error(f"Data quality below threshold: {validation_results['quality_rate']:.2f}%")
                    self.metrics.validation_errors.append({
                        'type': 'quality_threshold',
                        'message': 'Batch quality below acceptable threshold',
                        'details': validation_results
                    })
                    
                    if self.load_config.get('fail_on_quality_threshold', True):
                        raise ValueError("Data quality below acceptable threshold")
                
                # Filter to valid records only
                valid_records = [
                    detail['record'] 
                    for detail in validation_results['validation_details'] 
                    if detail['valid']
                ]
                
                # Store validation errors
                for detail in validation_results['validation_details']:
                    if not detail['valid']:
                        self.metrics.validation_errors.extend(detail['errors'])
                
                records = valid_records
            
            # Load records in batches
            batch_size = self.load_config.get('batch_size', 1000)
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                try:
                    self._load_batch(batch)
                    self.metrics.records_loaded += len(batch)
                    logger.info(f"Loaded batch {i // batch_size + 1}: {len(batch)} records")
                except Exception as e:
                    logger.error(f"Failed to load batch {i // batch_size + 1}: {str(e)}")
                    self.metrics.records_failed += len(batch)
                    self.metrics.validation_errors.append({
                        'type': 'load_error',
                        'batch': i // batch_size + 1,
                        'message': str(e)
                    })
            
            self.metrics.load_end_time = datetime.now()
            
            logger.info(f"Load complete: {self.metrics.records_loaded} loaded, "
                       f"{self.metrics.records_failed} failed, "
                       f"success rate: {self.metrics.success_rate:.2f}%")
            
            return self.metrics
            
        except Exception as e:
            self.metrics.load_end_time = datetime.now()
            logger.error(f"Load failed: {str(e)}")
            raise
    
    def _load_batch(self, batch: List[Dict[str, Any]]):
        """Load a batch of records (implementation depends on target system)"""
        # This is a placeholder - actual implementation would depend on target system
        # For database: use bulk insert
        # For API: use batch API calls
        # For file: write to file system
        
        target_type = self.load_config.get('target_type', 'database')
        
        if target_type == 'database':
            self._load_to_database(batch)
        elif target_type == 'api':
            self._load_to_api(batch)
        elif target_type == 'file':
            self._load_to_file(batch)
        else:
            raise ValueError(f"Unsupported target type: {target_type}")
    
    def _load_to_database(self, batch: List[Dict[str, Any]]):
        """Load batch to database"""
        # In production, this would use actual database connection
        logger.debug(f"Loading {len(batch)} records to database")
        # Simulated database load
        pass
    
    def _load_to_api(self, batch: List[Dict[str, Any]]):
        """Load batch via API"""
        logger.debug(f"Loading {len(batch)} records via API")
        # Simulated API load
        pass
    
    def _load_to_file(self, batch: List[Dict[str, Any]]):
        """Load batch to file"""
        logger.debug(f"Loading {len(batch)} records to file")
        # Simulated file load
        pass
    
    def reconcile_with_source(self, source_records: List[Dict[str, Any]], 
                             loaded_records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Reconcile loaded data with source system
        
        Returns:
            Dict with reconciliation results
        """
        logger.info("Starting data reconciliation")
        
        results = {
            'source_count': len(source_records),
            'loaded_count': len(loaded_records),
            'match_count': 0,
            'mismatch_count': 0,
            'missing_in_target': [],
            'extra_in_target': [],
            'field_mismatches': []
        }
        
        # Create lookup dictionaries
        key_field = self.load_config.get('reconciliation_key', 'order_id')
        source_dict = {rec.get(key_field): rec for rec in source_records}
        loaded_dict = {rec.get(key_field): rec for rec in loaded_records}
        
        # Find missing and extra records
        source_keys = set(source_dict.keys())
        loaded_keys = set(loaded_dict.keys())
        
        results['missing_in_target'] = list(source_keys - loaded_keys)
        results['extra_in_target'] = list(loaded_keys - source_keys)
        
        # Compare matching records
        compare_fields = self.load_config.get('reconciliation_fields', [])
        for key in source_keys & loaded_keys:
            source_rec = source_dict[key]
            loaded_rec = loaded_dict[key]
            
            match = True
            mismatches = []
            
            for field in compare_fields:
                source_val = source_rec.get(field)
                loaded_val = loaded_rec.get(field)
                
                if source_val != loaded_val:
                    match = False
                    mismatches.append({
                        'key': key,
                        'field': field,
                        'source_value': source_val,
                        'loaded_value': loaded_val
                    })
            
            if match:
                results['match_count'] += 1
            else:
                results['mismatch_count'] += 1
                results['field_mismatches'].extend(mismatches)
        
        # Calculate reconciliation rate
        total_compared = len(source_keys & loaded_keys)
        results['reconciliation_rate'] = (results['match_count'] / total_compared * 100) if total_compared > 0 else 0.0
        
        logger.info(f"Reconciliation complete: {results['match_count']} matches, "
                   f"{results['mismatch_count']} mismatches, "
                   f"rate: {results['reconciliation_rate']:.2f}%")
        
        return results


def create_load_process_group(parent_pg_id: str, config: Dict[str, Any]) -> ProcessGroupEntity:
    """
    Create a process group for sales order loading
    
    Args:
        parent_pg_id: Parent process group ID
        config: Configuration dictionary
        
    Returns:
        Created process group entity
    """
    logger.info("Creating sales order load process group")
    
    # Create process group
    pg = nipyapi.canvas.create_process_group(
        parent_pg=nipyapi.canvas.get_process_group(parent_pg_id, 'id'),
        name='Sales Order Load',
        location=(400.0, 400.0)
    )
    
    # Setup load flow
    loader = SalesOrderLoader(config)
    processors = loader.setup_nifi_load_flow(pg)
    
    logger.info(f"Created load process group with {len(processors)} processors")
    
    return pg


===FILE: src/testing.py===
"""
Sales Order Testing Module
Comprehensive testing framework for sales order processing
"""
import logging
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime
from dataclasses import dataclass, field
import pandas as pd
import json

logger = logging.getLogger(__name__)


@dataclass
class TestResult:
    """Result of a single test"""
    test_name: str
    test_type: str
    passed: bool
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    execution_time: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class TestSuite:
    """Collection of test results"""
    suite_name: str
    results: List[TestResult] = field(default_factory=list)
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    
    @property
    def total_tests(self) -> int:
        return len(self.results)
    
    @property
    def passed_tests(self) -> int:
        return sum(1 for r in self.results if r.passed)
    
    @property
    def failed_tests(self) -> int:
        return sum(1 for r in self.results if not r.passed)
    
    @property
    def pass_rate(self) -> float:
        return (self.passed_tests / self.total_tests * 100) if self.total_tests > 0 else 0.0
    
    @property
    def duration_seconds(self) -> float:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0
    
    def add_result(self, result: TestResult):
        """Add a test result to the suite"""
        self.results.append(result)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert test suite to dictionary"""
        return {
            'suite_name': self.suite_name,
            'total_tests': self.total_tests,
            'passed_tests': self.passed_tests,
            'failed_tests': self.failed_tests,
            'pass_rate': self.pass_rate,
            'duration_seconds': self.duration_seconds,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'results': [
                {
                    'test_name': r.test_name,
                    'test_type': r.test_type,
                    'passed': r.passed,
                    'message': r.message,
                    'execution_time': r.execution_time,
                    'details': r.details
                }
                for r in self.results
            ]
        }


class SalesOrderTester:
    """Comprehensive testing for sales order processing"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.test_config = config.get('testing', {})
        self.test_suite = TestSuite(suite_name='Sales Order Processing Tests')
        
    def run_all_tests(self, source_data: List[Dict[str, Any]], 
                     transformed_data: List[Dict[str, Any]],
                     loaded_data: List[Dict[str, Any]]) -> TestSuite:
        """
        Run all test categories
        
        Args:
            source_data: Original source records
            transformed_data: Transformed records
            loaded_data: Loaded records from target
            
        Returns:
            TestSuite with all results
        """
        logger.info("Starting comprehensive test suite")
        self.test_suite.start_time = datetime.now()
        
        try:
            # Data quality tests
            self.run_data_quality_tests(source_data)
            
            # Transformation tests
            self.run_transformation_tests(source_data, transformed_data)
            
            # Load validation tests
            self.run_load_validation_tests(transformed_data, loaded_data)
            
            # Business rule tests
            self.run_business_rule_tests(loaded_data)
            
            # Performance tests
            self.run_performance_tests()
            
            # Integration tests
            self.run_integration_tests(source_data, loaded_data)
            
        finally:
            self.test_suite.end_time = datetime.now()
        
        logger.info(f"Test suite complete: {self.test_suite.passed_tests}/{self.test_suite.total_tests} passed "
                   f"({self.test_suite.pass_rate:.2f}%)")
        
        return self.test_suite
    
    def run_data_quality_tests(self, data: List[Dict[str, Any]]):
        """Run data quality tests"""
        logger.info("Running data quality tests")
        
        # Test: No null values in required fields
        required_fields = self.test_config.get('required_fields', [])
        null_count = 0
        for record in data:
            for field in required_fields:
                if not record.get(field):
                    null_count += 1
        
        self.test_suite.add_result(TestResult(
            test_name='No Nulls in Required Fields',
            test_type='data_quality',
            passed=null_count == 0,
            message=f'Found {null_count} null values in required fields',
            details={'null_count': null_count, 'required_fields': required_fields}
        ))
        
        # Test: Data type consistency
        type_errors = self._check_data_types(data)
        self.test_suite.add_result(TestResult(
            test_name='Data Type Consistency',
            test_type='data_quality',
            passed=len(type_errors) == 0,
            message=f'Found {len(type_errors)} type inconsistencies',
            details={'type_errors': type_errors}
        ))
        
        # Test: Value ranges
        range_errors = self._check_value_ranges(data)
        self.test_suite.add_result(TestResult(
            test_name='Value Range Validation',
            test_type='data_quality',
            passed=len(range_errors) == 0,
            message=f'Found {len(range_errors)} values out of range',
            details={'range_errors': range_errors}
        ))
        
        # Test: Duplicate detection
        duplicates = self._check_duplicates(data)
        self.test_suite.add_result(TestResult(
            test_name='No Duplicate Records',
            test_type='data_quality',
            passed=len(duplicates) == 0,
            message=f'Found {len(duplicates)} duplicate records',
            details={'duplicates': duplicates}
        ))
    
    def run_transformation_tests(self, source: List[Dict[str, Any]], 
                                 transformed: List[Dict[str, Any]]):
        """Run transformation logic tests"""
        logger.info("Running transformation tests")
        
        # Test: Record count preservation
        count_match = len(source) == len(transformed)
        self.test_suite.add_result(TestResult(
            test_name='Record Count Preservation',
            test_type='transformation',
            passed=count_match,
            message=f'Source: {len(source)}, Transformed: {len(transformed)}',
            details={'source_count': len(source), 'transformed_count': len(transformed)}
        ))
        
        # Test: Field mapping correctness
        mapping_errors = self._validate_field_mappings(source, transformed)
        self.test_suite.add_result(TestResult(
            test_name='Field Mapping Correctness',
            test_type='transformation',
            passed=len(mapping_errors) == 0,
            message=f'Found {len(mapping_errors)} mapping errors',
            details={'mapping_errors': mapping_errors}
        ))
        
        # Test: Calculated fields
        calc_errors = self._validate_calculated_fields(transformed)
        self.test_suite.add_result(TestResult(
            test_name='Calculated Field Accuracy',
            test_type='transformation',
            passed=len(calc_errors) == 0,
            message=f'Found {len(calc_errors)} calculation errors',
            details={'calculation_errors': calc_errors}
        ))
        
        # Test: Data enrichment
        enrichment_success = self._validate_enrichment(source, transformed)
        self.test_suite.add_result(TestResult(
            test_name='Data Enrichment Success',
            test_type='transformation',
            passed=enrichment_success,
            message='Data enrichment validation',
            details={'enrichment_validated': enrichment_success}
        ))
    
    def run_load_validation_tests(self, transformed: List[Dict[str, Any]], 
                                  loaded: List[Dict[str, Any]]):
        """Run load validation tests"""
        logger.info("Running load validation tests")
        
        # Test: All records loaded
        all_loaded = len(transformed) == len(loaded)
        self.test_suite.add_result(TestResult(
            test_name='All Records Loaded',
            test_type='load_validation',
            passed=all_loaded,
            message=f'Transformed: {len(transformed)}, Loaded: {len(loaded)}',
            details={'transformed_count': len(transformed), 'loaded_count': len(loaded)}
        ))
        
        # Test: Data integrity after load
        integrity_errors = self._check_load_integrity(transformed, loaded)
        self.test_suite.add_result(TestResult(
            test_name='Data Integrity After Load',
            test_type='load_validation',
            passed=len(integrity_errors) == 0,
            message=f'Found {len(integrity_errors)} integrity issues',
            details={'integrity_errors': integrity_errors}
        ))
        
        # Test: No data truncation
        truncation_errors = self._check_truncation(transformed, loaded)
        self.test_suite.add_result(TestResult(
            test_name='No Data Truncation