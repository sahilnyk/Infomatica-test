"""
Validation module for address data quality checks.
Implements completeness, format, and referential integrity validation.
"""

import logging
import re
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import nipyapi
from nipyapi.nifi import ProcessorConfigDTO, ProcessGroupEntity

logger = logging.getLogger(__name__)


class ValidationResult(Enum):
    """Validation result types."""
    VALID = "valid"
    INVALID = "invalid"
    INCOMPLETE = "incomplete"
    ORPHANED = "orphaned"


@dataclass
class ValidationError:
    """Represents a validation error."""
    field: str
    error_type: str
    message: str
    value: Optional[str] = None


class AddressValidator:
    """Validates address data for completeness and correctness."""
    
    def __init__(self, config: Dict[str, Any], canvas: Any):
        """
        Initialize the validator.
        
        Args:
            config: Configuration dictionary
            canvas: NiFi canvas object
        """
        self.config = config
        self.canvas = canvas
        self.validation_config = config.get('validation', {})
        self.completeness_config = self.validation_config.get('address_completeness', {})
        
    def create_validation_flow(self, process_group: ProcessGroupEntity) -> Dict[str, Any]:
        """
        Create the validation flow in NiFi.
        
        Args:
            process_group: Parent process group
            
        Returns:
            Dictionary containing processor references
        """
        logger.info("Creating validation flow for address data")
        
        processors = {}
        
        try:
            # Create referential integrity checker
            processors['check_referential_integrity'] = self._create_referential_integrity_checker(
                process_group,
                position={'x': 700, 'y': 200}
            )
            
            # Create address completeness validator
            processors['validate_completeness'] = self._create_completeness_validator(
                process_group,
                position={'x': 1000, 'y': 200}
            )
            
            # Create format validators
            processors['validate_zip_code'] = self._create_format_validator(
                process_group,
                "Validate Zip Code Format",
                'zip_code',
                position={'x': 1300, 'y': 100}
            )
            
            processors['validate_state'] = self._create_format_validator(
                process_group,
                "Validate State Code",
                'state',
                position={'x': 1300, 'y': 300}
            )
            
            processors['validate_country'] = self._create_format_validator(
                process_group,
                "Validate Country Code",
                'country',
                position={'x': 1300, 'y': 500}
            )
            
            # Create route processor for validation results
            processors['route_validation_results'] = self._create_route_processor(
                process_group,
                position={'x': 1600, 'y': 200}
            )
            
            logger.info("Validation flow created successfully")
            return processors
            
        except Exception as e:
            logger.error(f"Failed to create validation flow: {str(e)}")
            raise
    
    def _create_referential_integrity_checker(
        self,
        process_group: ProcessGroupEntity,
        position: Dict[str, int]
    ) -> Any:
        """Create processor to check referential integrity with customer master."""
        
        # Use LookupRecord processor to join with customer data
        processor = nipyapi.canvas.create_processor(
            parent_pg=process_group,
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.LookupRecord'),
            location=(position['x'], position['y']),
            name="Check Customer Reference",
            config=ProcessorConfigDTO(
                properties={
                    'Record Reader': 'CSVReader',
                    'Record Writer': 'CSVRecordSetWriter',
                    'Lookup Service': 'DistributedMapCacheLookupService',
                    'Result RecordPath': '/customer_exists',
                    'Routing Strategy': 'Route to Property name',
                    'Record Result Contents': 'Insert Entire Record',
                    'customer_id': '/customer_id'
                },
                auto_terminated_relationships=[],
                scheduling_period='0 sec',
                scheduling_strategy='EVENT_DRIVEN'
            )
        )
        
        logger.info("Created referential integrity checker processor")
        return processor
    
    def _create_completeness_validator(
        self,
        process_group: ProcessGroupEntity,
        position: Dict[str, int]
    ) -> Any:
        """Create processor to validate address completeness."""
        
        required_fields = self.completeness_config.get('required_fields', [])
        
        # Build validation expression
        validation_expressions = []
        for field in required_fields:
            validation_expressions.append(f"isNotEmpty(/{field})")
        
        validation_expr = " and ".join(validation_expressions)
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=process_group,
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.QueryRecord'),
            location=(position['x'], position['y']),
            name="Validate Address Completeness",
            config=ProcessorConfigDTO(
                properties={
                    'Record Reader': 'CSVReader',
                    'Record Writer': 'CSVRecordSetWriter',
                    'complete': f"SELECT * FROM FLOWFILE WHERE {validation_expr}",
                    'incomplete': f"SELECT * FROM FLOWFILE WHERE NOT ({validation_expr})",
                    'Include Zero Record FlowFiles': 'false'
                },
                auto_terminated_relationships=[],
                scheduling_period='0 sec',
                scheduling_strategy='EVENT_DRIVEN'
            )
        )
        
        logger.info("Created completeness validator processor")
        return processor
    
    def _create_format_validator(
        self,
        process_group: ProcessGroupEntity,
        name: str,
        field: str,
        position: Dict[str, int]
    ) -> Any:
        """Create processor to validate field format."""
        
        # Get validation patterns
        patterns = self._get_validation_patterns(field)
        
        # Create UpdateRecord processor with validation logic
        processor = nipyapi.canvas.create_processor(
            parent_pg=process_group,
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.UpdateRecord'),
            location=(position['x'], position['y']),
            name=name,
            config=ProcessorConfigDTO(
                properties={
                    'Record Reader': 'CSVReader',
                    'Record Writer': 'CSVRecordSetWriter',
                    'Replacement Value Strategy': 'Record Path Value',
                    f'/validation_{field}_result': self._build_validation_expression(field, patterns),
                    f'/validation_{field}_error': f"concat('Invalid {field} format: ', /{field})"
                },
                auto_terminated_relationships=[],
                scheduling_period='0 sec',
                scheduling_strategy='EVENT_DRIVEN'
            )
        )
        
        logger.info(f"Created format validator processor: {name}")
        return processor
    
    def _get_validation_patterns(self, field: str) -> Dict[str, str]:
        """Get validation patterns for a field."""
        
        if field == 'zip_code':
            return self.completeness_config.get('zip_code_patterns', {})
        elif field == 'state':
            return {'codes': self.completeness_config.get('state_codes', {})}
        elif field == 'country':
            return {'codes': self.completeness_config.get('country_codes', [])}
        
        return {}
    
    def _build_validation_expression(self, field: str, patterns: Dict[str, Any]) -> str:
        """Build RecordPath validation expression."""
        
        if field == 'zip_code':
            # Build regex validation for zip codes based on country
            conditions = []
            for country, pattern in patterns.items():
                conditions.append(
                    f"(equals(/country, '{country}') and matches(/{field}, '{pattern}'))"
                )
            return f"if({' or '.join(conditions)}, 'VALID', 'INVALID')"
        
        elif field == 'state':
            # Validate state codes
            state_codes = patterns.get('codes', {})
            all_codes = []
            for country, codes in state_codes.items():
                all_codes.extend(codes)
            
            codes_list = "', '".join(all_codes)
            return f"if(contains(['{codes_list}'], /{field}), 'VALID', 'INVALID')"
        
        elif field == 'country':
            # Validate country codes
            codes = patterns.get('codes', [])
            codes_list = "', '".join(codes)
            return f"if(contains(['{codes_list}'], /{field}), 'VALID', 'INVALID')"
        
        return "'VALID'"
    
    def _create_route_processor(
        self,
        process_group: ProcessGroupEntity,
        position: Dict[str, int]
    ) -> Any:
        """Create processor to route records based on validation results."""
        
        processor = nipyapi.canvas.create_processor(
            parent_pg=process_group,
            processor=nipyapi.canvas.get_processor_type('org.apache.nifi.processors.standard.RouteOnAttribute'),
            location=(position['x'], position['y']),
            name="Route Validation Results",
            config=ProcessorConfigDTO(
                properties={
                    'Routing Strategy': 'Route to Property name',
                    'valid': "${validation_zip_code_result:equals('VALID'):and("
                            "${validation_state_result:equals('VALID')}):and("
                            "${validation_country_result:equals('VALID')}):and("
                            "${customer_exists:equals('true')})}",
                    'invalid': "${validation_zip_code_result:equals('INVALID'):or("
                              "${validation_state_result:equals('INVALID')}):or("
                              "${validation_country_result:equals('INVALID')})}",
                    'orphaned': "${customer_exists:equals('false')}"
                },
                auto_terminated_relationships=['unmatched'],
                scheduling_period='0 sec',
                scheduling_strategy='EVENT_DRIVEN'
            )
        )
        
        logger.info("Created route processor for validation results")
        return processor
    
    def validate_address_record(self, record: Dict[str, Any]) -> Tuple[ValidationResult, List[ValidationError]]:
        """
        Validate a single address record.
        
        Args:
            record: Address record dictionary
            
        Returns:
            Tuple of (ValidationResult, List of ValidationErrors)
        """
        errors = []
        
        # Check completeness
        completeness_errors = self._validate_completeness(record)
        errors.extend(completeness_errors)
        
        # Check format
        format_errors = self._validate_formats(record)
        errors.extend(format_errors)
        
        # Determine overall result
        if not errors:
            return ValidationResult.VALID, []
        elif any(e.error_type == 'missing_required' for e in errors):
            return ValidationResult.INCOMPLETE, errors
        else:
            return ValidationResult.INVALID, errors
    
    def _validate_completeness(self, record: Dict[str, Any]) -> List[ValidationError]:
        """Validate that required fields are present and non-empty."""
        
        errors = []
        required_fields = self.completeness_config.get('required_fields', [])
        
        for field in required_fields:
            value = record.get(field)
            if not value or (isinstance(value, str) and not value.strip()):
                errors.append(ValidationError(
                    field=field,
                    error_type='missing_required',
                    message=f"Required field '{field}' is missing or empty",
                    value=value
                ))
        
        return errors
    
    def _validate_formats(self, record: Dict[str, Any]) -> List[ValidationError]:
        """Validate field formats."""
        
        errors = []
        
        # Validate zip code
        zip_errors = self._validate_zip_code(record)
        errors.extend(zip_errors)
        
        # Validate state
        state_errors = self._validate_state(record)
        errors.extend(state_errors)
        
        # Validate country
        country_errors = self._validate_country(record)
        errors.extend(country_errors)
        
        return errors
    
    def _validate_zip_code(self, record: Dict[str, Any]) -> List[ValidationError]:
        """Validate zip code format based on country."""
        
        errors = []
        zip_code = record.get('zip_code', '').strip()
        country = record.get('country', '').strip().upper()
        
        if not zip_code:
            return errors
        
        patterns = self.completeness_config.get('zip_code_patterns', {})
        pattern = patterns.get(country)
        
        if pattern and not re.match(pattern, zip_code):
            errors.append(ValidationError(
                field='zip_code',
                error_type='invalid_format',
                message=f"Invalid zip code format for country {country}",
                value=zip_code
            ))
        
        return errors
    
    def _validate_state(self, record: Dict[str, Any]) -> List[ValidationError]:
        """Validate state code."""
        
        errors = []
        state = record.get('state', '').strip().upper()
        country = record.get('country', '').strip().upper()
        
        if not state:
            return errors
        
        state_codes = self.completeness_config.get('state_codes', {})
        valid_codes = state_codes.get(country, [])
        
        if valid_codes and state not in valid_codes:
            errors.append(ValidationError(
                field='state',
                error_type='invalid_code',
                message=f"Invalid state code '{state}' for country {country}",
                value=state
            ))
        
        return errors
    
    def _validate_country(self, record: Dict[str, Any]) -> List[ValidationError]:
        """Validate country code."""
        
        errors = []
        country = record.get('country', '').strip().upper()
        
        if not country:
            return errors
        
        valid_codes = self.completeness_config.get('country_codes', [])
        
        if country not in valid_codes:
            errors.append(ValidationError(
                field='country',
                error_type='invalid_code',
                message=f"Invalid country code '{country}'",
                value=country
            ))
        
        return errors