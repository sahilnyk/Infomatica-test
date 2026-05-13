"""
Validation module for customer master data quality checks.
Implements business rules and data quality validations.
"""
import re
import logging
from typing import Dict, Any, List, Tuple
from datetime import datetime
from enum import Enum

logger = logging.getLogger(__name__)


class ValidationSeverity(Enum):
    """Validation error severity levels."""
    CRITICAL = "CRITICAL"
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"


class ValidationResult:
    """Container for validation results."""
    
    def __init__(self):
        self.is_valid = True
        self.errors = []
        self.warnings = []
        self.info = []
        self.metrics = {}
        
    def add_error(self, field: str, message: str, severity: ValidationSeverity = ValidationSeverity.ERROR):
        """Add validation error."""
        error_entry = {
            'field': field,
            'message': message,
            'severity': severity.value,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        if severity == ValidationSeverity.CRITICAL or severity == ValidationSeverity.ERROR:
            self.is_valid = False
            self.errors.append(error_entry)
        elif severity == ValidationSeverity.WARNING:
            self.warnings.append(error_entry)
        else:
            self.info.append(error_entry)
            
    def get_all_issues(self) -> List[Dict[str, Any]]:
        """Get all validation issues."""
        return self.errors + self.warnings + self.info
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert validation result to dictionary."""
        return {
            'is_valid': self.is_valid,
            'error_count': len(self.errors),
            'warning_count': len(self.warnings),
            'info_count': len(self.info),
            'errors': self.errors,
            'warnings': self.warnings,
            'info': self.info,
            'metrics': self.metrics
        }


class CustomerDataValidator:
    """Validate customer master data against business rules."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize validator with configuration.
        
        Args:
            config: Configuration dictionary containing validation rules
        """
        self.config = config
        self.validation_rules = config.get('validation_rules', {})
        self.required_fields = config.get('required_fields', [])
        self.field_patterns = config.get('field_patterns', {})
        self.valid_statuses = config.get('valid_statuses', [])
        self.valid_countries = config.get('valid_countries', [])
        self.valid_states = config.get('valid_states', [])
        
    def validate_customer(self, customer_data: Dict[str, Any]) -> ValidationResult:
        """
        Validate customer master data record.
        
        Args:
            customer_data: Customer data dictionary
            
        Returns:
            ValidationResult object containing validation results
        """
        result = ValidationResult()
        
        # Required field validation
        self._validate_required_fields(customer_data, result)
        
        # Field format validation
        self._validate_field_formats(customer_data, result)
        
        # Business rule validation
        self._validate_business_rules(customer_data, result)
        
        # Data quality checks
        self._validate_data_quality(customer_data, result)
        
        # Calculate metrics
        result.metrics = self._calculate_metrics(customer_data, result)
        
        logger.info(f"Validation completed for customer {customer_data.get('customer_id')}: "
                   f"Valid={result.is_valid}, Errors={len(result.errors)}, "
                   f"Warnings={len(result.warnings)}")
        
        return result
        
    def _validate_required_fields(self, data: Dict[str, Any], result: ValidationResult):
        """Validate required fields are present and not empty."""
        for field in self.required_fields:
            value = data.get(field)
            if value is None or (isinstance(value, str) and not value.strip()):
                result.add_error(
                    field,
                    f"Required field '{field}' is missing or empty",
                    ValidationSeverity.CRITICAL
                )
                
    def _validate_field_formats(self, data: Dict[str, Any], result: ValidationResult):
        """Validate field formats using regex patterns."""
        
        # Customer ID format
        customer_id = data.get('customer_id', '')
        if customer_id:
            pattern = self.field_patterns.get('customer_id', r'^[A-Z0-9]{10}$')
            if not re.match(pattern, customer_id):
                result.add_error(
                    'customer_id',
                    f"Customer ID '{customer_id}' does not match required format",
                    ValidationSeverity.ERROR
                )
                
        # Email format
        email = data.get('email', '')
        if email:
            email_pattern = self.field_patterns.get('email', 
                r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
            if not re.match(email_pattern, email):
                result.add_error(
                    'email',
                    f"Email '{email}' is not in valid format",
                    ValidationSeverity.ERROR
                )
                
        # Phone format
        phone = data.get('phone', '')
        if phone:
            phone_pattern = self.field_patterns.get('phone', r'^\+?[\d\s\-\(\)]{10,20}$')
            if not re.match(phone_pattern, phone):
                result.add_error(
                    'phone',
                    f"Phone '{phone}' is not in valid format",
                    ValidationSeverity.WARNING
                )
                
        # Zip code format
        zip_code = data.get('zip_code', '')
        country = data.get('country', '')
        if zip_code and country == 'US':
            zip_pattern = self.field_patterns.get('zip_code_us', r'^\d{5}(-\d{4})?$')
            if not re.match(zip_pattern, zip_code):
                result.add_error(
                    'zip_code',
                    f"US zip code '{zip_code}' is not in valid format",
                    ValidationSeverity.ERROR
                )
                
    def _validate_business_rules(self, data: Dict[str, Any], result: ValidationResult):
        """Validate business rules."""
        
        # Status validation
        status = data.get('status', '')
        if status and status not in self.valid_statuses:
            result.add_error(
                'status',
                f"Status '{status}' is not valid. Must be one of: {', '.join(self.valid_statuses)}",
                ValidationSeverity.ERROR
            )
            
        # Country validation
        country = data.get('country', '')
        if country and country not in self.valid_countries:
            result.add_error(
                'country',
                f"Country code '{country}' is not valid",
                ValidationSeverity.ERROR
            )
            
        # State validation for US addresses
        if country == 'US':
            state = data.get('state', '')
            if state and state not in self.valid_states:
                result.add_error(
                    'state',
                    f"State code '{state}' is not valid for US addresses",
                    ValidationSeverity.ERROR
                )
                
        # Name validation
        first_name = data.get('first_name', '')
        last_name = data.get('last_name', '')
        
        if first_name and len(first_name) < 2:
            result.add_error(
                'first_name',
                "First name must be at least 2 characters",
                ValidationSeverity.WARNING
            )
            
        if last_name and len(last_name) < 2:
            result.add_error(
                'last_name',
                "Last name must be at least 2 characters",
                ValidationSeverity.WARNING
            )
            
        # Registration date validation
        reg_date = data.get('registration_date', '')
        if reg_date:
            try:
                parsed_date = datetime.fromisoformat(reg_date.replace('Z', '+00:00'))
                if parsed_date > datetime.utcnow():
                    result.add_error(
                        'registration_date',
                        "Registration date cannot be in the future",
                        ValidationSeverity.ERROR
                    )
            except (ValueError, AttributeError):
                result.add_error(
                    'registration_date',
                    f"Registration date '{reg_date}' is not in valid ISO format",
                    ValidationSeverity.ERROR
                )
                
    def _validate_data_quality(self, data: Dict[str, Any], result: ValidationResult):
        """Perform data quality checks."""
        
        # Check for suspicious patterns
        email = data.get('email', '')
        if email and ('test' in email.lower() or 'dummy' in email.lower()):
            result.add_error(
                'email',
                "Email appears to be a test/dummy value",
                ValidationSeverity.WARNING
            )
            
        # Check for placeholder values
        placeholder_patterns = ['xxx', 'n/a', 'na', 'null', 'none', 'unknown']
        for field in ['first_name', 'last_name', 'city', 'address_line1']:
            value = str(data.get(field, '')).lower()
            if any(pattern in value for pattern in placeholder_patterns):
                result.add_error(
                    field,
                    f"Field '{field}' contains placeholder value",
                    ValidationSeverity.WARNING
                )
                
        # Check for data completeness
        address_fields = ['address_line1', 'city', 'state', 'zip_code', 'country']
        filled_address_fields = sum(1 for f in address_fields if data.get(f))
        
        if 0 < filled_address_fields < len(address_fields):
            result.add_error(
                'address',
                "Address information is incomplete",
                ValidationSeverity.WARNING
            )
            
        # Check for duplicate spaces
        for field in ['first_name', 'last_name', 'city', 'address_line1']:
            value = data.get(field, '')
            if '  ' in value:
                result.add_error(
                    field,
                    f"Field '{field}' contains multiple consecutive spaces",
                    ValidationSeverity.INFO
                )
                
    def _calculate_metrics(self, data: Dict[str, Any], result: ValidationResult) -> Dict[str, Any]:
        """Calculate data quality metrics."""
        total_fields = len(data)
        filled_fields = sum(1 for v in data.values() if v and str(v).strip())
        
        return {
            'completeness_score': round((filled_fields / total_fields) * 100, 2) if total_fields > 0 else 0,
            'total_fields': total_fields,
            'filled_fields': filled_fields,
            'empty_fields': total_fields - filled_fields,
            'validation_timestamp': datetime.utcnow().isoformat()
        }
        
    def validate_batch(self, customer_records: List[Dict[str, Any]]) -> List[Tuple[Dict[str, Any], ValidationResult]]:
        """
        Validate multiple customer records.
        
        Args:
            customer_records: List of customer data dictionaries
            
        Returns:
            List of tuples containing (customer_data, validation_result)
        """
        results = []
        
        for record in customer_records:
            validation_result = self.validate_customer(record)
            results.append((record, validation_result))
            
        logger.info(f"Batch validation completed: {len(results)} records processed")
        return results