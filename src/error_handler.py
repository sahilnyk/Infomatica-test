"""
Error handling and logging module for customer data validation.
Manages error categorization, logging, and reconciliation reporting.
"""
import json
import logging
from typing import Dict, Any, List
from datetime import datetime
from collections import defaultdict

logger = logging.getLogger(__name__)


class ErrorCategory:
    """Error category constants."""
    DATA_FORMAT = "DATA_FORMAT"
    BUSINESS_RULE = "BUSINESS_RULE"
    DATA_QUALITY = "DATA_QUALITY"
    SYSTEM = "SYSTEM"
    UNKNOWN = "UNKNOWN"


class ErrorHandler:
    """Handle and log validation errors with categorization."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize error handler with configuration.
        
        Args:
            config: Configuration dictionary containing error handling settings
        """
        self.config = config
        self.error_log = []
        self.error_stats = defaultdict(int)
        self.max_errors = config.get('max_errors_per_record', 100)
        
    def log_validation_error(self, customer_id: str, validation_result: Any, 
                           record_data: Dict[str, Any]):
        """
        Log validation errors for a customer record.
        
        Args:
            customer_id: Customer identifier
            validation_result: ValidationResult object
            record_data: Original customer data
        """
        if not validation_result.is_valid:
            error_entry = {
                'customer_id': customer_id,
                'timestamp': datetime.utcnow().isoformat(),
                'error_count': len(validation_result.errors),
                'warning_count': len(validation_result.warnings),
                'errors': validation_result.errors,
                'warnings': validation_result.warnings,
                'record_snapshot': self._sanitize_record(record_data),
                'metrics': validation_result.metrics
            }
            
            self.error_log.append(error_entry)
            
            # Update statistics
            self.error_stats['total_errors'] += len(validation_result.errors)
            self.error_stats['total_warnings'] += len(validation_result.warnings)
            self.error_stats['failed_records'] += 1
            
            # Categorize errors
            for error in validation_result.errors:
                category = self._categorize_error(error)
                self.error_stats[f'category_{category}'] += 1
                
            logger.error(f"Validation failed for customer {customer_id}: "
                        f"{len(validation_result.errors)} errors, "
                        f"{len(validation_result.warnings)} warnings")
                        
    def log_system_error(self, error_type: str, message: str, context: Dict[str, Any] = None):
        """
        Log system-level errors.
        
        Args:
            error_type: Type of system error
            message: Error message
            context: Additional context information
        """
        error_entry = {
            'error_type': error_type,
            'category': ErrorCategory.SYSTEM,
            'message': message,
            'timestamp': datetime.utcnow().isoformat(),
            'context': context or {}
        }
        
        self.error_log.append(error_entry)
        self.error_stats['system_errors'] += 1
        
        logger.error(f"System error: {error_type} - {message}")
        
    def _categorize_error(self, error: Dict[str, Any]) -> str:
        """
        Categorize error based on field and message.
        
        Args:
            error: Error dictionary
            
        Returns:
            Error category string
        """
        field = error.get('field', '')
        message = error.get('message', '').lower()
        
        if 'format' in message or 'pattern' in message or 'match' in message:
            return ErrorCategory.DATA_FORMAT
        elif 'required' in message or 'missing' in message or 'empty' in message:
            return ErrorCategory.DATA_QUALITY
        elif 'valid' in message or 'must be' in message:
            return ErrorCategory.BUSINESS_RULE
        else:
            return ErrorCategory.UNKNOWN
            
    def _sanitize_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Sanitize record data for logging (remove sensitive information).
        
        Args:
            record: Customer record dictionary
            
        Returns:
            Sanitized record dictionary
        """
        sensitive_fields = self.config.get('sensitive_fields', ['email', 'phone'])
        sanitized = record.copy()
        
        for field in sensitive_fields:
            if field in sanitized and sanitized[field]:
                value = str(sanitized[field])
                if field == 'email' and '@' in value:
                    parts = value.split('@')
                    sanitized[field] = f"{parts[0][:2]}***@{parts[1]}"
                elif field == 'phone':
                    sanitized[field] = f"***{value[-4:]}" if len(value) >= 4 else "***"
                else:
                    sanitized[field] = "***REDACTED***"
                    
        return sanitized
        
    def get_error_summary(self) -> Dict[str, Any]:
        """
        Get summary of all errors.
        
        Returns:
            Dictionary containing error summary statistics
        """
        return {
            'total_records_processed': self.error_stats.get('failed_records', 0),
            'total_errors': self.error_stats.get('total_errors', 0),
            'total_warnings': self.error_stats.get('total_warnings', 0),
            'system_errors': self.error_stats.get('system_errors', 0),
            'errors_by_category': {
                'data_format': self.error_stats.get(f'category_{ErrorCategory.DATA_FORMAT}', 0),
                'business_rule': self.error_stats.get(f'category_{ErrorCategory.BUSINESS_RULE}', 0),
                'data_quality': self.error_stats.get(f'category_{ErrorCategory.DATA_QUALITY}', 0),
                'unknown': self.error_stats.get(f'category_{ErrorCategory.UNKNOWN}', 0)
            },
            'timestamp': datetime.utcnow().isoformat()
        }
        
    def get_error_log(self) -> List[Dict[str, Any]]:
        """Get complete error log."""
        return self.error_log
        
    def clear_error_log(self):
        """Clear error log and statistics."""
        self.error_log.clear()
        self.error_stats.clear()
        logger.info("Error log cleared")


class ReconciliationReporter:
    """Generate reconciliation reports for data validation."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize reconciliation reporter.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        
    def generate_report(self, total_records: int, validation_results: List[tuple],
                       error_handler: ErrorHandler) -> Dict[str, Any]:
        """
        Generate comprehensive reconciliation report.
        
        Args:
            total_records: Total number of records processed
            validation_results: List of (record, validation_result) tuples
            error_handler: ErrorHandler instance
            
        Returns:
            Dictionary containing reconciliation report
        """
        valid_records = sum(1 for _, result in validation_results if result.is_valid)
        invalid_records = total_records - valid_records
        
        # Calculate quality scores
        quality_scores = [result.metrics.get('completeness_score', 0) 
                         for _, result in validation_results]
        avg_quality_score = sum(quality_scores) / len(quality_scores) if quality_scores else 0
        
        # Field-level error analysis
        field_errors = defaultdict(int)
        for _, result in validation_results:
            for error in result.errors:
                field_errors[error.get('field', 'unknown')] += 1
                
        report = {
            'report_metadata': {
                'report_id': f"RECON_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
                'generated_at': datetime.utcnow().isoformat(),
                'report_type': 'customer_data_validation'
            },
            'summary': {
                'total_records_processed': total_records,
                'valid_records': valid_records,
                'invalid_records': invalid_records,
                'validation_success_rate': round((valid_records / total_records * 100), 2) if total_records > 0 else 0,
                'average_quality_score': round(avg_quality_score, 2)
            },
            'error_summary': error_handler.get_error_summary(),
            'field_error_analysis': dict(sorted(field_errors.items(), 
                                               key=lambda x: x[1], 
                                               reverse=True)[:10]),
            'quality_metrics': {
                'min_quality_score': round(min(quality_scores), 2) if quality_scores else 0,
                'max_quality_score': round(max(quality_scores), 2) if quality_scores else 0,
                'avg_quality_score': round(avg_quality_score, 2)
            },
            'recommendations': self._generate_recommendations(validation_results, field_errors)
        }
        
        logger.info(f"Reconciliation report generated: {report['report_metadata']['report_id']}")
        return report
        
    def _generate_recommendations(self, validation_results: List[tuple], 
                                 field_errors: Dict[str, int]) -> List[str]:
        """
        Generate recommendations based on validation results.
        
        Args:
            validation_results: List of validation results
            field_errors: Dictionary of field error counts
            
        Returns:
            List of recommendation strings
        """
        recommendations = []
        
        # Check for high error rate
        total = len(validation_results)
        invalid = sum(1 for _, result in validation_results if not result.is_valid)
        
        if total > 0 and (invalid / total) > 0.3:
            recommendations.append(
                "High error rate detected (>30%). Review source data quality and extraction process."
            )
            
        # Check for common field errors
        if field_errors:
            top_error_field = max(field_errors.items(), key=lambda x: x[1])
            if top_error_field[1] > total * 0.2:
                recommendations.append(
                    f"Field '{top_error_field[0]}' has high error rate. "
                    f"Review validation rules and source data for this field."
                )
                
        # Check for data quality issues
        low_quality_count = sum(1 for _, result in validation_results 
                               if result.metrics.get('completeness_score', 100) < 70)
        if low_quality_count > total * 0.2:
            recommendations.append(
                "More than 20% of records have low completeness scores (<70%). "
                "Investigate missing data in source systems."
            )
            
        if not recommendations:
            recommendations.append("Data quality is within acceptable thresholds.")
            
        return recommendations
        
    def export_report(self, report: Dict[str, Any], output_path: str):
        """
        Export report to JSON file.
        
        Args:
            report: Report dictionary
            output_path: Output file path
        """
        try:
            with open(output_path, 'w') as f:
                json.dump(report, f, indent=2)
            logger.info(f"Report exported to {output_path}")
        except Exception as e:
            logger.error(f"Failed to export report: {e}")
            raise