"""
Sales Line Item Load Module
Loads processed line items to target system with validation
"""
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import pandas as pd
from decimal import Decimal, ROUND_HALF_UP
import psycopg2
from psycopg2.extras import execute_batch
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SalesLineItemLoader:
    """Handles loading of processed sales line items with validation"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize loader with configuration"""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.db_config = self.config['database']
        self.load_config = self.config['load']
        self.validation_config = self.config['validation']
        self.connection = None
        self.cursor = None
        
    def connect(self) -> None:
        """Establish database connection"""
        try:
            self.connection = psycopg2.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password']
            )
            self.cursor = self.connection.cursor()
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def disconnect(self) -> None:
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Database connection closed")
    
    def validate_calculations(self, line_items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate line item calculations against business rules
        
        Args:
            line_items: List of processed line items
            
        Returns:
            Validation results with errors and warnings
        """
        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'total_records': len(line_items),
            'valid_records': 0,
            'invalid_records': 0
        }
        
        for idx, item in enumerate(line_items):
            item_errors = []
            item_warnings = []
            
            try:
                # Validate required fields
                required_fields = self.validation_config['required_fields']
                for field in required_fields:
                    if field not in item or item[field] is None:
                        item_errors.append(f"Missing required field: {field}")
                
                # Validate numeric fields
                quantity = Decimal(str(item.get('quantity', 0)))
                unit_price = Decimal(str(item.get('unit_price', 0)))
                discount_percent = Decimal(str(item.get('discount_percent', 0)))
                tax_rate = Decimal(str(item.get('tax_rate', 0)))
                
                # Validate ranges
                if quantity <= 0:
                    item_errors.append(f"Invalid quantity: {quantity}")
                if unit_price < 0:
                    item_errors.append(f"Invalid unit_price: {unit_price}")
                if not (0 <= discount_percent <= 100):
                    item_errors.append(f"Invalid discount_percent: {discount_percent}")
                if not (0 <= tax_rate <= 100):
                    item_errors.append(f"Invalid tax_rate: {tax_rate}")
                
                # Validate calculation: line_total = quantity * unit_price
                expected_line_total = (quantity * unit_price).quantize(
                    Decimal('0.01'), rounding=ROUND_HALF_UP
                )
                actual_line_total = Decimal(str(item.get('line_total', 0)))
                
                if abs(expected_line_total - actual_line_total) > Decimal('0.01'):
                    item_errors.append(
                        f"Line total mismatch: expected {expected_line_total}, "
                        f"got {actual_line_total}"
                    )
                
                # Validate discount calculation
                discount_amount = Decimal(str(item.get('discount_amount', 0)))
                expected_discount = (expected_line_total * discount_percent / 100).quantize(
                    Decimal('0.01'), rounding=ROUND_HALF_UP
                )
                
                if abs(expected_discount - discount_amount) > Decimal('0.01'):
                    item_errors.append(
                        f"Discount amount mismatch: expected {expected_discount}, "
                        f"got {discount_amount}"
                    )
                
                # Validate net amount calculation
                net_amount = Decimal(str(item.get('net_amount', 0)))
                expected_net = (expected_line_total - expected_discount).quantize(
                    Decimal('0.01'), rounding=ROUND_HALF_UP
                )
                
                if abs(expected_net - net_amount) > Decimal('0.01'):
                    item_errors.append(
                        f"Net amount mismatch: expected {expected_net}, "
                        f"got {net_amount}"
                    )
                
                # Validate tax calculation
                tax_amount = Decimal(str(item.get('tax_amount', 0)))
                expected_tax = (expected_net * tax_rate / 100).quantize(
                    Decimal('0.01'), rounding=ROUND_HALF_UP
                )
                
                if abs(expected_tax - tax_amount) > Decimal('0.01'):
                    item_errors.append(
                        f"Tax amount mismatch: expected {expected_tax}, "
                        f"got {tax_amount}"
                    )
                
                # Validate final total
                final_total = Decimal(str(item.get('final_total', 0)))
                expected_final = (expected_net + expected_tax).quantize(
                    Decimal('0.01'), rounding=ROUND_HALF_UP
                )
                
                if abs(expected_final - final_total) > Decimal('0.01'):
                    item_errors.append(
                        f"Final total mismatch: expected {expected_final}, "
                        f"got {final_total}"
                    )
                
                # Check for warnings
                if discount_percent > self.validation_config['max_discount_warning']:
                    item_warnings.append(
                        f"High discount: {discount_percent}%"
                    )
                
                if quantity > self.validation_config['max_quantity_warning']:
                    item_warnings.append(
                        f"High quantity: {quantity}"
                    )
                
                # Record validation status
                if item_errors:
                    validation_results['invalid_records'] += 1
                    validation_results['errors'].append({
                        'record_index': idx,
                        'line_item_id': item.get('line_item_id'),
                        'errors': item_errors
                    })
                else:
                    validation_results['valid_records'] += 1
                
                if item_warnings:
                    validation_results['warnings'].append({
                        'record_index': idx,
                        'line_item_id': item.get('line_item_id'),
                        'warnings': item_warnings
                    })
                    
            except Exception as e:
                validation_results['invalid_records'] += 1
                validation_results['errors'].append({
                    'record_index': idx,
                    'line_item_id': item.get('line_item_id'),
                    'errors': [f"Validation exception: {str(e)}"]
                })
        
        validation_results['valid'] = validation_results['invalid_records'] == 0
        
        return validation_results
    
    def load_line_items(self, line_items: List[Dict[str, Any]], 
                       validate: bool = True) -> Dict[str, Any]:
        """
        Load line items to target database
        
        Args:
            line_items: List of processed line items
            validate: Whether to validate before loading
            
        Returns:
            Load results with statistics
        """
        load_results = {
            'success': False,
            'total_records': len(line_items),
            'loaded_records': 0,
            'failed_records': 0,
            'validation_results': None,
            'load_timestamp': datetime.now().isoformat(),
            'errors': []
        }
        
        try:
            # Validate if requested
            if validate:
                validation_results = self.validate_calculations(line_items)
                load_results['validation_results'] = validation_results
                
                if not validation_results['valid']:
                    logger.error(
                        f"Validation failed: {validation_results['invalid_records']} "
                        f"invalid records"
                    )
                    if not self.load_config['load_invalid_records']:
                        load_results['errors'].append(
                            "Validation failed and load_invalid_records is False"
                        )
                        return load_results
                    else:
                        logger.warning("Loading despite validation errors")
            
            # Connect to database
            self.connect()
            
            # Prepare insert statement
            insert_sql = """
                INSERT INTO sales_line_items (
                    line_item_id, order_id, product_id, product_name,
                    quantity, unit_price, line_total,
                    discount_percent, discount_amount, net_amount,
                    tax_rate, tax_amount, final_total,
                    order_date, customer_id, status,
                    created_date, modified_date
                ) VALUES (
                    %(line_item_id)s, %(order_id)s, %(product_id)s, %(product_name)s,
                    %(quantity)s, %(unit_price)s, %(line_total)s,
                    %(discount_percent)s, %(discount_amount)s, %(net_amount)s,
                    %(tax_rate)s, %(tax_amount)s, %(final_total)s,
                    %(order_date)s, %(customer_id)s, %(status)s,
                    %(created_date)s, %(modified_date)s
                )
                ON CONFLICT (line_item_id) 
                DO UPDATE SET
                    quantity = EXCLUDED.quantity,
                    unit_price = EXCLUDED.unit_price,
                    line_total = EXCLUDED.line_total,
                    discount_percent = EXCLUDED.discount_percent,
                    discount_amount = EXCLUDED.discount_amount,
                    net_amount = EXCLUDED.net_amount,
                    tax_rate = EXCLUDED.tax_rate,
                    tax_amount = EXCLUDED.tax_amount,
                    final_total = EXCLUDED.final_total,
                    status = EXCLUDED.status,
                    modified_date = EXCLUDED.modified_date
            """
            
            # Batch insert
            batch_size = self.load_config['batch_size']
            for i in range(0, len(line_items), batch_size):
                batch = line_items[i:i + batch_size]
                
                try:
                    execute_batch(self.cursor, insert_sql, batch, page_size=batch_size)
                    self.connection.commit()
                    load_results['loaded_records'] += len(batch)
                    logger.info(f"Loaded batch {i//batch_size + 1}: {len(batch)} records")
                    
                except Exception as e:
                    self.connection.rollback()
                    load_results['failed_records'] += len(batch)
                    error_msg = f"Failed to load batch {i//batch_size + 1}: {str(e)}"
                    logger.error(error_msg)
                    load_results['errors'].append(error_msg)
                    
                    if not self.load_config['continue_on_error']:
                        raise
            
            # Update load statistics
            self._update_load_statistics(load_results)
            
            load_results['success'] = load_results['loaded_records'] > 0
            logger.info(
                f"Load completed: {load_results['loaded_records']} records loaded, "
                f"{load_results['failed_records']} failed"
            )
            
        except Exception as e:
            logger.error(f"Load failed: {e}")
            load_results['errors'].append(f"Load exception: {str(e)}")
            if self.connection:
                self.connection.rollback()
        finally:
            self.disconnect()
        
        return load_results
    
    def _update_load_statistics(self, load_results: Dict[str, Any]) -> None:
        """Update load statistics table"""
        try:
            stats_sql = """
                INSERT INTO load_statistics (
                    load_timestamp, table_name, total_records,
                    loaded_records, failed_records, status
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            status = 'SUCCESS' if load_results['success'] else 'FAILED'
            
            self.cursor.execute(stats_sql, (
                load_results['load_timestamp'],
                'sales_line_items',
                load_results['total_records'],
                load_results['loaded_records'],
                load_results['failed_records'],
                status
            ))
            self.connection.commit()
            
        except Exception as e:
            logger.warning(f"Failed to update load statistics: {e}")
    
    def validate_against_source(self, source_data: List[Dict[str, Any]], 
                               loaded_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate loaded data against source system
        
        Args:
            source_data: Original source data
            loaded_data: Data loaded to target
            
        Returns:
            Reconciliation results
        """
        recon_results = {
            'match': True,
            'source_count': len(source_data),
            'target_count': len(loaded_data),
            'missing_in_target': [],
            'extra_in_target': [],
            'value_mismatches': []
        }
        
        # Create lookup dictionaries
        source_dict = {item['line_item_id']: item for item in source_data}
        target_dict = {item['line_item_id']: item for item in loaded_data}
        
        # Check for missing records
        source_ids = set(source_dict.keys())
        target_ids = set(target_dict.keys())
        
        missing_ids = source_ids - target_ids
        extra_ids = target_ids - source_ids
        
        if missing_ids:
            recon_results['missing_in_target'] = list(missing_ids)
            recon_results['match'] = False
            logger.warning(f"Missing {len(missing_ids)} records in target")
        
        if extra_ids:
            recon_results['extra_in_target'] = list(extra_ids)
            recon_results['match'] = False
            logger.warning(f"Found {len(extra_ids)} extra records in target")
        
        # Compare values for matching records
        compare_fields = self.validation_config['reconciliation_fields']
        
        for line_item_id in source_ids & target_ids:
            source_item = source_dict[line_item_id]
            target_item = target_dict[line_item_id]
            
            for field in compare_fields:
                source_value = Decimal(str(source_item.get(field, 0)))
                target_value = Decimal(str(target_item.get(field, 0)))
                
                if abs(source_value - target_value) > Decimal('0.01'):
                    recon_results['value_mismatches'].append({
                        'line_item_id': line_item_id,
                        'field': field,
                        'source_value': float(source_value),
                        'target_value': float(target_value),
                        'difference': float(source_value - target_value)
                    })
                    recon_results['match'] = False
        
        if recon_results['value_mismatches']:
            logger.warning(
                f"Found {len(recon_results['value_mismatches'])} value mismatches"
            )
        
        return recon_results


def main():
    """Main execution function"""
    import json
    
    # Initialize loader
    loader = SalesLineItemLoader()
    
    # Load processed data (from transform output)
    with open('data/processed_line_items.json', 'r') as f:
        line_items = json.load(f)
    
    logger.info(f"Loaded {len(line_items)} line items for loading")
    
    # Load to target database
    load_results = loader.load_line_items(line_items, validate=True)
    
    # Print results
    print("\n" + "="*80)
    print("LOAD RESULTS")
    print("="*80)
    print(f"Total Records: {load_results['total_records']}")
    print(f"Loaded Records: {load_results['loaded_records']}")
    print(f"Failed Records: {load_results['failed_records']}")
    print(f"Success: {load_results['success']}")
    
    if load_results['validation_results']:
        val_results = load_results['validation_results']
        print(f"\nValidation Results:")
        print(f"  Valid Records: {val_results['valid_records']}")
        print(f"  Invalid Records: {val_results['invalid_records']}")
        print(f"  Warnings: {len(val_results['warnings'])}")
        
        if val_results['errors']:
            print(f"\nValidation Errors (first 5):")
            for error in val_results['errors'][:5]:
                print(f"  Line Item {error['line_item_id']}: {error['errors']}")
    
    if load_results['errors']:
        print(f"\nLoad Errors:")
        for error in load_results['errors']:
            print(f"  {error}")
    
    # Save results
    with open('data/load_results.json', 'w') as f:
        json.dump(load_results, f, indent=2, default=str)
    
    logger.info("Load results saved to data/load_results.json")


if __name__ == "__main__":
    main()