import os
import sys
import json
from pathlib import Path

TARGET_DIR = Path(os.environ.get('TARGET_DIR', '/app/target_data'))

EXPECTED_MAPPINGS = [
    'm_customer_load', 'm_customer_deduplicate', 'm_customer_validate',
    'm_customer_scd2', 'm_customer_address_normalize', 'm_customer_segment',
    'm_customer_merge', 'm_customer_privacy_mask', 'm_customer_lifetime_value',
    'm_customer_churn_flag',
    'm_sales_order_load', 'm_sales_order_validate', 'm_sales_line_item_load',
    'm_sales_discount_calc', 'm_sales_tax_calc', 'm_sales_commission_calc',
    'm_sales_returns_process', 'm_sales_revenue_aggregate', 'm_sales_forecast_load',
    'm_sales_pipeline_status',
    'm_product_catalog_load', 'm_product_category_hierarchy', 'm_product_price_history',
    'm_product_inventory_load', 'm_product_inventory_alert', 'm_product_supplier_map',
    'm_product_bundle_create', 'm_product_review_sentiment', 'm_product_lifecycle_stage',
    'm_product_recommendation',
    'm_finance_gl_load', 'm_finance_ap_load', 'm_finance_ar_load',
    'm_finance_journal_validate', 'm_finance_currency_convert', 'm_finance_budget_variance',
    'm_finance_expense_categorize', 'm_finance_revenue_recognize', 'm_finance_consolidation',
    'm_finance_audit_trail',
    'm_hr_employee_load', 'm_hr_payroll_calc', 'm_hr_attendance_load',
    'm_hr_performance_score', 'm_hr_turnover_analysis',
    'm_ops_shipping_load', 'm_ops_delivery_tracking', 'm_ops_warehouse_inventory',
    'm_ops_quality_metrics', 'm_ops_vendor_scorecard',
]


def validate():
    print('=' * 70)
    print('INFORMATICA ETL OUTPUT VALIDATION')
    print('=' * 70)

    passed = 0
    failed = 0
    results = []

    for mapping in EXPECTED_MAPPINGS:
        csv_path = TARGET_DIR / f'{mapping}.csv'
        if not csv_path.exists():
            print(f'  FAIL  {mapping} — output file missing')
            failed += 1
            results.append((mapping, 'FAIL', 'file missing'))
            continue

        size = csv_path.stat().st_size
        if size == 0:
            print(f'  FAIL  {mapping} — file is empty')
            failed += 1
            results.append((mapping, 'FAIL', 'empty file'))
            continue

        with open(csv_path, 'r') as f:
            header = f.readline().strip()
            first_data = f.readline().strip()

        if not header:
            print(f'  FAIL  {mapping} — no header row')
            failed += 1
            results.append((mapping, 'FAIL', 'no header'))
            continue

        if not first_data:
            print(f'  FAIL  {mapping} — no data rows')
            failed += 1
            results.append((mapping, 'FAIL', 'no data'))
            continue

        cols = header.split(',')
        has_metadata = 'load_date' in cols and 'batch_id' in cols

        line_count = sum(1 for _ in open(csv_path)) - 1

        if not has_metadata:
            print(f'  WARN  {mapping} — {line_count} rows, missing metadata columns')
            passed += 1
            results.append((mapping, 'WARN', f'{line_count} rows, no metadata'))
        else:
            print(f'  PASS  {mapping} — {line_count} rows, {len(cols)} columns')
            passed += 1
            results.append((mapping, 'PASS', f'{line_count} rows'))

    summary_path = TARGET_DIR / 'etl_summary.json'
    if summary_path.exists():
        with open(summary_path) as f:
            summary = json.load(f)
        print(f'\n  ETL Summary: {summary.get("success", 0)} succeeded, '
              f'{summary.get("failed", 0)} failed, {summary.get("skipped", 0)} skipped')

    print('\n' + '=' * 70)
    total = passed + failed
    print(f'VALIDATION RESULT: {passed}/{total} PASSED, {failed}/{total} FAILED')
    if failed == 0:
        print('STATUS: ALL TESTS PASSED')
    else:
        print('STATUS: SOME TESTS FAILED')
    print('=' * 70)

    return 0 if failed == 0 else 1


if __name__ == '__main__':
    sys.exit(validate())
