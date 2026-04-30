import os
import sys
import json
import time
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from etl_engine.mapping_parser import parse_repository
from etl_engine import transformations as T

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / 'config' / 'etl_config.json'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(BASE_DIR / 'etl_execution.log', mode='w'),
    ],
)
log = logging.getLogger('etl_runner')


def load_config():
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH) as f:
            return json.load(f)
    return {}


def read_csv(source_dir, filename):
    path = Path(source_dir) / filename
    if path.exists():
        return pd.read_csv(path)
    for f in Path(source_dir).glob('*.csv'):
        if filename.replace('.csv', '') in f.stem:
            return pd.read_csv(f)
    return pd.DataFrame()


def write_target(df, target_dir, name):
    Path(target_dir).mkdir(parents=True, exist_ok=True)
    out = Path(target_dir) / f"{name}.csv"
    df.to_csv(out, index=False)
    return len(df)


def add_metadata(df, batch_id):
    df = df.copy()
    df['load_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df['source_system'] = 'informatica_etl'
    df['batch_id'] = batch_id
    return df


# ---------- mapping handlers ----------

def run_customer_load(src, tgt, batch_id):
    df = read_csv(src, 'customers.csv')
    return write_target(add_metadata(df, batch_id), tgt, 'm_customer_load')


def run_customer_deduplicate(src, tgt, batch_id):
    df = read_csv(src, 'customers.csv')
    df = T.deduplicate_records(df, ['email'])
    return write_target(add_metadata(df, batch_id), tgt, 'm_customer_deduplicate')


def run_customer_validate(src, tgt, batch_id):
    df = read_csv(src, 'customers.csv')
    df = df.copy()
    df['email_valid'] = df['email'].apply(T.validate_email)
    df['phone_valid'] = df['phone'].apply(T.validate_phone)
    df['is_valid'] = df['email_valid'] & df['phone_valid']
    return write_target(add_metadata(df, batch_id), tgt, 'm_customer_validate')


def run_customer_scd2(src, tgt, batch_id):
    df = read_csv(src, 'customers.csv')
    existing = pd.DataFrame()
    result = T.calculate_scd2_changes(
        existing, df,
        key_cols=['customer_id'],
        tracked_cols=['email', 'phone', 'city', 'state', 'status'],
    )
    return write_target(add_metadata(result, batch_id), tgt, 'm_customer_scd2')


def run_customer_address_normalize(src, tgt, batch_id):
    df = read_csv(src, 'customer_addresses.csv')
    df = df.copy()
    df['address_line1_normalized'] = df['address_line1'].apply(T.normalize_address)
    df['address_line2_normalized'] = df['address_line2'].apply(T.normalize_address)
    return write_target(add_metadata(df, batch_id), tgt, 'm_customer_address_normalize')


def run_customer_segment(src, tgt, batch_id):
    txn = read_csv(src, 'customer_transactions.csv')
    cust = read_csv(src, 'customers.csv')
    purchases = txn[txn['transaction_type'] == 'purchase']
    agg = purchases.groupby('customer_id').agg(
        total_spend=('amount', 'sum'), order_count=('transaction_id', 'count')
    ).reset_index()
    agg['segment'] = agg['total_spend'].apply(
        lambda x: 'platinum' if x >= 400 else ('gold' if x >= 200 else ('silver' if x >= 100 else 'bronze'))
    )
    result = cust[['customer_id', 'first_name', 'last_name']].merge(agg, on='customer_id', how='left')
    result['segment'] = result['segment'].fillna('new')
    return write_target(add_metadata(result, batch_id), tgt, 'm_customer_segment')


def run_customer_merge(src, tgt, batch_id):
    cust = read_csv(src, 'customers.csv')
    addr = read_csv(src, 'customer_addresses.csv')
    primary = addr[addr['is_primary'] == 'Y'] if 'is_primary' in addr.columns else addr
    result = cust.merge(primary[['customer_id', 'address_type']], on='customer_id', how='left')
    return write_target(add_metadata(result, batch_id), tgt, 'm_customer_merge')


def run_customer_privacy_mask(src, tgt, batch_id):
    df = read_csv(src, 'customers.csv')
    df = df.copy()
    df['email'] = df['email'].apply(T.mask_email)
    df['phone'] = df['phone'].apply(T.mask_pii)
    df['first_name'] = df['first_name'].apply(T.mask_pii)
    df['last_name'] = df['last_name'].apply(T.mask_pii)
    return write_target(add_metadata(df, batch_id), tgt, 'm_customer_privacy_mask')


def run_customer_lifetime_value(src, tgt, batch_id):
    cust = read_csv(src, 'customers.csv')
    txn = read_csv(src, 'customer_transactions.csv')
    txn['amount'] = pd.to_numeric(txn['amount'], errors='coerce').fillna(0)
    rows = []
    for _, c in cust.iterrows():
        ltv = T.calculate_lifetime_value(txn, c['customer_id'])
        rows.append({'customer_id': c['customer_id'], 'first_name': c['first_name'],
                      'last_name': c['last_name'], 'lifetime_value': ltv})
    return write_target(add_metadata(pd.DataFrame(rows), batch_id), tgt, 'm_customer_lifetime_value')


def run_customer_churn_flag(src, tgt, batch_id):
    cust = read_csv(src, 'customers.csv')
    txn = read_csv(src, 'customer_transactions.csv')
    txn['transaction_date'] = pd.to_datetime(txn['transaction_date'], errors='coerce')
    latest = txn.groupby('customer_id')['transaction_date'].max().reset_index()
    latest.columns = ['customer_id', 'last_transaction_date']
    result = cust[['customer_id', 'first_name', 'last_name', 'status']].merge(latest, on='customer_id', how='left')
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=90)
    result['churn_risk'] = result.apply(
        lambda r: 'high' if pd.isna(r['last_transaction_date']) or r['last_transaction_date'] < cutoff
        else ('medium' if r['status'] == 'inactive' else 'low'), axis=1
    )
    return write_target(add_metadata(result, batch_id), tgt, 'm_customer_churn_flag')


def run_sales_order_load(src, tgt, batch_id):
    df = read_csv(src, 'sales_orders.csv')
    return write_target(add_metadata(df, batch_id), tgt, 'm_sales_order_load')


def run_sales_order_validate(src, tgt, batch_id):
    df = read_csv(src, 'sales_orders.csv')
    df = df.copy()
    df['total_amount'] = pd.to_numeric(df['total_amount'], errors='coerce')
    df['amount_valid'] = df['total_amount'] > 0
    df['date_valid'] = pd.to_datetime(df['order_date'], errors='coerce').notna()
    df['is_valid'] = df['amount_valid'] & df['date_valid']
    return write_target(add_metadata(df, batch_id), tgt, 'm_sales_order_validate')


def run_sales_line_item_load(src, tgt, batch_id):
    df = read_csv(src, 'sales_line_items.csv')
    return write_target(add_metadata(df, batch_id), tgt, 'm_sales_line_item_load')


def run_sales_discount_calc(src, tgt, batch_id):
    df = read_csv(src, 'sales_line_items.csv')
    df = df.copy()
    df['gross_amount'] = df['quantity'] * df['unit_price']
    df['discount_amount'] = df['gross_amount'] - df.apply(
        lambda r: T.calculate_discount(r['gross_amount'], r['discount_pct']), axis=1
    )
    df['net_amount'] = df['gross_amount'] - df['discount_amount']
    return write_target(add_metadata(df, batch_id), tgt, 'm_sales_discount_calc')


def run_sales_tax_calc(src, tgt, batch_id):
    orders = read_csv(src, 'sales_orders.csv')
    orders = orders.copy()
    orders['tax_amount'] = orders.apply(
        lambda r: T.calculate_tax(r['total_amount'], r['region']), axis=1
    )
    orders['total_with_tax'] = orders['total_amount'] + orders['tax_amount']
    return write_target(add_metadata(orders, batch_id), tgt, 'm_sales_tax_calc')


def run_sales_commission_calc(src, tgt, batch_id):
    orders = read_csv(src, 'sales_orders.csv')
    orders = orders.copy()
    orders['commission_amount'] = orders['total_amount'].apply(
        lambda x: T.calculate_commission(x, 'standard')
    )
    return write_target(add_metadata(orders, batch_id), tgt, 'm_sales_commission_calc')


def run_sales_returns_process(src, tgt, batch_id):
    df = read_csv(src, 'sales_returns.csv')
    df = df.copy()
    df['processing_date'] = datetime.now().strftime('%Y-%m-%d')
    df['is_processed'] = df['status'] == 'completed'
    return write_target(add_metadata(df, batch_id), tgt, 'm_sales_returns_process')


def run_sales_revenue_aggregate(src, tgt, batch_id):
    orders = read_csv(src, 'sales_orders.csv')
    orders['order_date'] = pd.to_datetime(orders['order_date'])
    orders['period'] = orders['order_date'].dt.to_period('M').astype(str)
    agg = orders.groupby(['region', 'period']).agg(
        total_revenue=('total_amount', 'sum'),
        order_count=('order_id', 'count'),
        avg_order_value=('total_amount', 'mean'),
    ).reset_index()
    return write_target(add_metadata(agg, batch_id), tgt, 'm_sales_revenue_aggregate')


def run_sales_forecast_load(src, tgt, batch_id):
    df = read_csv(src, 'sales_forecast.csv')
    return write_target(add_metadata(df, batch_id), tgt, 'm_sales_forecast_load')


def run_sales_pipeline_status(src, tgt, batch_id):
    df = read_csv(src, 'sales_pipeline.csv')
    df = df.copy()
    df['weighted_amount'] = df['amount'] * df['probability'] / 100
    stage_order = {'qualification': 1, 'proposal': 2, 'negotiation': 3, 'closed_won': 4, 'closed_lost': 5}
    df['stage_rank'] = df['stage'].map(stage_order).fillna(0)
    return write_target(add_metadata(df, batch_id), tgt, 'm_sales_pipeline_status')


def run_product_catalog_load(src, tgt, batch_id):
    df = read_csv(src, 'products.csv')
    return write_target(add_metadata(df, batch_id), tgt, 'm_product_catalog_load')


def run_product_category_hierarchy(src, tgt, batch_id):
    df = read_csv(src, 'product_categories.csv')
    df = df.copy()
    parent_map = dict(zip(df['category_id'], df['category_name']))
    df['parent_name'] = df['parent_category_id'].map(parent_map).fillna('ROOT')
    df['full_path'] = df.apply(
        lambda r: f"{r['parent_name']} > {r['category_name']}" if r['parent_name'] != 'ROOT' else r['category_name'],
        axis=1,
    )
    return write_target(add_metadata(df, batch_id), tgt, 'm_product_category_hierarchy')


def run_product_price_history(src, tgt, batch_id):
    df = read_csv(src, 'product_prices.csv')
    df = df.copy()
    df['price_change'] = df.groupby('product_id')['list_price'].diff().fillna(0)
    df['price_change_pct'] = df.apply(
        lambda r: round(r['price_change'] / (r['list_price'] - r['price_change']) * 100, 2)
        if r['price_change'] != 0 and (r['list_price'] - r['price_change']) != 0 else 0.0, axis=1
    )
    return write_target(add_metadata(df, batch_id), tgt, 'm_product_price_history')


def run_product_inventory_load(src, tgt, batch_id):
    df = read_csv(src, 'product_inventory.csv')
    return write_target(add_metadata(df, batch_id), tgt, 'm_product_inventory_load')


def run_product_inventory_alert(src, tgt, batch_id):
    df = read_csv(src, 'product_inventory.csv')
    df = df.copy()
    df['stock_status'] = df.apply(
        lambda r: 'critical' if r['quantity_on_hand'] <= r['reorder_level'] * 0.5
        else ('low' if r['quantity_on_hand'] <= r['reorder_level'] else 'ok'), axis=1
    )
    df['needs_reorder'] = df['quantity_on_hand'] <= df['reorder_level']
    alerts = df[df['needs_reorder']]
    return write_target(add_metadata(alerts, batch_id), tgt, 'm_product_inventory_alert')


def run_product_supplier_map(src, tgt, batch_id):
    products = read_csv(src, 'products.csv')
    suppliers = read_csv(src, 'product_suppliers.csv')
    result = products.merge(suppliers, on='supplier_id', how='left', suffixes=('', '_supplier'))
    return write_target(add_metadata(result, batch_id), tgt, 'm_product_supplier_map')


def run_product_bundle_create(src, tgt, batch_id):
    products = read_csv(src, 'products.csv')
    bundles = []
    categories = products['category'].unique()
    for cat in categories:
        cat_prods = products[products['category'] == cat].head(3)
        if len(cat_prods) >= 2:
            bundle_price = round(cat_prods['unit_price'].sum() * 0.85, 2)
            bundle_cost = round(cat_prods['cost_price'].sum(), 2)
            prod_ids = ','.join(cat_prods['product_id'].tolist())
            bundles.append({
                'bundle_id': f"BDL_{cat.upper()[:4]}",
                'bundle_name': f"{cat} Essentials Bundle",
                'product_ids': prod_ids,
                'bundle_price': bundle_price,
                'bundle_cost': bundle_cost,
                'savings_pct': 15.0,
            })
    return write_target(add_metadata(pd.DataFrame(bundles), batch_id), tgt, 'm_product_bundle_create')


def run_product_review_sentiment(src, tgt, batch_id):
    reviews = read_csv(src, 'product_reviews.csv')
    agg = reviews.groupby('product_id').agg(
        avg_rating=('rating', 'mean'),
        review_count=('review_id', 'count'),
        min_rating=('rating', 'min'),
        max_rating=('rating', 'max'),
    ).reset_index()
    agg['sentiment'] = agg['avg_rating'].apply(
        lambda x: 'positive' if x >= 4.0 else ('neutral' if x >= 3.0 else 'negative')
    )
    return write_target(add_metadata(agg, batch_id), tgt, 'm_product_review_sentiment')


def run_product_lifecycle_stage(src, tgt, batch_id):
    products = read_csv(src, 'products.csv')
    products = products.copy()
    products['lifecycle_stage'] = products.apply(
        lambda r: T.classify_lifecycle_stage(r['launch_date'], r['status']), axis=1
    )
    return write_target(add_metadata(products, batch_id), tgt, 'm_product_lifecycle_stage')


def run_product_recommendation(src, tgt, batch_id):
    line_items = read_csv(src, 'sales_line_items.csv')
    products = read_csv(src, 'products.csv')
    freq = line_items.groupby('product_id')['order_id'].count().reset_index()
    freq.columns = ['product_id', 'purchase_frequency']
    result = products[['product_id', 'product_name', 'category']].merge(freq, on='product_id', how='left')
    result['purchase_frequency'] = result['purchase_frequency'].fillna(0).astype(int)
    result['recommendation_score'] = result['purchase_frequency'] / max(result['purchase_frequency'].max(), 1) * 100
    result = result.sort_values('recommendation_score', ascending=False)
    return write_target(add_metadata(result, batch_id), tgt, 'm_product_recommendation')


def run_finance_gl_load(src, tgt, batch_id):
    df = read_csv(src, 'general_ledger.csv')
    return write_target(add_metadata(df, batch_id), tgt, 'm_finance_gl_load')


def run_finance_ap_load(src, tgt, batch_id):
    df = read_csv(src, 'accounts_payable.csv')
    return write_target(add_metadata(df, batch_id), tgt, 'm_finance_ap_load')


def run_finance_ar_load(src, tgt, batch_id):
    df = read_csv(src, 'accounts_receivable.csv')
    return write_target(add_metadata(df, batch_id), tgt, 'm_finance_ar_load')


def run_finance_journal_validate(src, tgt, batch_id):
    gl = read_csv(src, 'general_ledger.csv')
    gl['debit_amount'] = pd.to_numeric(gl['debit_amount'], errors='coerce').fillna(0)
    gl['credit_amount'] = pd.to_numeric(gl['credit_amount'], errors='coerce').fillna(0)
    total_debits = gl['debit_amount'].sum()
    total_credits = gl['credit_amount'].sum()
    gl = gl.copy()
    gl['running_debit'] = gl['debit_amount'].cumsum()
    gl['running_credit'] = gl['credit_amount'].cumsum()
    gl['balance_check'] = abs(total_debits - total_credits) < 0.01
    gl['validation_status'] = 'balanced' if abs(total_debits - total_credits) < 0.01 else 'imbalanced'
    return write_target(add_metadata(gl, batch_id), tgt, 'm_finance_journal_validate')


def run_finance_currency_convert(src, tgt, batch_id):
    gl = read_csv(src, 'general_ledger.csv')
    rates = read_csv(src, 'exchange_rates.csv')
    gl = gl.copy()
    gl['debit_usd'] = gl.apply(
        lambda r: T.currency_convert(r['debit_amount'], r['currency'], 'USD', rates)
        if r['currency'] != 'USD' else r['debit_amount'], axis=1
    )
    gl['credit_usd'] = gl.apply(
        lambda r: T.currency_convert(r['credit_amount'], r['currency'], 'USD', rates)
        if r['currency'] != 'USD' else r['credit_amount'], axis=1
    )
    gl['converted_currency'] = 'USD'
    return write_target(add_metadata(gl, batch_id), tgt, 'm_finance_currency_convert')


def run_finance_budget_variance(src, tgt, batch_id):
    df = read_csv(src, 'budget.csv')
    df = df.copy()
    df['variance_amount'] = df['actual_amount'] - df['budget_amount']
    df['variance_pct'] = round(df['variance_amount'] / df['budget_amount'] * 100, 2)
    df['variance_status'] = df['variance_pct'].apply(
        lambda x: 'under_budget' if x < -5 else ('on_budget' if abs(x) <= 5 else 'over_budget')
    )
    return write_target(add_metadata(df, batch_id), tgt, 'm_finance_budget_variance')


def run_finance_expense_categorize(src, tgt, batch_id):
    gl = read_csv(src, 'general_ledger.csv')
    expenses = gl[gl['debit_amount'] > 0].copy()
    expenses['expense_category'] = expenses['account_code'].apply(T.categorize_expense)
    return write_target(add_metadata(expenses, batch_id), tgt, 'm_finance_expense_categorize')


def run_finance_revenue_recognize(src, tgt, batch_id):
    gl = read_csv(src, 'general_ledger.csv')
    revenue = gl[gl['account_code'] == 4000].copy()
    revenue['entry_date'] = pd.to_datetime(revenue['entry_date'])
    revenue['recognition_period'] = revenue['entry_date'].dt.to_period('M').astype(str)
    revenue['recognized_amount'] = revenue['credit_amount']
    revenue['recognition_status'] = 'recognized'
    return write_target(add_metadata(revenue, batch_id), tgt, 'm_finance_revenue_recognize')


def run_finance_consolidation(src, tgt, batch_id):
    gl = read_csv(src, 'general_ledger.csv')
    gl['debit_amount'] = pd.to_numeric(gl['debit_amount'], errors='coerce').fillna(0)
    gl['credit_amount'] = pd.to_numeric(gl['credit_amount'], errors='coerce').fillna(0)
    consolidated = gl.groupby(['account_code', 'account_name']).agg(
        total_debits=('debit_amount', 'sum'),
        total_credits=('credit_amount', 'sum'),
        entry_count=('entry_id', 'count'),
    ).reset_index()
    consolidated['net_balance'] = consolidated['total_debits'] - consolidated['total_credits']
    return write_target(add_metadata(consolidated, batch_id), tgt, 'm_finance_consolidation')


def run_finance_audit_trail(src, tgt, batch_id):
    gl = read_csv(src, 'general_ledger.csv')
    gl = gl.copy()
    gl['audit_hash'] = gl.apply(lambda r: T.generate_audit_hash(r.to_dict()), axis=1)
    gl['audit_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    gl['audit_user'] = 'etl_system'
    gl['audit_action'] = 'load'
    return write_target(add_metadata(gl, batch_id), tgt, 'm_finance_audit_trail')


def run_hr_employee_load(src, tgt, batch_id):
    df = read_csv(src, 'employees.csv')
    return write_target(add_metadata(df, batch_id), tgt, 'm_hr_employee_load')


def run_hr_payroll_calc(src, tgt, batch_id):
    df = read_csv(src, 'payroll.csv')
    df = df.copy()
    df['total_deductions'] = df['federal_tax'] + df['state_tax'] + df['insurance'] + df['retirement_401k']
    df['effective_tax_rate'] = round((df['federal_tax'] + df['state_tax']) / df['gross_pay'] * 100, 2)
    df['calc_net_pay'] = df['gross_pay'] - df['total_deductions']
    df['net_pay_variance'] = round(df['net_pay'] - df['calc_net_pay'], 2)
    return write_target(add_metadata(df, batch_id), tgt, 'm_hr_payroll_calc')


def run_hr_attendance_load(src, tgt, batch_id):
    df = read_csv(src, 'attendance.csv')
    return write_target(add_metadata(df, batch_id), tgt, 'm_hr_attendance_load')


def run_hr_performance_score(src, tgt, batch_id):
    df = read_csv(src, 'performance.csv')
    df = df.copy()
    df['weighted_score'] = df.apply(
        lambda r: T.calculate_performance_score(r['goal_score'], r['competency_score'], r['manager_rating']), axis=1
    )
    df['performance_tier'] = df['weighted_score'].apply(
        lambda x: 'exceptional' if x >= 4.5 else ('strong' if x >= 3.8 else ('meets' if x >= 3.0 else 'below'))
    )
    return write_target(add_metadata(df, batch_id), tgt, 'm_hr_performance_score')


def run_hr_turnover_analysis(src, tgt, batch_id):
    emp = read_csv(src, 'employees.csv')
    perf = read_csv(src, 'performance.csv')
    merged = emp.merge(perf[['employee_id', 'goal_score', 'manager_rating']], on='employee_id', how='left')
    merged = merged.copy()
    merged['tenure_months'] = merged['hire_date'].apply(
        lambda d: max(1, (datetime.now() - pd.to_datetime(d)).days // 30) if pd.notna(d) else 0
    )
    merged['turnover_risk'] = merged.apply(
        lambda r: 'high' if r['status'] == 'terminated'
        else ('medium' if (pd.notna(r.get('manager_rating')) and r['manager_rating'] < 3.0) or r['tenure_months'] < 12
              else 'low'), axis=1
    )
    return write_target(add_metadata(merged, batch_id), tgt, 'm_hr_turnover_analysis')


def run_ops_shipping_load(src, tgt, batch_id):
    df = read_csv(src, 'shipping.csv')
    return write_target(add_metadata(df, batch_id), tgt, 'm_ops_shipping_load')


def run_ops_delivery_tracking(src, tgt, batch_id):
    df = read_csv(src, 'shipping.csv')
    df = df.copy()
    df['ship_date'] = pd.to_datetime(df['ship_date'], errors='coerce')
    df['delivery_date'] = pd.to_datetime(df['delivery_date'], errors='coerce')
    df['transit_days'] = (df['delivery_date'] - df['ship_date']).dt.days
    sla_days = 5
    df['sla_met'] = df['transit_days'].apply(lambda x: x <= sla_days if pd.notna(x) else False)
    df['delivery_status'] = df.apply(
        lambda r: 'delivered_on_time' if r['sla_met']
        else ('delivered_late' if pd.notna(r['transit_days']) else 'in_transit'), axis=1
    )
    return write_target(add_metadata(df, batch_id), tgt, 'm_ops_delivery_tracking')


def run_ops_warehouse_inventory(src, tgt, batch_id):
    wh = read_csv(src, 'warehouse_inventory.csv')
    inv = read_csv(src, 'product_inventory.csv')
    wh = wh.copy()
    wh['abs_discrepancy'] = wh['discrepancy'].abs()
    wh['accuracy_pct'] = round((1 - wh['abs_discrepancy'] / wh['quantity'].clip(lower=1)) * 100, 2)
    wh['needs_recount'] = wh['abs_discrepancy'] > 1
    return write_target(add_metadata(wh, batch_id), tgt, 'm_ops_warehouse_inventory')


def run_ops_quality_metrics(src, tgt, batch_id):
    df = read_csv(src, 'quality_metrics.csv')
    df = df.copy()
    df['defect_rate'] = round(df['defect_count'] / df['batch_size'] * 100, 4)
    df['quality_grade'] = df['pass_rate'].apply(
        lambda x: 'A' if x >= 99.5 else ('B' if x >= 98.0 else ('C' if x >= 95.0 else 'F'))
    )
    return write_target(add_metadata(df, batch_id), tgt, 'm_ops_quality_metrics')


def run_ops_vendor_scorecard(src, tgt, batch_id):
    vendors = read_csv(src, 'vendors.csv')
    vendors = vendors.copy()
    vendors['composite_score'] = vendors.apply(
        lambda r: T.score_vendor(r['on_time_delivery_pct'], r['quality_score'], r['total_spend']), axis=1
    )
    vendors['tier'] = vendors['composite_score'].apply(
        lambda x: 'preferred' if x >= 80 else ('approved' if x >= 60 else 'probationary')
    )
    return write_target(add_metadata(vendors, batch_id), tgt, 'm_ops_vendor_scorecard')


MAPPING_HANDLERS = {
    'm_customer_load': run_customer_load,
    'm_customer_deduplicate': run_customer_deduplicate,
    'm_customer_validate': run_customer_validate,
    'm_customer_scd2': run_customer_scd2,
    'm_customer_address_normalize': run_customer_address_normalize,
    'm_customer_segment': run_customer_segment,
    'm_customer_merge': run_customer_merge,
    'm_customer_privacy_mask': run_customer_privacy_mask,
    'm_customer_lifetime_value': run_customer_lifetime_value,
    'm_customer_churn_flag': run_customer_churn_flag,
    'm_sales_order_load': run_sales_order_load,
    'm_sales_order_validate': run_sales_order_validate,
    'm_sales_line_item_load': run_sales_line_item_load,
    'm_sales_discount_calc': run_sales_discount_calc,
    'm_sales_tax_calc': run_sales_tax_calc,
    'm_sales_commission_calc': run_sales_commission_calc,
    'm_sales_returns_process': run_sales_returns_process,
    'm_sales_revenue_aggregate': run_sales_revenue_aggregate,
    'm_sales_forecast_load': run_sales_forecast_load,
    'm_sales_pipeline_status': run_sales_pipeline_status,
    'm_product_catalog_load': run_product_catalog_load,
    'm_product_category_hierarchy': run_product_category_hierarchy,
    'm_product_price_history': run_product_price_history,
    'm_product_inventory_load': run_product_inventory_load,
    'm_product_inventory_alert': run_product_inventory_alert,
    'm_product_supplier_map': run_product_supplier_map,
    'm_product_bundle_create': run_product_bundle_create,
    'm_product_review_sentiment': run_product_review_sentiment,
    'm_product_lifecycle_stage': run_product_lifecycle_stage,
    'm_product_recommendation': run_product_recommendation,
    'm_finance_gl_load': run_finance_gl_load,
    'm_finance_ap_load': run_finance_ap_load,
    'm_finance_ar_load': run_finance_ar_load,
    'm_finance_journal_validate': run_finance_journal_validate,
    'm_finance_currency_convert': run_finance_currency_convert,
    'm_finance_budget_variance': run_finance_budget_variance,
    'm_finance_expense_categorize': run_finance_expense_categorize,
    'm_finance_revenue_recognize': run_finance_revenue_recognize,
    'm_finance_consolidation': run_finance_consolidation,
    'm_finance_audit_trail': run_finance_audit_trail,
    'm_hr_employee_load': run_hr_employee_load,
    'm_hr_payroll_calc': run_hr_payroll_calc,
    'm_hr_attendance_load': run_hr_attendance_load,
    'm_hr_performance_score': run_hr_performance_score,
    'm_hr_turnover_analysis': run_hr_turnover_analysis,
    'm_ops_shipping_load': run_ops_shipping_load,
    'm_ops_delivery_tracking': run_ops_delivery_tracking,
    'm_ops_warehouse_inventory': run_ops_warehouse_inventory,
    'm_ops_quality_metrics': run_ops_quality_metrics,
    'm_ops_vendor_scorecard': run_ops_vendor_scorecard,
}


def main():
    config = load_config()
    default_src = str(BASE_DIR / 'source_data')
    default_tgt = str(BASE_DIR / 'target_data')
    default_map = str(BASE_DIR / 'mappings' / 'informatica_repository.xml')

    source_dir = os.environ.get('SOURCE_DIR', default_src)
    target_dir = os.environ.get('TARGET_DIR', default_tgt)
    mapping_file = os.environ.get('MAPPING_FILE', default_map)
    batch_id = os.environ.get('BATCH_ID', config.get('batch_id', datetime.now().strftime('BATCH_%Y%m%d_%H%M%S')))
    log_level = os.environ.get('LOG_LEVEL', config.get('log_level', 'INFO'))

    logging.getLogger().setLevel(getattr(logging, log_level.upper(), logging.INFO))

    log.info('=' * 70)
    log.info('INFORMATICA POWERCENTER ETL SIMULATION ENGINE')
    log.info('=' * 70)
    log.info(f'Batch ID    : {batch_id}')
    log.info(f'Source Dir  : {source_dir}')
    log.info(f'Target Dir  : {target_dir}')
    log.info(f'Mapping File: {mapping_file}')
    log.info('-' * 70)

    try:
        mapping_configs = parse_repository(mapping_file)
        mapping_names = [m.name for m in mapping_configs]
        log.info(f'Parsed {len(mapping_names)} mappings from repository XML')
    except FileNotFoundError:
        log.warning(f'Repository XML not found at {mapping_file}, using built-in mapping list')
        mapping_names = list(MAPPING_HANDLERS.keys())

    Path(target_dir).mkdir(parents=True, exist_ok=True)

    total = len(mapping_names)
    success = 0
    failed = 0
    skipped = 0
    results = []

    for i, name in enumerate(mapping_names, 1):
        handler = MAPPING_HANDLERS.get(name)
        if not handler:
            log.warning(f'[{i}/{total}] SKIP  {name} — no handler registered')
            skipped += 1
            results.append({'mapping': name, 'status': 'skipped', 'rows': 0, 'duration_ms': 0})
            continue

        start = time.time()
        try:
            rows = handler(source_dir, target_dir, batch_id)
            elapsed = round((time.time() - start) * 1000, 1)
            log.info(f'[{i}/{total}] OK    {name} — {rows} rows ({elapsed}ms)')
            success += 1
            results.append({'mapping': name, 'status': 'success', 'rows': rows, 'duration_ms': elapsed})
        except Exception as e:
            elapsed = round((time.time() - start) * 1000, 1)
            log.error(f'[{i}/{total}] FAIL  {name} — {e} ({elapsed}ms)')
            failed += 1
            results.append({'mapping': name, 'status': 'failed', 'rows': 0, 'duration_ms': elapsed, 'error': str(e)})

    log.info('-' * 70)
    log.info(f'SUMMARY: {success} succeeded, {failed} failed, {skipped} skipped out of {total}')
    log.info('=' * 70)

    summary = {
        'batch_id': batch_id,
        'timestamp': datetime.now().isoformat(),
        'total_mappings': total,
        'success': success,
        'failed': failed,
        'skipped': skipped,
        'results': results,
    }
    summary_path = Path(target_dir) / 'etl_summary.json'
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)
    log.info(f'Summary written to {summary_path}')

    sys.exit(0 if failed == 0 else 1)


if __name__ == '__main__':
    main()
