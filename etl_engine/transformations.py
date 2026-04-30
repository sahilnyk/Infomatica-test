import re
import hashlib
from datetime import datetime

import pandas as pd


def validate_email(email):
    if pd.isna(email):
        return False
    return bool(re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', str(email)))


def validate_phone(phone):
    if pd.isna(phone):
        return False
    cleaned = re.sub(r'[\s\-\(\)\+]', '', str(phone))
    return len(cleaned) >= 10 and cleaned.isdigit()


def mask_pii(value, mask_char='*'):
    if pd.isna(value):
        return value
    s = str(value)
    if len(s) <= 4:
        return mask_char * len(s)
    return s[:2] + mask_char * (len(s) - 4) + s[-2:]


def mask_email(email):
    if pd.isna(email):
        return email
    parts = str(email).split('@')
    if len(parts) != 2:
        return mask_pii(email)
    local = parts[0]
    masked_local = local[0] + '*' * (len(local) - 1) if len(local) > 1 else '*'
    return f"{masked_local}@{parts[1]}"


def calculate_tax(amount, region):
    tax_rates = {
        'Northeast': 0.08, 'West': 0.0725, 'Midwest': 0.065,
        'South': 0.07, 'Southeast': 0.06, 'default': 0.07
    }
    rate = tax_rates.get(region, tax_rates['default'])
    return round(float(amount) * rate, 2)


def calculate_commission(amount, tier='standard'):
    rates = {'junior': 0.05, 'standard': 0.08, 'senior': 0.10, 'director': 0.12}
    rate = rates.get(tier, 0.08)
    return round(float(amount) * rate, 2)


def calculate_discount(amount, discount_pct):
    return round(float(amount) * (1 - float(discount_pct) / 100), 2)


def currency_convert(amount, from_curr, to_curr, rates_df):
    if from_curr == to_curr:
        return float(amount)
    mask = (rates_df['from_currency'] == from_curr) & (rates_df['to_currency'] == to_curr)
    matched = rates_df[mask]
    if matched.empty:
        return float(amount)
    rate = matched.iloc[-1]['exchange_rate']
    return round(float(amount) * float(rate), 2)


def calculate_scd2_changes(existing_df, new_df, key_cols, tracked_cols):
    now = datetime.now().strftime('%Y-%m-%d')
    if existing_df.empty:
        new_df = new_df.copy()
        new_df['effective_date'] = now
        new_df['end_date'] = '9999-12-31'
        new_df['is_current'] = 'Y'
        return new_df

    result_rows = []
    for _, new_row in new_df.iterrows():
        key_mask = pd.Series([True] * len(existing_df))
        for kc in key_cols:
            key_mask = key_mask & (existing_df[kc] == new_row[kc])
        existing_match = existing_df[key_mask & (existing_df['is_current'] == 'Y')]

        if existing_match.empty:
            row = new_row.to_dict()
            row['effective_date'] = now
            row['end_date'] = '9999-12-31'
            row['is_current'] = 'Y'
            result_rows.append(row)
        else:
            old_row = existing_match.iloc[0]
            changed = any(str(old_row.get(tc, '')) != str(new_row.get(tc, '')) for tc in tracked_cols)
            if changed:
                expired = old_row.to_dict()
                expired['end_date'] = now
                expired['is_current'] = 'N'
                result_rows.append(expired)
                row = new_row.to_dict()
                row['effective_date'] = now
                row['end_date'] = '9999-12-31'
                row['is_current'] = 'Y'
                result_rows.append(row)
            else:
                result_rows.append(old_row.to_dict())

    if result_rows:
        return pd.DataFrame(result_rows)
    return existing_df.copy()


def deduplicate_records(df, key_cols, sort_cols=None):
    if sort_cols:
        df = df.sort_values(sort_cols, ascending=False)
    return df.drop_duplicates(subset=key_cols, keep='first').reset_index(drop=True)


def normalize_address(address):
    if pd.isna(address):
        return address
    s = str(address).strip().title()
    replacements = {
        ' St ': ' Street ', ' St.': ' Street', ' Ave ': ' Avenue ', ' Ave.': ' Avenue',
        ' Blvd ': ' Boulevard ', ' Blvd.': ' Boulevard', ' Dr ': ' Drive ', ' Dr.': ' Drive',
        ' Ln ': ' Lane ', ' Ln.': ' Lane', ' Ct ': ' Court ', ' Ct.': ' Court',
        ' Rd ': ' Road ', ' Rd.': ' Road', ' Apt ': ' Apartment ', ' Apt.': ' Apartment',
    }
    for old, new in replacements.items():
        s = s.replace(old, new)
    return s


def calculate_lifetime_value(transactions_df, customer_id):
    cust_txn = transactions_df[transactions_df['customer_id'] == customer_id]
    if cust_txn.empty:
        return 0.0
    purchases = cust_txn[cust_txn['transaction_type'] == 'purchase']['amount'].sum()
    refunds = cust_txn[cust_txn['transaction_type'] == 'refund']['amount'].sum()
    return round(float(purchases - refunds), 2)


def score_vendor(on_time_pct, quality_score, spend):
    on_time_weight = 0.4
    quality_weight = 0.4
    spend_weight = 0.2
    normalized_on_time = float(on_time_pct) / 100.0
    normalized_quality = float(quality_score) / 5.0
    normalized_spend = min(float(spend) / 500000.0, 1.0)
    score = (normalized_on_time * on_time_weight +
             normalized_quality * quality_weight +
             normalized_spend * spend_weight)
    return round(score * 100, 2)


def categorize_expense(account_code):
    code = str(account_code)
    categories = {
        '6100': 'Payroll', '6200': 'Facilities', '6300': 'Utilities',
        '6400': 'Marketing', '6500': 'Technology', '5000': 'COGS',
    }
    return categories.get(code, 'Other')


def calculate_performance_score(goal, competency, manager_rating):
    return round((float(goal) * 0.4 + float(competency) * 0.3 + float(manager_rating) * 0.3), 2)


def classify_lifecycle_stage(launch_date, status):
    if status == 'discontinued':
        return 'end_of_life'
    if pd.isna(launch_date):
        return 'unknown'
    launch = pd.to_datetime(launch_date)
    months = (datetime.now() - launch).days / 30
    if months < 6:
        return 'introduction'
    elif months < 18:
        return 'growth'
    elif months < 36:
        return 'maturity'
    return 'decline'


def generate_audit_hash(row_dict):
    content = '|'.join(str(v) for v in sorted(row_dict.items()))
    return hashlib.sha256(content.encode()).hexdigest()[:16]
