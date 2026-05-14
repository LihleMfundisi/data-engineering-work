import pandas as pd
import duckdb

# ── EXTRACT ──────────────────────────────────────────────────────────────────
df = pd.read_csv('raw_transactions.csv')
print(f"Rows extracted: {len(df)}")
print(f"\nRaw sample:\n{df.head()}")
print(f"\nColumn names: {list(df.columns)}")
print(f"\nData types:\n{df.dtypes}")

# ── TRANSFORM ─────────────────────────────────────────────────────────────────
print("\nTransforming data...")

# 1. Standardise date formats
df['trans_date'] = pd.to_datetime(df['trans_date'], format='mixed', dayfirst=True)
print("Date format standardised")

# 2. Handle missing values
df['amt'] = df['amt'].fillna(0.0)
df['fee'] = df['fee'].fillna(0.0)
print("Missing values handled")

# 3. Convert flagged column to boolean
df['flagged'] = df['flagged'].map({'Y': True, 'N': False})
print("Flagged column converted")

# 4. Rename columns to match warehouse schema
df = df.rename(columns={
    'trans_id'   : 'transaction_id',
    'trans_date' : 'date_key',
    'cust_id'    : 'customer_id',
    'acc_id'     : 'account_id',
    'amt'        : 'amount',
    'fee'        : 'fee_charged',
    'flagged'    : 'is_flagged'
})
print("Columns renamed")

# 5. Strip whitespace from string columns
df['customer_id'] = df['customer_id'].str.strip()
df['account_id']  = df['account_id'].str.strip()
df['branch']      = df['branch'].str.strip()
print("Whitespace stripped")

# 6. Reject rows with non-existent customers
valid_customers = ['CUS001', 'CUS002', 'CUS003', 'CUS004', 'CUS005']
rejected_transform = df[~df['customer_id'].isin(valid_customers)]
df = df[df['customer_id'].isin(valid_customers)]
print(f"Rejected {len(rejected_transform)} rows with invalid customers")
for _, row in rejected_transform.iterrows():
    print(f"  → {row['transaction_id']} — customer {row['customer_id']} not found")

print(f"\nRows remaining after transform: {len(df)}")
print(f"\nTransformed sample:\n{df.head()}")
print(f"\nData types after transform:\n{df.dtypes}")

# ── LOAD ──────────────────────────────────────────────────────────────────────
print("\nLoading clean data into warehouse...")

conn = duckdb.connect('banking_dwh.duckdb')

# Load all dimension keys ONCE upfront into dicts (no per-row queries)
customer_keys = {
    r[0]: r[1]
    for r in conn.execute("SELECT customer_id, customer_key FROM DIM_CUSTOMER").fetchall()
}
account_keys = {
    r[0]: r[1]
    for r in conn.execute("SELECT account_id, account_key FROM DIM_ACCOUNT").fetchall()
}
branch_keys = {
    r[0]: r[1]
    for r in conn.execute("SELECT branch_code, branch_key FROM DIM_BRANCH").fetchall()
}

loaded        = 0
skipped       = 0
rejected_load = []

for _, row in df.iterrows():

    # Validate dimension lookups BEFORE attempting insert
    customer_key = customer_keys.get(row['customer_id'])
    account_key  = account_keys.get(row['account_id'])
    branch_key   = branch_keys.get(row['branch'])

    if None in (customer_key, account_key, branch_key):
        missing = []
        if customer_key is None: missing.append(f"customer_id={row['customer_id']}")
        if account_key  is None: missing.append(f"account_id={row['account_id']}")
        if branch_key   is None: missing.append(f"branch={row['branch']}")
        rejected_load.append((row['transaction_id'], f"Dimension lookup failed: {', '.join(missing)}"))
        continue

    try:
        result = conn.execute("""
            INSERT OR IGNORE INTO FACT_TRANSACTIONS VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            row['transaction_id'],
            int(row['date_key'].strftime('%Y%m%d')),
            customer_key,
            account_key,
            branch_key,
            None,            # loan_product_key — intentionally NULL for non-loan transactions
            row['trans_type'],
            row['amount'],
            row['balance'],
            row['fee_charged'],
            row['is_flagged']
        ])

        if result.fetchone()[0] == 0:
            skipped += 1
        else:
            loaded += 1

    except Exception as e:
        rejected_load.append((row['transaction_id'], str(e)))

conn.close()

# ── SUMMARY ───────────────────────────────────────────────────────────────────
print(f"\n{'─'*40}")
print(f"  Loaded  : {loaded} new rows")
print(f"  Skipped : {skipped} already existing rows (duplicates correctly ignored)")
print(f"  Rejected: {len(rejected_load)} rows (errors)")
if rejected_load:
    print(f"\nRejection detail:")
    for t, e in rejected_load:
        print(f"  → {t} — {e}")
print(f"{'─'*40}")
print("\nETL pipeline completed")