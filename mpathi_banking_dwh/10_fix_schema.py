import duckdb

conn = duckdb.connect('banking_dwh.duckdb')

# Rename the misspelled columns
conn.execute("ALTER TABLE FACT_TRANSACTIONS RENAME COLUMN transection_id TO transaction_id")
conn.execute("ALTER TABLE FACT_TRANSACTIONS RENAME COLUMN transection_type TO transaction_type")

# Verify
print(conn.execute("DESCRIBE FACT_TRANSACTIONS").df().to_string())
conn.close()
print("Schema typos fixed.")