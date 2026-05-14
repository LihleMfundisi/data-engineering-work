import duckdb

conn = duckdb.connect('banking_dwh.duckdb')

conn.execute("ALTER TABLE FACT_TRANSECTIONS RENAME TO FACT_TRANSACTIONS")

tables = conn.execute("SHOW TABLES").fetchall()
print("Updated tables:")
for t in tables:
    print(f"  → {t[0]}")

conn.close()