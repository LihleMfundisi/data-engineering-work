import duckdb

conn = duckdb.connect('banking_dwh.duckdb')
print(conn.execute("DESCRIBE FACT_TRANSACTIONS").df().to_string())
conn.close()