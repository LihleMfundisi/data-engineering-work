import duckdb 
conn = duckdb.connect('banking_dwh.duckdb')
tables = conn.execute("SHOW TABLES").fetchall()
print("Tables in banking_dwh:")
for t in tables: 
    print(f"  → {t[0]}")
conn.close()
