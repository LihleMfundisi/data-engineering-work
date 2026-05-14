import duckdb 
conn = duckdb.connect('banking_dwh.duckdb')

result = conn.execute("SELECT * FROM DIM_CUSTOMER").fetchall()

print("All rows:")
print(result)

print("\nFirst row only:")
print(result[0])

print("\nJust the name from the first row:")
print(result[0][2])
conn.close()
