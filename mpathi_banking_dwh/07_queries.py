import duckdb 
conn = duckdb.connect('banking_dwh.duckdb')

#Q1: Total transaction volume by branch
q1 = """
SELECT DIM_BRANCH.branch_name, SUM(FACT_TRANSACTIONS.amount) as total_volume
FROM FACT_TRANSACTIONS
INNER JOIN DIM_BRANCH ON FACT_TRANSACTIONS.branch_key = DIM_BRANCH.branch_key
GROUP BY DIM_BRANCH.branch_name
ORDER BY total_volume DESC;
"""
result = conn.execute(q1).fetchall()
print("Q1-Transaction Volumeper Branch:")
for row in result:
    print(f"Branch {row[0]:<20} R{row[1]:>12,.2f}")

#q2: customers with flagged transactions
q2 = """
SELECT DIM_CUSTOMER.full_name, FACT_TRANSACTIONS.transaction_id, FACT_TRANSACTIONS.amount
FROM FACT_TRANSACTIONS
INNER JOIN DIM_CUSTOMER ON FACT_TRANSACTIONS.customer_key = DIM_CUSTOMER.customer_key
WHERE FACT_TRANSACTIONS.is_flagged = TRUE;
"""
result2 = conn.execute(q2).fetchall()  
print("\nQ2-Flagged Transactions")
for row in result2:
    print(f"{row[0]:<20} {row[1]}  R{row[2]:>10,.2f}")

#Total repayment amount per loan product 
q3 = """
SELECT DIM_LOAN_PRODUCT.product_name, SUM(FACT_TRANSACTIONS.amount) as total_repayment
FROM FACT_TRANSACTIONS
INNER JOIN DIM_LOAN_PRODUCT ON FACT_TRANSACTIONS.loan_product_key = DIM_LOAN_PRODUCT.loan_product_key
WHERE FACT_TRANSACTIONS.transaction_type = 'LOAN_REPAYMENT'
GROUP BY DIM_LOAN_PRODUCT.product_name,
ORDER BY DIM_LOAN_PRODUCT.product_name;
"""     
result3 = conn.execute(q3).fetchall()
print("\nQ3-Total Repayment Amount per Loan Product")
for row in result3:
    print(f"  {row[0]:<25} R{row[1]:>12,.2f}")

#Dormant accounts with recents transactions
q4 = """
SELECT DIM_ACCOUNT.account_id, DIM_ACCOUNT.account_type, MAX(FACT_TRANSACTIONS.amount) as transaction_amount
FROM FACT_TRANSACTIONS
INNER JOIN DIM_ACCOUNT ON FACT_TRANSACTIONS.account_key = DIM_ACCOUNT.account_key
WHERE DIM_ACCOUNT.status = 'DORMANT'
GROUP BY DIM_ACCOUNT.account_id, DIM_ACCOUNT.account_type;
"""
result4 = conn.execute(q4).fetchall()
print("\nQ4-Dormant Accounts with Recent Transactions")
print(f"{'Account ID':<15} {'Account Type':<15} {'Last Transaction Amount':>25}")
for row in result4:
    print(f"  {row[0]:<12} {row[1]:<15} R{row[2]:>12,.2f}")
conn.close()